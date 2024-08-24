use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;

use crate::api::iceberg::v1::{
    ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateTableRequest, DataAccess, ErrorModel, ListTablesResponse, LoadTableResult,
    NamespaceParameters, PaginationQuery, Prefix, RegisterTableRequest, RenameTableRequest, Result,
    TableIdent, TableParameters,
};
use crate::catalog::compression_codec::CompressionCodec;
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::{NamespaceIdent, TableUpdate};
use iceberg_ext::configs::namespace::NamespaceProperties;
use iceberg_ext::configs::Location;
use serde::Serialize;
use uuid::Uuid;

use super::commit_tables::apply_commit;
use super::{
    io::write_metadata_file, namespace::validate_namespace_ident, require_warehouse_id,
    CatalogServer,
};
use crate::service::contract_verification::{ContractVerification, ContractVerificationOutcome};
use crate::service::event_publisher::{CloudEventsPublisher, EventMetadata};
use crate::service::storage::{StorageLocations as _, StoragePermissions, StorageProfile};
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, CreateTableResponse,
    LoadTableResponse as CatalogLoadTableResult, State, Transaction,
};
use crate::service::{GetNamespaceResponse, TableCommit, TableIdentUuid, WarehouseStatus};

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::tables::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        pagination_query: PaginationQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        validate_namespace_ident(&namespace)?;

        // ------------------- AUTHZ -------------------
        A::check_list_tables(
            &request_metadata,
            warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let include_staged = false;
        let tables = C::list_tables(
            warehouse_id,
            &namespace,
            include_staged,
            state.v1_state.catalog,
            pagination_query,
        )
        .await?;

        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: tables.into_iter().map(|t| t.1).collect(),
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        // mut because we need to change location
        mut request: CreateTableRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;

        if let Some(properties) = &request.properties {
            validate_table_properties(properties.keys())?;
        }

        // ------------------- AUTHZ -------------------
        A::check_create_table(
            &request_metadata,
            warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_id: TabularIdentUuid = TabularIdentUuid::Table(uuid::Uuid::now_v7());

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace = C::get_namespace(warehouse_id, &namespace, t.transaction()).await?;
        let warehouse = C::get_warehouse(warehouse_id, t.transaction()).await?;
        let storage_profile = warehouse.storage_profile;
        require_active_warehouse(warehouse.status)?;

        let table_location = determine_table_location(
            &namespace,
            request.location.clone(),
            TabularIdentUuid::Table(*table_id),
            &storage_profile,
        )?;

        // This is the only place where we change the request
        request.location = Some(table_location.to_string());
        let request = request; // Make it non-mutable again for our sanity

        // If stage-create is true, we should not create the metadata file
        let metadata_location = if request.stage_create.unwrap_or(false) {
            None
        } else {
            let metadata_id = uuid::Uuid::now_v7();
            Some(storage_profile.default_metadata_location(
                &table_location,
                &CompressionCodec::try_from_maybe_properties(request.properties.as_ref())?,
                metadata_id,
            ))
        };

        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let CreateTableResponse { table_metadata } = C::create_table(
            namespace.namespace_id,
            &table,
            TableIdentUuid::from(*table_id),
            request,
            metadata_location
                .as_ref()
                .map(iceberg_ext::configs::Location::as_str),
            t.transaction(),
        )
        .await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = &warehouse.storage_secret_id {
            let secret_state = state.v1_state.secrets;
            Some(secret_state.get_secret_by_id(secret_id).await?.secret)
        } else {
            None
        };

        if let Some(metadata_location) = &metadata_location {
            let file_io = storage_profile.file_io(storage_secret.as_ref())?;
            let compression_codec = CompressionCodec::try_from_metadata(&table_metadata)?;
            write_metadata_file(
                metadata_location,
                &table_metadata,
                compression_codec,
                &file_io,
            )
            .await?;
        };

        // This requires the storage secret
        // because the table config might contain vended-credentials based
        // on the `data_access` parameter.
        let config = storage_profile
            .generate_table_config(
                &data_access,
                storage_secret.as_ref(),
                &table_location,
                // TODO: This should be a permission based on authz
                StoragePermissions::ReadWriteDelete,
            )
            .await?;
        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.map(|l| l.to_string()),
            metadata: table_metadata,
            config: Some(config.into()),
        };

        // Metadata file written, now we can commit the transaction
        t.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id: *warehouse_id,
                name: table.name.clone(),
                namespace: table.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "createTable",
            state.v1_state.publisher.clone(),
        )
        .await;
        Ok(load_table_result)
    }

    /// Register a table in the given namespace using given metadata file location
    async fn register_table(
        _parameters: NamespaceParameters,
        _request: RegisterTableRequest,
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        Err(ErrorModel::builder()
            .code(StatusCode::NOT_IMPLEMENTED.into())
            .message("Registering tables is not supported".to_string())
            .r#type("RegisterTableNotSupported".to_string())
            .build()
            .into())
    }

    /// Load a table from the catalog
    async fn load_table(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        // ToDo: Remove workaround when hierarchical namespaces are supported.
        // It is important for now to throw a 404 if a table cannot be found,
        // because spark might check if `table`.`branch` exists, which should return 404.
        // Only then will it treat it as a branch.
        // 404 is returned by the logic in the remainder of this function. Here, we only
        // need to make sure that we don't fail prematurely on longer namespaces.
        match validate_table_or_view_ident(&table) {
            Ok(()) => {}
            Err(e) => {
                if e.error.r#type != *"NamespaceDepthExceeded" {
                    return Err(e);
                }
            }
        }

        // ------------------- AUTHZ -------------------
        let include_stage = false;
        let table_id = C::table_ident_to_id(
            warehouse_id,
            &table,
            include_stage,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_load_table(
            &request_metadata,
            warehouse_id,
            Some(&table.namespace),
            table_id,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_id = require_table_id(&table, table_id)?;
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let mut metadatas = C::load_tables(warehouse_id, vec![table_id], t.transaction()).await?;
        let CatalogLoadTableResult {
            table_id: _,
            namespace_id: _,
            table_metadata,
            metadata_location,
            storage_secret_ident,
            storage_profile,
        } = remove_table(&table_id, &table, &mut metadatas)?;
        require_not_staged(&metadata_location)?;

        // ToDo: This is a small inefficiency: We fetch the secret even if it might
        // not be required based on the `data_access` parameter.
        let storage_secret = if let Some(secret_id) = storage_secret_ident {
            Some(
                state
                    .v1_state
                    .secrets
                    .get_secret_by_id(&secret_id)
                    .await?
                    .secret,
            )
        } else {
            None
        };
        let table_location = Location::from_str(table_metadata.location()).map_err(|e| {
            ErrorModel::internal(
                format!("Invalid table location in DB: {e}"),
                "InvalidViewLocation",
                Some(Box::new(e)),
            )
        })?;
        let load_table_result = LoadTableResult {
            metadata_location,
            metadata: table_metadata,
            config: Some(
                storage_profile
                    .generate_table_config(
                        &data_access,
                        storage_secret.as_ref(),
                        &table_location,
                        // TODO: This should be a permission based on authz
                        StoragePermissions::ReadWriteDelete,
                    )
                    .await?
                    .into(),
            ),
        };

        Ok(load_table_result)
    }

    /// Commit updates to a table
    #[allow(clippy::too_many_lines)]
    async fn commit_table(
        parameters: TableParameters,
        mut request: CommitTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;
        let TableParameters { table, prefix } = parameters;

        let table_ident = determine_table_ident(table, request.identifier)?;
        // Fix identifier in request for emitted event
        request.identifier = Some(table_ident.clone());
        validate_table_or_view_ident(&table_ident)?;
        validate_table_updates(&request.updates)?;

        // ------------------- AUTHZ -------------------
        let include_staged = true;
        let table_id = C::table_ident_to_id(
            warehouse_id,
            &table_ident,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_commit_table(
            &request_metadata,
            warehouse_id,
            table_id,
            Some(&table_ident.namespace),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_id = require_table_id(&table_ident, table_id)?;
        // serialize body before moving it
        let body = maybe_body_to_json(&request);
        let CommitTableRequest {
            identifier: _,
            requirements,
            updates,
        } = request;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let mut previous_table =
            C::load_tables(warehouse_id, vec![table_id], t.transaction()).await?;
        let previous_table = remove_table(&table_id, &table_ident, &mut previous_table)?;
        let warehouse = C::get_warehouse(warehouse_id, t.transaction()).await?;

        // Contract verification
        state
            .v1_state
            .contract_verifiers
            .check_table_updates(&updates, &previous_table.table_metadata)
            .await?
            .into_result()?;

        // Apply changes
        let new_metadata = apply_commit(
            previous_table.table_metadata,
            &previous_table.metadata_location,
            &requirements,
            updates,
        )?;
        let new_table_location = Location::from_str(new_metadata.location()).map_err(|e| {
            ErrorModel::internal(
                format!("Invalid new table location: {e}"),
                "InvalidTableLocation",
                Some(Box::new(e)),
            )
        })?;
        let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
        let new_metadata_location = previous_table.storage_profile.default_metadata_location(
            &new_table_location,
            &new_compression_codec,
            uuid::Uuid::now_v7(),
        );
        let commit = TableCommit {
            new_metadata,
            new_metadata_location,
        };
        C::commit_table_transaction(warehouse_id, vec![commit.clone()], t.transaction()).await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret =
            Self::maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

        // Write metadata file
        let file_io = warehouse.storage_profile.file_io(storage_secret.as_ref())?;
        write_metadata_file(
            &commit.new_metadata_location,
            &commit.new_metadata,
            new_compression_codec,
            &file_io,
        )
        .await?;

        t.commit().await?;
        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*previous_table.table_id),
                warehouse_id: *warehouse_id,
                name: table_ident.name,
                namespace: table_ident.namespace.to_url_string(),
                prefix: prefix
                    .map(crate::api::iceberg::types::Prefix::into_string)
                    .unwrap_or_default(),

                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "updateTable",
            state.v1_state.publisher.clone(),
        )
        .await;

        Ok(CommitTableResponse {
            metadata_location: commit.new_metadata_location.to_string(),
            metadata: commit.new_metadata,
            config: None,
        })
    }

    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let include_staged = true;
        let table_id = C::table_ident_to_id(
            warehouse_id,
            &table,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_drop_table(
            &request_metadata,
            warehouse_id,
            table_id,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;

        let table_id = table_id.ok_or_else(|| {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("Table does not exist in warehouse {warehouse_id}"))
                .r#type("TableNotFound".to_string())
                .build()
        })?;
        C::drop_table(table_id, transaction.transaction()).await?;

        // ToDo: Delete metadata files
        state
            .v1_state
            .contract_verifiers
            .check_drop(TabularIdentUuid::Table(*table_id))
            .await?
            .into_result()?;

        transaction.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id: *warehouse_id,
                name: table.name,
                namespace: table.namespace.to_url_string(),
                prefix: prefix
                    .map(crate::api::iceberg::types::Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            serde_json::Value::Null,
            "dropTable",
            state.v1_state.publisher,
        )
        .await;

        Ok(())
    }

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let include_staged = false;
        let table_id = C::table_ident_to_id(
            warehouse_id,
            &table,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        .transpose();
        // We can't fail before AuthZ.
        A::check_table_exists(
            &request_metadata,
            warehouse_id,
            Some(&table.namespace),
            table_id.as_ref().and_then(|x| x.as_ref().ok()).copied(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        if table_id.transpose()?.is_some() {
            Ok(())
        } else {
            Err(ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("Table does not exist in warehouse {warehouse_id}"))
                .r#type("TableNotFound".to_string())
                .build()
                .into())
        }
    }

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let body = maybe_body_to_json(&request);
        let RenameTableRequest {
            source,
            destination,
        } = request;
        validate_table_or_view_ident(&source)?;
        validate_table_or_view_ident(&destination)?;

        // ------------------- AUTHZ -------------------
        let include_staged = false;
        let source_id = C::table_ident_to_id(
            warehouse_id,
            &source,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        // We need to be allowed to delete the old table and create the new one
        let rename_check = A::check_rename_table(
            &request_metadata,
            warehouse_id,
            source_id,
            state.v1_state.auth.clone(),
        );
        let create_check = A::check_create_table(
            &request_metadata,
            warehouse_id,
            &destination.namespace,
            state.v1_state.auth,
        );
        futures::try_join!(rename_check, create_check)?;

        // ------------------- BUSINESS LOGIC -------------------
        if source == destination {
            return Ok(());
        }

        // This case should not happen after AuthZ.
        // Its rust though, so we have do to something.
        let source_id = source_id.ok_or_else(|| {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!(
                    "Source table does not exist in warehouse {warehouse_id}"
                ))
                .r#type("TableNotFound".to_string())
                .build()
        })?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        C::rename_table(
            warehouse_id,
            source_id,
            &source,
            &destination,
            transaction.transaction(),
        )
        .await?;

        state
            .v1_state
            .contract_verifiers
            .check_rename(TabularIdentUuid::Table(*source_id), &destination)
            .await?
            .into_result()?;

        transaction.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*source_id),
                warehouse_id: *warehouse_id,
                name: source.name,
                namespace: source.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "renameTable",
            state.v1_state.publisher.clone(),
        )
        .await;

        Ok(())
    }

    /// Commit updates to multiple tables in an atomic operation
    #[allow(clippy::too_many_lines)]
    // ToDo: Split some of this into helper functions
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        for change in &request.table_changes {
            validate_table_updates(&change.updates)?;
            change
                .identifier
                .as_ref()
                .map(validate_table_or_view_ident)
                .transpose()?;

            if change.identifier.is_none() {
                return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message(
                            "Table identifier is required for each change in the CommitTransactionRequest"
                                .to_string(),
                        )
                        .r#type("TableIdentifierRequiredForCommitTransaction".to_string())
                        .build()
                        .into());
            };
        }

        // ------------------- AUTHZ -------------------
        let include_staged = true;
        let identifiers = request
            .table_changes
            .iter()
            .filter_map(|change| change.identifier.as_ref())
            .collect::<HashSet<_>>();

        let table_ids = C::table_idents_to_ids(
            warehouse_id,
            identifiers,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching table ids".to_string())
                .r#type("TableIdsFetchError".to_string())
                .stack(
                    vec![e.error.message, e.error.r#type]
                        .into_iter()
                        .chain(e.error.stack.into_iter())
                        .collect(),
                )
                .build()
        })?;

        let auth_checks = table_ids
            .iter()
            .map(|(table_ident, table_id)| {
                A::check_commit_table(
                    &request_metadata,
                    warehouse_id,
                    *table_id,
                    Some(&table_ident.namespace),
                    state.v1_state.auth.clone(),
                )
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(auth_checks).await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_ids = require_table_ids(table_ids)?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let warehouse = C::get_warehouse(warehouse_id, transaction.transaction()).await?;

        // Store data for events before it is moved
        let mut events = vec![];
        let mut event_table_ids: Vec<(TableIdent, TableIdentUuid)> = vec![];
        let mut updates = vec![];
        for commit_table_request in &request.table_changes {
            if let Some(id) = &commit_table_request.identifier {
                if let Some(uuid) = table_ids.get(id) {
                    events.push(maybe_body_to_json(commit_table_request));
                    event_table_ids.push((id.clone(), *uuid));
                    updates.push(commit_table_request.updates.clone());
                }
            }
        }

        // Load old metadata
        let mut previous_metadatas = C::load_tables(
            warehouse_id,
            table_ids.values().copied(),
            transaction.transaction(),
        )
        .await?;

        // Apply changes
        let commits = request
            .table_changes
            .into_iter()
            .map(|change| {
                let table_ident = change.identifier.ok_or_else(||
                    // This should never happen due to validation
                    ErrorModel::internal(
                        "Change without Identifier",
                        "ChangeWithoutIdentifier",
                        None,
                    ))?;
                let table_id =
                    require_table_id(&table_ident, table_ids.get(&table_ident).copied())?;
                let previous_table =
                    remove_table(&table_id, &table_ident, &mut previous_metadatas)?;
                let new_metadata = apply_commit(
                    previous_table.table_metadata.clone(),
                    &previous_table.metadata_location,
                    &change.requirements,
                    change.updates.clone(),
                )?;
                let new_table_location =
                    Location::from_str(new_metadata.location()).map_err(|e| {
                        ErrorModel::internal(
                            format!("Invalid new table location: {e}"),
                            "InvalidTableLocation",
                            Some(Box::new(e)),
                        )
                    })?;
                let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
                let new_metadata_location =
                    previous_table.storage_profile.default_metadata_location(
                        &new_table_location,
                        &new_compression_codec,
                        uuid::Uuid::now_v7(),
                    );
                Ok(CommitContext {
                    new_metadata,
                    new_metadata_location,
                    new_compression_codec,
                    updates: change.updates,
                    previous_metadata: previous_table.table_metadata,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Commit changes in DB
        C::commit_table_transaction(
            warehouse_id,
            commits.iter().map(CommitContext::commit),
            transaction.transaction(),
        )
        .await?;

        // Check contract verification
        let futures = commits.iter().map(|c| {
            state
                .v1_state
                .contract_verifiers
                .check_table_updates(&c.updates, &c.previous_metadata)
        });

        futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .map(ContractVerificationOutcome::into_result)
            .collect::<Result<Vec<()>, ErrorModel>>()?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret =
            Self::maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

        // Write metadata files
        let file_io = warehouse.storage_profile.file_io(storage_secret.as_ref())?;

        let write_futures: Vec<_> = commits
            .iter()
            .map(|commit| {
                write_metadata_file(
                    &commit.new_metadata_location,
                    &commit.new_metadata,
                    commit.new_compression_codec,
                    &file_io,
                )
            })
            .collect();
        futures::future::try_join_all(write_futures).await?;

        transaction.commit().await?;
        let number_of_events = events.len();

        for (event_sequence_number, (body, (table_ident, table_id))) in
            events.into_iter().zip(event_table_ids).enumerate()
        {
            emit_change_event(
                EventMetadata {
                    tabular_id: TabularIdentUuid::Table(*table_id),
                    warehouse_id: *warehouse_id,
                    name: table_ident.name,
                    namespace: table_ident.namespace.to_url_string(),
                    prefix: prefix
                        .clone()
                        .map(|p| p.as_str().to_string())
                        .unwrap_or_default(),
                    num_events: number_of_events,
                    sequence_number: event_sequence_number,
                    trace_id: request_metadata.request_id,
                },
                body,
                "updateTable",
                state.v1_state.publisher.clone(),
            )
            .await;
        }

        Ok(())
    }
}

struct CommitContext {
    pub new_metadata: iceberg::spec::TableMetadata,
    pub new_metadata_location: Location,
    pub previous_metadata: iceberg::spec::TableMetadata,
    pub updates: Vec<TableUpdate>,
    pub new_compression_codec: CompressionCodec,
}

impl CommitContext {
    fn commit(&self) -> TableCommit {
        TableCommit {
            new_metadata: self.new_metadata.clone(),
            new_metadata_location: self.new_metadata_location.clone(),
        }
    }
}

fn determine_table_ident(
    parameters_ident: TableIdent,
    request_ident: Option<TableIdent>,
) -> Result<TableIdent> {
    // When querying a branch, spark sends something like the following as part of the `parameters`:
    // namespace: (<my>, <namespace>, <table_name>)
    // table_name: branch_<branch_name>

    if let Some(identifier) = request_ident {
        if identifier == parameters_ident {
            Ok(identifier)
        } else {
            let ns_parts = parameters_ident.namespace.clone().inner();
            let table_name_candidate = if ns_parts.len() >= 2 {
                NamespaceIdent::from_vec(
                    ns_parts.iter().take(ns_parts.len() - 1).cloned().collect(),
                )
                .ok()
                .map(|n| TableIdent::new(n, ns_parts.last().cloned().unwrap_or_default()))
            } else {
                None
            };

            if table_name_candidate != Some(identifier.clone()) {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message(
                        "Table identifier in path does not match the one in the request body"
                            .to_string(),
                    )
                    .r#type("TableIdentifierMismatch".to_string())
                    .build()
                    .into());
            }

            Ok(identifier)
        }
    } else {
        Ok(parameters_ident)
    }
}

fn determine_table_location(
    namespace: &GetNamespaceResponse,
    request_table_location: Option<String>,
    table_id: TabularIdentUuid,
    storage_profile: &StorageProfile,
) -> Result<Location> {
    let request_table_location = request_table_location
        .map(|l| Location::from_str(&l))
        .transpose()
        .map_err(|e| {
            ErrorModel::bad_request(
                format!("Specified table location is invalid: {e}"),
                "InvalidTableLocation",
                Some(Box::new(e)),
            )
        })?;

    if let Some(location) = request_table_location {
        if !storage_profile.is_allowed_location(&location) {
            return Err(ErrorModel::bad_request(
                format!("Specified table location is not allowed: {location}"),
                "InvalidTableLocation",
                None,
            )
            .into());
        }

        Ok(location)
    } else {
        let namespace_props = NamespaceProperties::from_props_unchecked(
            namespace.properties.clone().unwrap_or_default(),
        );

        let namespace_location = match namespace_props.get_location() {
            Some(location) => location,
            None => storage_profile
                .default_namespace_location(namespace.namespace_id)
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to generate default namespace location",
                        "InvalidDefaultNamespaceLocaiton",
                        Some(Box::new(e)),
                    )
                })?,
        };

        Ok(storage_profile.default_tabular_location(&namespace_location, table_id))
    }
}

fn require_table_ids(
    table_ids: HashMap<TableIdent, Option<TableIdentUuid>>,
) -> Result<HashMap<TableIdent, TableIdentUuid>> {
    table_ids
        .into_iter()
        .map(|(table_ident, table_id)| {
            if let Some(table_id) = table_id {
                Ok((table_ident, table_id))
            } else {
                Err(ErrorModel::not_found(
                    format!("Table {table_ident:#?} does not exist."),
                    "TableNotFound",
                    None,
                )
                .into())
            }
        })
        .collect::<Result<std::collections::HashMap<_, _>>>()
}

fn require_table_id(
    table_ident: &TableIdent,
    table_id: Option<TableIdentUuid>,
) -> Result<TableIdentUuid> {
    table_id.ok_or_else(|| {
        ErrorModel::not_found(
            format!("Table {table_ident:#?} does not exist."),
            "TableNotFound",
            None,
        )
        .into()
    })
}

fn require_not_staged(metadata_location: &Option<String>) -> Result<()> {
    if metadata_location.is_none() {
        return Err(ErrorModel::not_found(
            "Table not found or staged.",
            "TableNotFoundOrStaged",
            None,
        )
        .into());
    }

    Ok(())
}

fn remove_table<T>(
    table_id: &TableIdentUuid,
    table_ident: &TableIdent,
    metadatas: &mut HashMap<TableIdentUuid, T>,
) -> Result<T> {
    metadatas
        .remove(table_id)
        .ok_or_else(|| {
            ErrorModel::not_found(
                format!("Table {table_ident:#?} does not exist."),
                "TableNotFound",
                None,
            )
        })
        .map_err(Into::into)
}

pub(crate) fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
    if status != WarehouseStatus::Active {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Warehouse is not active".to_string())
            .r#type("WarehouseNotActive".to_string())
            .build()
            .into());
    }
    Ok(())
}

async fn emit_change_event(
    parameters: EventMetadata,
    body: serde_json::Value,
    operation_id: &str,
    publisher: CloudEventsPublisher,
) {
    let _ = publisher
        .publish(Uuid::now_v7(), operation_id, body, parameters)
        .await;
}

// Quick validation of properties for early fails.
// Full validation is performed when changes are applied.
fn validate_table_updates(updates: &Vec<TableUpdate>) -> Result<()> {
    for update in updates {
        match update {
            TableUpdate::SetProperties { updates } => {
                validate_table_properties(updates.keys())?;
            }
            TableUpdate::RemoveProperties { removals } => {
                validate_table_properties(removals)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn validate_lowercase_property(prop: &str) -> Result<()> {
    if prop != prop.to_lowercase() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message(format!("The property '{prop}' is not all lowercase."))
            .r#type("PropertyNotLowercase")
            .build()
            .into());
    }
    Ok(())
}

pub(crate) fn validate_table_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if (prop.starts_with("write.metadata") && prop != "write.metadata.compression-codec")
            || prop.starts_with("write.data.path")
        {
            return Err(ErrorModel::conflict(
                format!("Properties contain unsupported property: '{prop}'"),
                "FailedToSetProperties",
                None,
            )
            .into());
        }
        validate_lowercase_property(prop)?;
    }

    Ok(())
}

pub(crate) fn validate_table_or_view_ident(table: &TableIdent) -> Result<()> {
    let TableIdent {
        ref namespace,
        ref name,
    } = &table;
    validate_namespace_ident(namespace)?;

    if name.is_empty() {
        return Err(ErrorModel::bad_request(
            "name of the identifier cannot be empty",
            "IdentifierNameEmpty",
            None,
        )
        .into());
    }
    Ok(())
}

// This function does not return a result but serde_json::Value::Null if serialization
// fails. This follows the rationale that we'll likely end up ignoring the error in the API handler
// anyway since we already effected the change and only the event emission about the change failed.
// Given that we are serializing stuff we've received as a json body and also successfully
// processed, it's unlikely to cause issues.
pub(crate) fn maybe_body_to_json(request: impl Serialize) -> serde_json::Value {
    if let Ok(body) = serde_json::to_value(&request) {
        body
    } else {
        tracing::warn!("Serializing the request body to json failed, this is very unexpected. It will not be part of any emitted Event.");
        serde_json::Value::Null
    }
}
