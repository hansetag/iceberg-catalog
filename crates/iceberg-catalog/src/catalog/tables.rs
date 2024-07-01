use std::collections::{HashMap, HashSet};
use std::vec;

use crate::api::iceberg::v1::{
    ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateTableRequest, DataAccess, ErrorModel, ListTablesResponse, LoadTableResult,
    NamespaceParameters, PaginationQuery, Prefix, RegisterTableRequest, RenameTableRequest, Result,
    TableIdent, TableParameters,
};
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::{NamespaceIdent, TableUpdate};
use serde::Serialize;
use uuid::Uuid;

use super::{
    io::write_metadata_file, namespace::validate_namespace_ident, require_warehouse_id,
    CatalogServer,
};
use crate::service::contract_verification::{ContractVerification, ContractVerificationOutcome};
use crate::service::event_publisher::{CloudEventsPublisher, EventMetadata};
use crate::service::storage::StorageCredential;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, CreateTableResponse,
    LoadTableResponse as CatalogLoadTableResult, State, Transaction,
};
use crate::service::{GetWarehouseResponse, TableIdentUuid, WarehouseStatus};

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::tables::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        _query: PaginationQuery,
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
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let include_staged = false;
        let tables = C::list_tables(
            &warehouse_id,
            &namespace,
            include_staged,
            state.v1_state.catalog,
        )
        .await?;

        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: tables.into_iter().map(|t| t.1).collect(),
        })
    }

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
        require_no_location_specified(&request.location)?;

        if let Some(properties) = &request.properties {
            validate_table_properties(properties.keys())?;
        }

        // ------------------- AUTHZ -------------------
        A::check_create_table(
            &request_metadata,
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id =
            C::namespace_ident_to_id(&warehouse_id, &namespace, state.v1_state.catalog.clone())
                .await?
                .ok_or(
                    ErrorModel::builder()
                        .code(StatusCode::NOT_FOUND.into())
                        .message("Namespace does not exist".to_string())
                        .r#type("NamespaceNotFound".to_string())
                        .build(),
                )?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let GetWarehouseResponse {
            id: _,
            name: _,
            project_id: _,
            storage_profile,
            storage_secret_id,
            status,
        } = C::get_warehouse(&warehouse_id, transaction.transaction()).await?;
        require_active_warehouse(status)?;

        let table_id: TabularIdentUuid = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        let table_location = storage_profile.tabular_location(&namespace_id, &table_id);

        // This is the only place where we change request
        request.location = Some(table_location.clone());
        let request = request; // Make it non-mutable again for our sanity

        // If stage-create is true, we should not create the metadata file
        let metadata_location = if request.stage_create.unwrap_or(false) {
            None
        } else {
            let metadata_id = uuid::Uuid::now_v7();
            Some(storage_profile.metadata_location(&table_location, &metadata_id))
        };

        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let CreateTableResponse { table_metadata } = C::create_table(
            &namespace_id,
            &table,
            &TableIdentUuid::from(*table_id),
            request,
            metadata_location.as_ref(),
            transaction.transaction(),
        )
        .await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = &storage_secret_id {
            Some(
                S::get_secret_by_id(secret_id, state.v1_state.secrets)
                    .await?
                    .secret,
            )
        } else {
            None
        };

        if let Some(metadata_location) = &metadata_location {
            let file_io = storage_profile.file_io(storage_secret.as_ref())?;
            write_metadata_file(metadata_location, &table_metadata, &file_io).await?;
        }

        // Generate the storage profile. This requires the storage secret
        // because the table config might contain vended-credentials based
        // on the `data_access` parameter.
        // ToDo: There is a small inefficiency here: If storage credentials
        // are not required because of i.e. remote-signing and if this
        // is a stage-create, we still fetch the secret.
        let config = storage_profile
            .generate_table_config(
                &warehouse_id,
                &namespace_id,
                &TableIdentUuid::from(*table_id),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;
        let load_table_result = LoadTableResult {
            metadata_location,
            metadata: table_metadata,
            config: Some(config),
        };

        // Metadata file written, now we can commit the transaction
        transaction.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id: *warehouse_id.as_uuid(),
                name: table.name.clone(),
                namespace: table.namespace.encode_in_url(),
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
        // ToDo: Should we support this?
        // May be problematic if we don't know the location
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
            &warehouse_id,
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
            &warehouse_id,
            Some(&table.namespace),
            table_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let CatalogLoadTableResult {
            table_id,
            namespace_id,
            table_metadata,
            metadata_location,
            storage_secret_ident,
            storage_profile,
        } = C::load_table(&warehouse_id, &table, state.v1_state.catalog).await?;

        // ToDo: This is a small inefficiency: We fetch the secret even if it might
        // not be required based on the `data_access` parameter.
        let storage_secret = if let Some(secret_id) = storage_secret_ident {
            Some(
                S::get_secret_by_id(&secret_id, state.v1_state.secrets)
                    .await?
                    .secret,
            )
        } else {
            None
        };

        let load_table_result = LoadTableResult {
            metadata_location,
            metadata: table_metadata,
            config: Some(
                storage_profile
                    .generate_table_config(
                        &warehouse_id,
                        &namespace_id,
                        &table_id,
                        &data_access,
                        storage_secret.as_ref(),
                    )
                    .await?,
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

        if let Some(identifier) = &request.identifier {
            if identifier != &parameters.table {
                // When querying a branch, spark sends something like:
                // namespace: (<my>, <namespace>, <table_name>)
                // table_name: branch_<branch_name>
                let ns_parts = parameters.table.namespace.clone().inner();
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
            }
        }

        if request.identifier.is_none() {
            request.identifier = Some(parameters.table.clone());
        }
        if let Some(ref mut identifier) = request.identifier {
            validate_table_or_view_ident(identifier)?;
        }
        // Make it non-mutable again for our sanity
        let request = request;

        let CommitTableRequest {
            identifier,
            // If requirements are validated in the future
            // also add validation to commit_transaction
            requirements: _,
            updates,
        } = &request;

        validate_table_updates(updates)?;
        identifier
            .as_ref()
            .map(validate_table_or_view_ident)
            .transpose()?;

        if let Some(identifier) = identifier {
            if identifier != &parameters.table {
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
        }

        // ------------------- AUTHZ -------------------
        let include_staged = true;
        let table_id = C::table_ident_to_id(
            &warehouse_id,
            &parameters.table,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_commit_table(
            &request_metadata,
            &warehouse_id,
            table_id.as_ref(),
            Some(&parameters.table.namespace),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_id = table_id.ok_or_else(|| {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("Table does not exist in warehouse {warehouse_id}"))
                .r#type("TableNotFound".to_string())
                .build()
        })?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let updates = updates.clone();

        let transaction_request = CommitTransactionRequest {
            table_changes: vec![request],
        };
        let table_ids = HashMap::from_iter(vec![(parameters.table.clone(), table_id)]);
        let result = C::commit_table_transaction(
            &warehouse_id,
            transaction_request,
            &table_ids,
            transaction.transaction(),
        )
        .await?;

        if result.len() > 1 {
            return Err(ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("More than one result from commit_table_transaction".to_string())
                .r#type("MoreThanOneResultFromCommitTableTransaction".to_string())
                .build()
                .into());
        }
        // Get the first and only result
        let result = result.into_iter().next().ok_or(
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("No result from commit_table_transaction".to_string())
                .r#type("NoResultFromCommitTableTransaction".to_string())
                .build(),
        )?;
        state
            .v1_state
            .contract_verifiers
            .check_table_updates(&updates, &result.previous_table_metadata)
            .await?
            .into_result()?;
        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = &result.storage_config.storage_secret_ident {
            Some(
                S::get_secret_by_id(secret_id, state.v1_state.secrets)
                    .await?
                    .secret,
            )
        } else {
            None
        };

        // Write metadata file
        let file_io = result
            .storage_config
            .storage_profile
            .file_io(storage_secret.as_ref())?;
        write_metadata_file(
            &result.commit_response.metadata_location,
            &result.commit_response.metadata,
            &file_io,
        )
        .await?;

        transaction.commit().await?;
        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id.as_uuid()),
                warehouse_id: *warehouse_id.as_uuid(),
                name: parameters.table.name,
                namespace: parameters.table.namespace.encode_in_url(),
                prefix: parameters
                    .prefix
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

        Ok(result.commit_response)
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
            &warehouse_id,
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
            &warehouse_id,
            table_id.as_ref(),
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
        C::drop_table(&warehouse_id, &table_id, transaction.transaction()).await?;

        // ToDo: Delete metadata files
        state
            .v1_state
            .contract_verifiers
            .check_drop(TabularIdentUuid::Table(table_id.into_uuid()))
            .await?
            .into_result()?;

        transaction.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id.as_uuid()),
                warehouse_id: *warehouse_id.as_uuid(),
                name: table.name,
                namespace: table.namespace.encode_in_url(),
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
            &warehouse_id,
            &table,
            include_staged,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_table_exists(
            &request_metadata,
            &warehouse_id,
            Some(&table.namespace),
            table_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        if table_id.is_some() {
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
            &warehouse_id,
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
            &warehouse_id,
            source_id.as_ref(),
            state.v1_state.auth.clone(),
        );
        let create_check = A::check_create_table(
            &request_metadata,
            &warehouse_id,
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
            &warehouse_id,
            &source_id,
            &source,
            &destination,
            transaction.transaction(),
        )
        .await?;

        state
            .v1_state
            .contract_verifiers
            .check_rename(TabularIdentUuid::Table(source_id.into_uuid()), &destination)
            .await?
            .into_result()?;

        transaction.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(source_id.into_uuid()),
                warehouse_id: *warehouse_id.as_uuid(),
                name: source.name,
                namespace: source.namespace.encode_in_url(),
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
        let CommitTransactionRequest { table_changes } = &request;
        for change in table_changes {
            let CommitTableRequest {
                identifier,
                // If requirements are validated in the future
                // also add validation to commit_table
                requirements: _,
                updates,
            } = change;

            validate_table_updates(updates)?;
            identifier
                .as_ref()
                .map(validate_table_or_view_ident)
                .transpose()?;

            if identifier.is_none() {
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
        let identifiers = table_changes
            .iter()
            .filter_map(|change| change.identifier.as_ref())
            .collect::<HashSet<_>>();

        let table_ids = C::table_idents_to_ids(
            &warehouse_id,
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
                .stack(Some(
                    vec![e.error.message, e.error.r#type]
                        .into_iter()
                        .chain(e.error.stack.unwrap_or_default().into_iter())
                        .collect(),
                ))
                .build()
        })?;

        let auth_checks = table_ids
            .iter()
            .map(|(table_ident, table_id)| {
                A::check_commit_table(
                    &request_metadata,
                    &warehouse_id,
                    table_id.as_ref(),
                    Some(&table_ident.namespace),
                    state.v1_state.auth.clone(),
                )
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(auth_checks).await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_ids = table_ids
            .into_iter()
            .map(|(table_ident, table_id)| {
                if let Some(table_id) = table_id {
                    Ok((table_ident, table_id))
                } else {
                    Err(ErrorModel::builder()
                        .code(StatusCode::NOT_FOUND.into())
                        .message(format!(
                            "Table {table_ident:#?} does not exist in warehouse {warehouse_id}"
                        ))
                        .r#type("TableNotFound".to_string())
                        .build()
                        .into())
                }
            })
            .collect::<Result<std::collections::HashMap<_, _>>>()?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;

        // serialize request body before moving it here
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

        let commit_response = C::commit_table_transaction(
            &warehouse_id,
            request,
            &table_ids,
            transaction.transaction(),
        )
        .await?;
        let futures = updates
            .iter()
            .zip(&commit_response)
            .map(|(update, response)| {
                state
                    .v1_state
                    .contract_verifiers
                    .check_table_updates(update, &response.previous_table_metadata)
            });

        futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .map(ContractVerificationOutcome::into_result)
            .collect::<Result<Vec<()>, ErrorModel>>()?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        // Fetch all secrets concurrently
        let storage_secrets = futures::future::try_join_all(
            commit_response
                .iter()
                .filter_map(|r| r.storage_config.storage_secret_ident.as_ref())
                //unique
                .collect::<HashSet<_>>()
                .into_iter()
                .map(|secret_id| S::get_secret_by_id(secret_id, state.v1_state.secrets.clone())),
        )
        .await?;
        let storage_secrets: HashMap<_, StorageCredential> = storage_secrets
            .into_iter()
            .map(|r| (r.secret_id, r.secret))
            .collect();

        // Write metadata files
        let commit_response_with_io = commit_response
            .iter()
            .map(|r| {
                let storage_secret = r
                    .storage_config
                    .storage_secret_ident
                    .as_ref()
                    .and_then(|secret_id| storage_secrets.get(secret_id))
                    .cloned();
                let file_io = r
                    .storage_config
                    .storage_profile
                    .file_io(storage_secret.as_ref())
                    .map(|io| (r, io));
                file_io
            })
            .collect::<Result<Vec<_>>>()?;

        let mut write_futures = vec![];
        for response in &commit_response_with_io {
            let (r, io) = response;
            write_futures.push(write_metadata_file(
                &r.commit_response.metadata_location,
                &r.commit_response.metadata,
                io,
            ));
        }

        futures::future::try_join_all(write_futures).await?;

        transaction.commit().await?;
        let number_of_events = events.len();

        for (event_sequence_number, (body, (table_ident, table_id))) in
            events.into_iter().zip(event_table_ids).enumerate()
        {
            emit_change_event(
                EventMetadata {
                    tabular_id: TabularIdentUuid::Table(table_id.into_uuid()),
                    warehouse_id: *warehouse_id.as_uuid(),
                    name: table_ident.name,
                    namespace: table_ident.namespace.encode_in_url(),
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

fn require_no_location_specified(location: &Option<String>) -> Result<()> {
    if location.is_some() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Specifying a Table `location` is not supported. Location is managed by the Catalog.".to_string())
            .r#type("LocationNotSupported".to_string())
            .build()
            .into());
    }
    Ok(())
}

fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
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

fn validate_table_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if prop != &prop.to_lowercase() {
            validate_lowercase_property(prop)?;
        }
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
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("name of the identifier cannot be empty".to_string())
            .r#type("IdentifierNameEmpty".to_string())
            .build()
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
