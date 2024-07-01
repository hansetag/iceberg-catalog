use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, ViewParameters,
};
use crate::catalog::io::write_metadata_file;
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{IcebergErrorResponse, UpgradeFormatVersionUpdate, ViewUpdate};
use iceberg_ext::catalog::ViewRequirement;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use super::tables::{
    maybe_body_to_json, require_active_warehouse, require_no_location_specified,
    validate_table_or_view_ident, validate_table_properties,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
use crate::service::contract_verification::ContractVerification;
use crate::service::event_publisher::EventMetadata;
use crate::service::storage::StorageCredential;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, GetWarehouseResponse, State, TableIdentUuid,
    Transaction,
};

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::views::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all view identifiers underneath a given namespace
    async fn list_views(
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
        A::check_list_views(
            &request_metadata,
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------

        let views = C::list_views(&warehouse_id, &namespace, state.v1_state.catalog.clone())
            .await
            .unwrap();

        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: views.into_iter().map(|t| t.1).collect(),
        })
    }

    // TODO: split up into smaller functions
    #[allow(clippy::too_many_lines)]
    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let view = TableIdent::new(namespace.clone(), request.name.clone());

        validate_table_or_view_ident(&view)?;
        require_no_location_specified(&request.location)?;

        if request.location.is_some() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Specifying a View `location` is not supported. Location is managed by the Catalog.".to_string())
                .r#type("LocationNotSupported".to_string())
                .build()
                .into());
        }

        validate_view_properties(request.properties.keys())?;

        if request.view_version.representations().is_empty() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("View must have at least one query.".to_string())
                .r#type("EmptyView".to_string())
                .build()
                .into());
        }

        // ------------------- AUTHZ -------------------
        A::check_create_view(
            &request_metadata,
            &warehouse_id,
            &namespace,
            state.v1_state.auth.clone(),
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

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let GetWarehouseResponse {
            id: _,
            name: _,
            project_id: _,
            storage_profile,
            storage_secret_id,
            status,
        } = C::get_warehouse(&warehouse_id, transaction.transaction()).await?;
        require_active_warehouse(status)?;

        let view_id: TabularIdentUuid = TabularIdentUuid::View(uuid::Uuid::now_v7());

        let view_location = storage_profile.tabular_location(&namespace_id, &view_id);
        let mut request = request;
        let metadata_location = storage_profile.metadata_location(&view_location, &view_id);
        request.location = Some(view_location);
        let request = request;
        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let metadata = C::create_view(
            &warehouse_id,
            &namespace_id,
            &view_id,
            &view,
            request,
            &metadata_location,
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

        let file_io = storage_profile.file_io(storage_secret.as_ref())?;
        write_metadata_file(metadata_location.as_str(), &metadata, &file_io).await?;
        tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

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
                &TableIdentUuid::from(*view_id),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;

        transaction.commit().await?;

        let _ = state
            .v1_state
            .publisher
            .publish(
                Uuid::now_v7(),
                "createView",
                body,
                EventMetadata {
                    tabular_id: TabularIdentUuid::View(*view_id),
                    warehouse_id: *warehouse_id.as_uuid(),
                    name: view.name.clone(),
                    namespace: view.namespace.encode_in_url(),
                    prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                    num_events: 1,
                    sequence_number: 0,
                    trace_id: request_metadata.request_id,
                },
            )
            .await;

        let load_table_result = LoadViewResult {
            metadata_location,
            metadata,
            config: Some(config),
        };

        return Ok(load_table_result);
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        // ToDo: Remove workaround when hierarchical namespaces are supported.
        // It is important for now to throw a 404 if a table cannot be found,
        // because spark might check if `table`.`branch` exists, which should return 404.
        // Only then will it treat it as a branch.
        // 404 is returned by the logic in the remainder of this function. Here, we only
        // need to make sure that we don't fail prematurely on longer namespaces.
        match validate_table_or_view_ident(&view) {
            Ok(()) => {}
            Err(e) => {
                if e.error.r#type != *"NamespaceDepthExceeded" {
                    return Err(e);
                }
            }
        }

        // ------------------- AUTHZ -------------------
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        A::check_load_view(
            &request_metadata,
            &warehouse_id,
            Some(&view.namespace),
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = C::namespace_ident_to_id(
            &warehouse_id,
            &view.namespace,
            state.v1_state.catalog.clone(),
        )
        .await?
        .ok_or(
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace does not exist".to_string())
                .r#type("NamespaceNotFound".to_string())
                .build(),
        )?;

        let Some(view_id) = view_id else {
            return Err(ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("View does not exist in warehouse {warehouse_id}"))
                .r#type("ViewNotFound".to_string())
                .build()
                .into());
        };
        let mut transaction = C::Transaction::begin_read(state.v1_state.catalog).await?;

        let GetWarehouseResponse {
            id: _,
            name: _,
            project_id: _,
            storage_profile,
            storage_secret_id,
            status,
        } = C::get_warehouse(&warehouse_id, transaction.transaction()).await?;
        require_active_warehouse(status)?;

        let view_metadata = C::load_view(view_id, transaction.transaction()).await?;

        // TODO: should load_view return this? What's there to gain?
        let location = storage_profile
            .tabular_location(&namespace_id, &TabularIdentUuid::View(view_id.into_uuid()));
        let metadata_location = storage_profile.metadata_location(&location, &view_metadata.uuid());

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret: Option<StorageCredential> = if let Some(secret_id) = &storage_secret_id
        {
            Some(
                S::get_secret_by_id(secret_id, state.v1_state.secrets)
                    .await?
                    .secret,
            )
        } else {
            None
        };

        let access = storage_profile
            .generate_table_config(
                &warehouse_id,
                &namespace_id,
                &view_metadata.uuid().into(),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;
        let load_table_result = LoadViewResult {
            metadata_location,
            metadata: view_metadata,
            config: Some(access),
        };
        eprintln!("{:?}", load_table_result);
        transaction.commit().await?;
        Ok(load_table_result)
    }

    /// Commit updates to a view
    // TODO: break up into smaller fns
    #[allow(clippy::too_many_lines)]
    async fn commit_view(
        parameters: ViewParameters,
        mut request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;

        if let Some(identifier) = &request.identifier {
            if identifier != &parameters.view {
                // When querying a branch, spark sends something like:
                // namespace: (<my>, <namespace>, <table_name>)
                // table_name: branch_<branch_name>
                let ns_parts = parameters.view.namespace.clone().inner();
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
            request.identifier = Some(parameters.view.clone());
        }
        if let Some(ref mut identifier) = request.identifier {
            validate_table_or_view_ident(identifier)?;
        }

        let CommitViewRequest {
            identifier,
            requirements,
            updates,
        } = &request;

        validate_view_updates_updates(updates)?;

        identifier
            .as_ref()
            .map(validate_table_or_view_ident)
            .transpose()?;

        if let Some(identifier) = identifier {
            if identifier != &parameters.view {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message(
                        "View identifier in path does not match the one in the request body"
                            .to_string(),
                    )
                    .r#type("ViewIdentifierMismatch".to_string())
                    .build()
                    .into());
            }
        }

        // ------------------- AUTHZ -------------------
        let table_id = C::view_ident_to_id(
            &warehouse_id,
            &parameters.view,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_commit_view(
            &request_metadata,
            &warehouse_id,
            table_id.as_ref(),
            Some(&parameters.view.namespace),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = C::namespace_ident_to_id(
            &warehouse_id,
            &parameters.view.namespace,
            state.v1_state.catalog.clone(),
        )
        .await?
        .ok_or(
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace does not exist".to_string())
                .r#type("NamespaceNotFound".to_string())
                .build(),
        )?;

        let view_id = table_id.ok_or_else(|| {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("Table does not exist in warehouse {warehouse_id}"))
                .r#type("TableNotFound".to_string())
                .build()
        })?;

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

        for assertion in requirements.as_deref().unwrap_or(&[]) {
            match assertion {
                ViewRequirement::AssertViewUuid(uuid) => {
                    if uuid.uuid != view_id.into_uuid() {
                        return Err(ErrorModel::builder()
                            .code(StatusCode::BAD_REQUEST.into())
                            .message("View UUID does not match".to_string())
                            .r#type("ViewUuidMismatch".to_string())
                            .build()
                            .into());
                    }
                }
            }
        }

        let before_update_metadata = C::load_view(view_id, transaction.transaction()).await?;

        state
            .v1_state
            .contract_verifiers
            .check_view_updates(updates, &before_update_metadata)
            .await?
            .into_result()?;
        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let mut last_added_schema_id = None;
        let mut last_version = None;
        for upd in request.updates {
            match upd {
                ViewUpdate::AssignUuid(_) => {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message("Assigning UUIDs is not supported".to_string())
                        .r#type("AssignUuidNotSupported".to_string())
                        .build()
                        .into());
                }
                ViewUpdate::SetLocation(_) => {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message("Setting location is not supported".to_string())
                        .r#type("SetLocationNotSupported".to_string())
                        .build()
                        .into());
                }

                ViewUpdate::UpgradeFormatVersion(UpgradeFormatVersionUpdate { format_version }) => {
                    match format_version {
                        1 => {
                            // No-op
                        }
                        _ => {
                            return Err(ErrorModel::builder()
                                .code(StatusCode::BAD_REQUEST.into())
                                .message("Format version not supported".to_string())
                                .r#type("FormatVersionNotSupported".to_string())
                                .build()
                                .into());
                        }
                    }
                }
                ViewUpdate::AddSchema(iceberg_ext::catalog::rest::AddSchemaUpdate {
                    schema,
                    last_column_id: _,
                }) => {
                    let new_id =
                        C::add_view_schema(&view_id, Arc::new(schema), transaction.transaction())
                            .await?;
                    last_added_schema_id = Some(new_id);
                }
                ViewUpdate::SetProperties(props) => {
                    C::insert_view_properties(&view_id, &props.updates, transaction.transaction())
                        .await?;
                }
                ViewUpdate::RemoveProperties(props) => {
                    C::delete_view_properties(&view_id, &props.removals, transaction.transaction())
                        .await?;
                }
                ViewUpdate::AddViewVersion(vv) => {
                    let mut view_version = vv.view_version;
                    if view_version.schema_id == -1 {
                        view_version.schema_id =
                            last_added_schema_id.ok_or(IcebergErrorResponse::from(
                                ErrorModel::builder()
                                    .code(StatusCode::BAD_REQUEST.into())
                                    .message(
                                        "-1 is only valid as a schema if one is added before"
                                            .to_string(),
                                    )
                                    .r#type("SchemaIdNotSet".to_string())
                                    .build(),
                            ))?;
                    }
                    last_version = Some(
                        C::create_view_version(
                            &view_id,
                            Arc::new(view_version),
                            transaction.transaction(),
                        )
                        .await?,
                    );
                }
                ViewUpdate::SetCurrentViewVersion(scvv) => {
                    let version_id =
                        if scvv.view_version_id == -1 {
                            last_version.as_ref().ok_or(IcebergErrorResponse::from(
                          ErrorModel::builder()
                              .code(StatusCode::BAD_REQUEST.into())
                              .message(
                                  "-1 is only valid as a view version if one is added before"
                                      .to_string(),
                              )
                              .r#type("ViewVersionIdNotSet".to_string())
                              .build(),
                      ))?.version_id
                        } else {
                            i64::from(scvv.view_version_id)
                        };
                    C::set_current_view_version(&view_id, version_id, transaction.transaction())
                        .await?;
                }
            }
        }
        let tab_location = storage_profile
            .tabular_location(&namespace_id, &TabularIdentUuid::View(view_id.into_uuid()));
        let metadata_location = storage_profile.metadata_location(&tab_location, &Uuid::now_v7());

        C::update_view_metadata_location(&view_id, &metadata_location, transaction.transaction())
            .await?;
        let updated_meta = C::load_view(view_id, transaction.transaction()).await?;

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

        let file_io = storage_profile.file_io(storage_secret.as_ref())?;
        write_metadata_file(metadata_location.as_str(), &updated_meta, &file_io).await?;
        tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);
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
                &TableIdentUuid::from(view_id.into_uuid()),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;
        transaction.commit().await?;

        let _ = state
            .v1_state
            .publisher
            .publish(
                Uuid::now_v7(),
                "commitView",
                body,
                EventMetadata {
                    tabular_id: TabularIdentUuid::View(view_id.into_uuid()),
                    warehouse_id: *warehouse_id.as_uuid(),
                    name: parameters.view.name,
                    namespace: parameters.view.namespace.encode_in_url(),
                    prefix: parameters
                        .prefix
                        .map(Prefix::into_string)
                        .unwrap_or_default(),
                    num_events: 1,
                    sequence_number: 0,
                    trace_id: request_metadata.request_id,
                },
            )
            .await;

        return Ok(LoadViewResult {
            metadata_location,
            metadata: updated_meta,
            config: Some(config),
        });
    }

    #[instrument(skip(state))]
    /// Drop a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;

        // ------------------- AUTHZ -------------------
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        A::check_drop_view(
            &request_metadata,
            &warehouse_id,
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let view_id = view_id.ok_or_else(|| {
            tracing::debug!("View does not exist.");
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("View does not exist in warehouse {warehouse_id}"))
                .r#type("ViewNotFound".to_string())
                .build()
        })?;

        state
            .v1_state
            .contract_verifiers
            .check_drop(TabularIdentUuid::View(view_id.into_uuid()))
            .await?
            .into_result()?;

        tracing::debug!("Proceeding to delete view");
        C::drop_view(&warehouse_id, &view_id, transaction.transaction()).await?;

        // TODO: Delete metadata files
        transaction.commit().await?;

        let _ = state
            .v1_state
            .publisher
            .publish(
                Uuid::now_v7(),
                "dropView",
                serde_json::Value::Null,
                EventMetadata {
                    tabular_id: TabularIdentUuid::View(view_id.into_uuid()),
                    warehouse_id: *warehouse_id.as_uuid(),
                    name: view.name.clone(),
                    namespace: view.namespace.encode_in_url(),
                    prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                    num_events: 1,
                    sequence_number: 0,
                    trace_id: request_metadata.request_id,
                },
            )
            .await;

        return Ok(());
    }

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;
        // TODO: authz
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            .ok()
            .flatten();

        A::check_view_exists(
            &request_metadata,
            &warehouse_id,
            Some(&view.namespace),
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        if view_id.is_some() {
            return Ok(());
        }

        Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("View does not exist in warehouse {warehouse_id}"))
            .r#type("ViewNotFound".to_string())
            .build()
            .into())
    }

    /// Rename a view
    async fn rename_view(
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
        let source_id = C::view_ident_to_id(&warehouse_id, &source, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        // We need to be allowed to delete the old table and create the new one
        let rename_check = A::check_rename_view(
            &request_metadata,
            &warehouse_id,
            source_id.as_ref(),
            state.v1_state.auth.clone(),
        );
        let create_check = A::check_create_view(
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
                    "Source view does not exist in warehouse {warehouse_id}"
                ))
                .r#type("ViewNotFound".to_string())
                .build()
        })?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        C::rename_view(
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
            .check_rename(TabularIdentUuid::View(source_id.into_uuid()), &destination)
            .await?
            .into_result()?;

        transaction.commit().await?;

        let _ = state
            .v1_state
            .publisher
            .publish(
                Uuid::now_v7(),
                "renameView",
                body,
                EventMetadata {
                    tabular_id: TabularIdentUuid::View(source_id.into_uuid()),
                    warehouse_id: *warehouse_id.as_uuid(),
                    name: source.name,
                    namespace: source.namespace.encode_in_url(),
                    prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                    num_events: 1,
                    sequence_number: 0,
                    trace_id: request_metadata.request_id,
                },
            )
            .await;

        return Ok(());
    }
}

fn validate_view_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    validate_table_properties(properties)
}

fn validate_view_updates_updates(updates: &Vec<ViewUpdate>) -> Result<()> {
    for update in updates {
        match update {
            ViewUpdate::SetProperties(updates) => {
                validate_view_properties(updates.updates.keys())?;
            }
            ViewUpdate::RemoveProperties(removals) => {
                validate_view_properties(removals.removals.iter())?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::v1::{views, DataAccess, NamespaceParameters, Prefix, ViewParameters};
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use crate::implementations::postgres::secrets::Server;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::implementations::postgres::{Catalog, CatalogState, SecretsState};
    use crate::implementations::{AllowAllAuthState, AllowAllAuthZHandler};
    use crate::service::contract_verification::ContractVerifiers;
    use crate::service::event_publisher::CloudEventsPublisher;
    use crate::service::storage::{StorageProfile, TestProfile};
    use crate::service::State;
    use crate::{implementations, WarehouseIdent};

    use iceberg::{NamespaceIdent, TableIdent};

    use iceberg_ext::catalog::rest::{CommitViewRequest, CreateViewRequest, LoadViewResult};

    use serde_json::json;
    use sqlx::PgPool;

    use uuid::Uuid;

    #[sqlx::test]
    async fn test_create_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;

        let mut rq = create_view_request(None, None);

        let _view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.into_uuid().to_string()),
        )
        .await
        .unwrap();
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.into_uuid().to_string()),
        )
        .await
        .expect_err("Recreate with same ident should fail.");
        assert_eq!(view.error.code, 409);
        let old_name = rq.name.clone();
        rq.name = "some-other-name".to_string();

        let _view = create_view(
            api_context.clone(),
            namespace,
            rq.clone(),
            Some(whi.into_uuid().to_string()),
        )
        .await
        .expect("Recreate with with another name it should work");

        rq.name = old_name;
        let new_ns = new_namespace(
            api_context.v1_state.catalog.clone(),
            &whi,
            Some(vec![Uuid::now_v7().to_string()]),
        )
        .await;
        let _view = create_view(api_context, new_ns, rq, Some(whi.into_uuid().to_string()))
            .await
            .expect("Recreate with same name but different ns should work.");
    }

    #[sqlx::test]
    async fn test_drop_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = &whi.into_uuid().to_string();
        let _view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(whi.into_uuid().to_string()),
        )
        .await
        .unwrap();
        let mut table_ident = namespace.inner();
        table_ident.push(view_name.into());
        drop_view(
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("Drop should work");

        let not_found = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should not exist anymore");
        assert_eq!(not_found.error.code, 404);
    }

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = &whi.into_uuid().to_string();
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        )
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
    }

    async fn load_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, Catalog, Server>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<Catalog, AllowAllAuthZHandler, Server> as views::Service<
            State<AllowAllAuthZHandler, Catalog, Server>,
        >>::load_view(
            params,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
    }

    async fn drop_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, Catalog, Server>>,
        params: ViewParameters,
    ) -> crate::api::Result<()> {
        <CatalogServer<Catalog, AllowAllAuthZHandler, Server> as views::Service<
            State<AllowAllAuthZHandler, Catalog, Server>,
        >>::drop_view(
            params,
            api_context,
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
    }

    async fn create_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, Catalog, Server>>,
        namespace: NamespaceIdent,
        rq: CreateViewRequest,
        prefix: Option<String>,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<Catalog, AllowAllAuthZHandler, Server> as views::Service<
            State<AllowAllAuthZHandler, Catalog, Server>,
        >>::create_view(
            NamespaceParameters {
                namespace: namespace.clone(),
                prefix: Some(Prefix(
                    prefix.unwrap_or("b8683712-3484-11ef-a305-1bc8771ed40c".to_string()),
                )),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
    }

    #[sqlx::test]
    async fn test_commit_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;
        let prefix = whi.into_uuid().to_string();
        let view_name = "myview";
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        )
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(Some(view.metadata.view_uuid));

        let res = <CatalogServer<
            Catalog,
            AllowAllAuthZHandler,
            implementations::postgres::secrets::Server,
        > as views::Service<
            State<
                AllowAllAuthZHandler,
                implementations::postgres::Catalog,
                implementations::postgres::secrets::Server,
            >,
        >>::commit_view(
            views::ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(
                    namespace.inner().into_iter().chain([view_name.into()]),
                )
                .unwrap(),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
        .unwrap();

        assert_eq!(res.metadata.current_version_id, 2);
        assert_eq!(res.metadata.schemas.len(), 2);
        assert_eq!(res.metadata.versions.len(), 2);
        let max_schema = res.metadata.schemas.keys().max();
        assert_eq!(
            res.metadata.current_version().schema_id,
            *max_schema.unwrap()
        );
    }

    async fn setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthZHandler, Catalog, Server>>,
        NamespaceIdent,
        WarehouseIdent,
    ) {
        let api_context = get_api_context(pool);
        let state = api_context.v1_state.catalog.clone();
        let warehouse_id =
            initialize_warehouse(state.clone(), Some(StorageProfile::Test(TestProfile)), None)
                .await;
        let namespace = new_namespace(state, &warehouse_id, None).await;
        (api_context, namespace, warehouse_id)
    }

    async fn new_namespace(
        state: CatalogState,
        warehouse_id: &WarehouseIdent,
        namespace: Option<Vec<String>>,
    ) -> NamespaceIdent {
        let namespace =
            NamespaceIdent::from_vec(namespace.unwrap_or(vec!["my_namespace".to_string()]))
                .unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        namespace
    }

    fn get_api_context(pool: PgPool) -> ApiContext<State<AllowAllAuthZHandler, Catalog, Server>> {
        let (tx, _) = tokio::sync::mpsc::channel(1000);

        ApiContext {
            v1_state: State {
                auth: AllowAllAuthState,
                catalog: CatalogState {
                    read_pool: pool.clone(),
                    write_pool: pool.clone(),
                },
                secrets: SecretsState {
                    read_pool: pool.clone(),
                    write_pool: pool,
                },
                publisher: CloudEventsPublisher::new(tx.clone()),
                contract_verifiers: ContractVerifiers::new(vec![]),
            },
        }
    }

    fn create_view_request(name: Option<&str>, location: Option<&str>) -> CreateViewRequest {
        serde_json::from_value(json!({
                                  "name": name.unwrap_or("myview"),
                                  "location": location,
                                  "schema": {
                                    "schema-id": 0,
                                    "type": "struct",
                                    "fields": [
                                      {
                                        "id": 0,
                                        "name": "id",
                                        "required": false,
                                        "type": "long"
                                      }
                                    ]
                                  },
                                  "view-version": {
                                    "version-id": 1,
                                    "schema-id": 0,
                                    "timestamp-ms": 1_719_395_654_343_i64,
                                    "summary": {
                                      "engine-version": "3.5.1",
                                      "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
                                      "engine-name": "spark",
                                      "app-id": "local-1719395622847"
                                    },
                                    "representations": [
                                      {
                                        "type": "sql",
                                        "sql": "select id, xyz from spark_demo.my_table",
                                        "dialect": "spark"
                                      }
                                    ],
                                    "default-namespace": []
                                  },
                                  "properties": {
                                    "create_engine_version": "Spark 3.5.1",
                                    "engine_version": "Spark 3.5.1",
                                    "spark.query-column-names": "id"
                                  }})).unwrap()
    }

    fn spark_commit_update_request(asserted_uuid: Option<Uuid>) -> CommitViewRequest {
        let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
            u.to_string()
        });
        serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "uuid": &uuid
    }
  ],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "create_engine_version": "Spark 3.5.1",
        "spark.query-column-names": "id",
        "engine_version": "Spark 3.5.1"
      }
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 1,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "id",
            "required": false,
            "type": "long",
            "doc": "id of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-view-version",
      "view-version": {
        "version-id": 2,
        "schema-id": -1,
        "timestamp-ms": 1_719_494_740_509_i64,
        "summary": {
          "engine-name": "spark",
          "engine-version": "3.5.1",
          "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
          "app-id": "local-1719494665567"
        },
        "representations": [
          {
            "type": "sql",
            "sql": "select id from spark_demo.my_table",
            "dialect": "spark"
          }
        ],
        "default-namespace": []
      }
    },
    {
      "action": "set-current-view-version",
      "view-version-id": -1
    }
  ]
})).unwrap()
    }
}
