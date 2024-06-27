use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, ViewParameters,
};
use crate::catalog::io::write_metadata_file;
use crate::implementations::postgres::tabular::TabularIdentUuid;
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::spec::ViewMetadataBuilder;
use iceberg::{NamespaceIdent, TableUpdate};
use iceberg_ext::catalog::rest::{
    AddSchemaUpdate, IcebergErrorResponse, UpgradeFormatVersionUpdate, ViewUpdate,
};
use iceberg_ext::catalog::{AssertViewUuid, ViewRequirement};
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use super::tables::{
    maybe_body_to_json, require_active_warehouse, require_no_location_specified,
    validate_lowercase_property, validate_table_or_view_ident, validate_table_properties,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
use crate::service::storage::StorageCredential;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, GetWarehouseResponse, State, TableIdentUuid,
    Transaction,
};
use iceberg_ext::catalog::ViewRequirementExt;

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
        A::check_list_tables(
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
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;

        // TODO: this correct?
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
        crate::catalog::tables::require_active_warehouse(status)?;

        let table_id: TabularIdentUuid = TabularIdentUuid::View(uuid::Uuid::now_v7());

        let view_location = storage_profile.tabular_location(&namespace_id, &table_id);
        let mut request = request;
        let metadata_location = storage_profile.metadata_location(&view_location, &table_id);
        request.location = Some(view_location);
        let request = request;

        let metadata = C::create_view(
            &warehouse_id,
            &namespace_id,
            &table_id,
            &table,
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
                &TableIdentUuid::from(*table_id),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;

        transaction.commit().await?;
        let load_table_result = LoadViewResult {
            metadata_location,
            metadata,
            config: Some(config),
        };
        eprintln!("{:?}", load_table_result);

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
        crate::catalog::tables::require_active_warehouse(status)?;

        let view_metadata = C::load_view(&warehouse_id, &view, transaction.transaction()).await?;

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

        for assertion in request.requirements.unwrap_or_default() {
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
                    todo!()
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
                    C::set_current_view_version(
                        &view_id,
                        scvv.view_version_id as i64,
                        transaction.transaction(),
                    )
                    .await?;
                }
            }
        }
        let tab_location = storage_profile
            .tabular_location(&namespace_id, &TabularIdentUuid::View(view_id.into_uuid()));
        let metadata_location = storage_profile.metadata_location(&tab_location, &Uuid::now_v7());

        C::update_metadata_location(&view_id, &metadata_location, transaction.transaction())
            .await?;
        let updated_meta =
            C::load_view(&warehouse_id, &parameters.view, transaction.transaction()).await?;

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
        let table_id = view_id.ok_or_else(|| {
            tracing::debug!("View does not exist.");
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("View does not exist in warehouse {warehouse_id}"))
                .r#type("ViewNotFound".to_string())
                .build()
        })?;
        tracing::debug!("Proceeding to delete view");
        C::drop_view(&warehouse_id, &table_id, transaction.transaction()).await?;

        // TODO: Delete metadata files
        transaction.commit().await?;

        return Ok(());
    }

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let whi = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;
        // TODO: authz
        let v = C::view_ident_to_id(&whi, &view, state.v1_state.catalog.clone()).await?;

        if let Some(_) = v {
            return Ok(());
        }

        Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("ViewExistsNotSupported".to_string())
            .build()
            .into())
    }

    /// Rename a view
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let _warehouse_id = require_warehouse_id(prefix.clone())?;
        let _body = maybe_body_to_json(&request);
        let RenameTableRequest {
            source,
            destination,
        } = request;
        validate_table_or_view_ident(&source)?;
        validate_table_or_view_ident(&destination)?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("RenameViewNotSupported".to_string())
            .build()
            .into());
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
    use crate::api::iceberg::v1::{views, DataAccess, Prefix};
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;
    use crate::implementations;
    use crate::implementations::postgres::{Catalog, CatalogState, SecretsState};
    use crate::implementations::{AllowAllAuthState, AllowAllAuthZHandler};
    use crate::service::contract_verification::ContractVerifiers;
    use crate::service::event_publisher::{
        CloudEventsPublisher, CloudEventsPublisherBackgroundTask,
    };
    use crate::service::State;
    use iceberg::spec::ViewRepresentation::SqlViewRepresentation;
    use iceberg::spec::{NestedField, Schema, StructType, ViewVersion};
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::ViewUpdate::{
        AddSchema, AddViewVersion, SetCurrentViewVersion, SetProperties,
    };
    use iceberg_ext::catalog::rest::{
        AddSchemaUpdate, AddViewVersionUpdate, CommitViewRequest, SetCurrentViewVersionUpdate,
        SetPropertiesUpdate,
    };
    use iceberg_ext::catalog::{AssertViewUuid, ViewRequirement};
    use maplit::hashmap;
    use serde_json::json;
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::util::Either::A;

    #[sqlx::test]
    async fn test_commit_view(pool: PgPool) {
        let rq: CommitViewRequest  = serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "uuid": "019059cb-9277-7ff0-b71a-537df05b33f8"
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
        "version-id": 1,
        "schema-id": -1,
        "timestamp-ms": 1719494740509i64,
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
})).unwrap();
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        let x = CloudEventsPublisherBackgroundTask {
            source: rx,
            sinks: vec![],
        };

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
                prefix: Some(Prefix("b8683712-3484-11ef-a305-1bc8771ed40c".to_string())),
                view: TableIdent::from_strs(["spark_demo", "myview4"]).unwrap(),
            },
            rq,
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
            },
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
        .unwrap();
    }
}
