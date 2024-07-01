use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, ViewParameters,
};
use crate::catalog::io::write_metadata_file;
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::ViewUpdate;
use uuid::Uuid;

use super::tables::{
    maybe_body_to_json, require_active_warehouse, require_no_location_specified,
    validate_table_or_view_ident, validate_table_properties,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
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
    async fn commit_view(
        parameters: ViewParameters,
        mut request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
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
            requirements: _,
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

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("CommitViewNotSupported".to_string())
            .build()
            .into());
    }

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

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("DropViewNotSupported".to_string())
            .build()
            .into());
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

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("ViewExistsNotSupported".to_string())
            .build()
            .into());
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
    use crate::WarehouseIdent;

    use iceberg::{NamespaceIdent, TableIdent};

    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};

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
}
