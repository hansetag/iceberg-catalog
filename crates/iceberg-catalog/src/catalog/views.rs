mod commit;
mod create;
mod drop;
mod list;
mod load;

use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    ViewParameters,
};
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::spec::view_properties::{METADATA_COMPRESSION, METADATA_COMPRESSION_DEFAULT};
use iceberg_ext::catalog::rest::ViewUpdate;

use super::tables::{validate_table_or_view_ident, validate_table_properties};
use super::{require_warehouse_id, CatalogServer};
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State};

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::views::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all view identifiers underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        query: PaginationQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        list::list_views(parameters, query, state, request_metadata).await
    }

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        create::create_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        load::load_view(parameters, state, data_access, request_metadata).await
    }

    /// Commit updates to a view
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        commit::commit_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Drop a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_view(parameters, state, request_metadata).await
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
        let view_id = C::view_ident_to_id(warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            .ok()
            .flatten();

        A::check_view_exists(
            &request_metadata,
            warehouse_id,
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
        let source_id = C::view_ident_to_id(warehouse_id, &source, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        // We need to be allowed to delete the old table and create the new one
        let rename_check = A::check_rename_view(
            &request_metadata,
            warehouse_id,
            source_id.as_ref(),
            state.v1_state.auth.clone(),
        );
        let create_check = A::check_create_view(
            &request_metadata,
            warehouse_id,
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

fn validate_view_updates(updates: &Vec<ViewUpdate>) -> Result<()> {
    for update in updates {
        match update {
            ViewUpdate::SetProperties { updates } => {
                let compression = updates
                    .get(METADATA_COMPRESSION)
                    .map_or(METADATA_COMPRESSION_DEFAULT, String::as_str);
                if compression != METADATA_COMPRESSION_DEFAULT {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message(format!(
                            "Only gzip compression is supported, got: '{compression:?}'"
                        ))
                        .r#type("UnsupportedCompression".to_string())
                        .build()
                        .into());
                }

                validate_view_properties(updates.keys())?;
            }
            ViewUpdate::RemoveProperties { removals } => {
                validate_view_properties(removals.iter())?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {

    use crate::api::ApiContext;

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

    use iceberg::NamespaceIdent;

    use sqlx::PgPool;

    pub(crate) async fn setup(
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
        let namespace = new_namespace(state, warehouse_id, None).await;
        (api_context, namespace, warehouse_id)
    }

    pub(crate) async fn new_namespace(
        state: CatalogState,
        warehouse_id: WarehouseIdent,
        namespace: Option<Vec<String>>,
    ) -> NamespaceIdent {
        let namespace =
            NamespaceIdent::from_vec(namespace.unwrap_or(vec!["my_namespace".to_string()]))
                .unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        namespace
    }

    pub(crate) fn get_api_context(
        pool: PgPool,
    ) -> ApiContext<State<AllowAllAuthZHandler, Catalog, Server>> {
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
}
