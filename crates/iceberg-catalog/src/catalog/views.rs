mod commit;
mod create;
mod drop;
mod exists;
mod list;
mod load;
mod rename;

use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    ViewParameters,
};
use crate::request_metadata::RequestMetadata;
use iceberg_ext::catalog::rest::ViewUpdate;

use super::tables::validate_table_properties;
use super::CatalogServer;
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
        exists::view_exists(parameters, state, request_metadata).await
    }

    /// Rename a view
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_view(prefix, request, state, request_metadata).await
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
        namespace_name: Option<Vec<String>>,
    ) -> (
        ApiContext<State<AllowAllAuthZHandler, Catalog, SecretsState>>,
        NamespaceIdent,
        WarehouseIdent,
    ) {
        let api_context = get_api_context(pool);
        let state = api_context.v1_state.catalog.clone();
        let warehouse_id =
            initialize_warehouse(state.clone(), Some(StorageProfile::Test(TestProfile)), None)
                .await;
        let namespace = new_namespace(state, warehouse_id, namespace_name).await;
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
    ) -> ApiContext<State<AllowAllAuthZHandler, Catalog, SecretsState>> {
        let (tx, _) = tokio::sync::mpsc::channel(1000);

        ApiContext {
            v1_state: State {
                auth: AllowAllAuthState,
                catalog: CatalogState::from_pools(pool.clone(), pool.clone()),
                secrets: SecretsState::from_pools(pool.clone(), pool),
                publisher: CloudEventsPublisher::new(tx.clone()),
                contract_verifiers: ContractVerifiers::new(vec![]),
            },
        }
    }
}
