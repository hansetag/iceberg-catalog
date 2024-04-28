use super::{storage::StorageProfile, CatalogState, ProjectIdent, WarehouseIdent};
use iceberg_rest_service::v1::{
    CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesQuery,
    ListNamespacesResponse, NamespaceIdent, Result, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
};

#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait Catalog<D: CatalogState>
where
    Self: Clone + Send + Sync + 'static,
{
    async fn list_namespaces(
        warehouse_id: &WarehouseIdent,
        query: &ListNamespacesQuery,
        catalog_state: D,
    ) -> Result<ListNamespacesResponse>;

    async fn create_warehouse_profile(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        catalog_state: D,
    ) -> Result<WarehouseIdent>;

    async fn create_namespace(
        warehouse_id: &WarehouseIdent,
        request: CreateNamespaceRequest,
        catalog_state: D,
    ) -> Result<CreateNamespaceResponse>;

    async fn get_namespace_metadata(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: D,
    ) -> Result<GetNamespaceResponse>;

    async fn namespace_exists(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: D,
    ) -> Result<bool>;

    async fn drop_namespace(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: D,
    ) -> Result<()>;

    async fn update_namespace_properties(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        request: UpdateNamespacePropertiesRequest,
        catalog_state: D,
    ) -> Result<UpdateNamespacePropertiesResponse>;
}
