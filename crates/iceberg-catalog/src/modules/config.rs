use super::{CatalogBackend, ProjectIdent, WarehouseIdent};
use crate::rest::{CatalogConfig, Result};

#[async_trait::async_trait]

pub trait ConfigProvider<C: CatalogBackend>
where
    Self: Clone + Send + Sync + 'static,
{
    // Should only return a warehouse if the warehouse is active.
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: ProjectIdent,
        catalog_state: C::State,
    ) -> Result<WarehouseIdent>;

    // Should only return a warehouse if the warehouse is active.
    async fn get_config_for_warehouse(
        warehouse_id: WarehouseIdent,
        catalog_state: C::State,
    ) -> Result<CatalogConfig>;
}
