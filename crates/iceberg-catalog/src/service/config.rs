use super::{Catalog, ProjectIdent, WarehouseIdent};
use crate::api::{CatalogConfig, Result};

#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait ConfigProvider<C: Catalog>
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
