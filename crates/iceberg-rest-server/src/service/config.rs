use super::{CatalogState, ProjectIdent, WarehouseIdent};
use iceberg_rest_service::{CatalogConfig, Result};

#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait ConfigProvider<D: CatalogState>
where
    Self: Clone + Send + Sync + 'static,
{
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectIdent,
        catalog_state: D,
    ) -> Result<WarehouseIdent>;

    async fn get_config_for_warehouse(
        warehouse_id: &WarehouseIdent,
        catalog_state: D,
    ) -> Result<CatalogConfig>;
}
