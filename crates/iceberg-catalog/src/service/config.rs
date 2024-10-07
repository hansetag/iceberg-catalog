use super::{Catalog, ProjectIdent, WarehouseIdent};
use crate::api::{CatalogConfig, Result};
use iceberg_ext::catalog::rest::ErrorModel;

#[async_trait::async_trait]

pub trait ConfigProvider<C: Catalog>
where
    Self: Clone + Send + Sync + 'static,
{
    /// Return Ok(Some(x)) only for active warehouses
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: ProjectIdent,
        catalog_state: C::State,
    ) -> Result<Option<WarehouseIdent>>;

    /// Wrapper around get_warehouse_by_name that returns
    /// not found error if the warehouse does not exist.
    async fn require_warehouse_by_name(
        warehouse_name: &str,
        project_id: ProjectIdent,
        catalog_state: C::State,
    ) -> Result<WarehouseIdent> {
        Self::get_warehouse_by_name(warehouse_name, project_id, catalog_state)
            .await?
            .ok_or(
                ErrorModel::not_found(
                    format!("Warehouse {warehouse_name} not found"),
                    "WarehouseNotFound",
                    None,
                )
                .into(),
            )
    }

    // Should only return a warehouse if the warehouse is active.
    async fn get_config_for_warehouse(
        warehouse_id: WarehouseIdent,
        catalog_state: C::State,
    ) -> Result<Option<CatalogConfig>>;

    /// Wrapper around get_config_for_warehouse that returns
    /// not found error if the warehouse does not exist.
    async fn require_config_for_warehouse(
        warehouse_id: WarehouseIdent,
        catalog_state: C::State,
    ) -> Result<CatalogConfig> {
        Self::get_config_for_warehouse(warehouse_id, catalog_state)
            .await?
            .ok_or(
                ErrorModel::not_found(
                    format!("Warehouse {warehouse_id} not found"),
                    "WarehouseNotFound",
                    None,
                )
                .into(),
            )
    }
}
