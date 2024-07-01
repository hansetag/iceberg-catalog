use std::collections::HashSet;

use crate::api::{iceberg::v1::NamespaceIdent, Result};
use crate::request_metadata::RequestMetadata;
use crate::{
    implementations::DEFAULT_PROJECT_ID,
    service::{
        auth::{AuthConfigHandler, AuthZHandler, UserWarehouse},
        TableIdentUuid,
    },
    ProjectIdent, WarehouseIdent,
};

#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthState;

#[derive(Clone, Debug, Default)]
/// Allow absolutely, gloriously, everything.
pub struct AllowAllAuthZHandler;

#[async_trait::async_trait]
impl AuthConfigHandler<AllowAllAuthZHandler> for AllowAllAuthZHandler {
    async fn get_and_validate_user_warehouse(
        _: AllowAllAuthState,
        _: &RequestMetadata,
    ) -> Result<UserWarehouse> {
        // The AuthHandler should return the user's project or warehouse if this
        // information is available. Otherwise return "None".
        // This requires the user to specify the project as part of the "warehouse" provided to the GET /config
        // endpoint.
        Ok(UserWarehouse {
            project_id: Some(ProjectIdent::from(DEFAULT_PROJECT_ID)),
            warehouse_id: None,
        })
    }

    async fn exchange_token_for_warehouse(
        _: AllowAllAuthState,
        _: &RequestMetadata,
        _: &ProjectIdent,
        _: &WarehouseIdent,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn check_list_warehouse_in_project(
        _: AllowAllAuthState,
        _: &ProjectIdent,
        _: &RequestMetadata,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_user_get_config_for_warehouse(
        _: AllowAllAuthState,
        _: &WarehouseIdent,
        _: &RequestMetadata,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuthZHandler for AllowAllAuthZHandler {
    type State = AllowAllAuthState;

    async fn check_list_namespace(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_namespace(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_namespace_metadata(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    // Should check if the user is allowed to check if a namespace exists,
    // not check if the namespace exists.
    async fn check_namespace_exists(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_namespace(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_update_namespace_properties(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_table(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_list_tables(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_rename_table(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_table(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_table_exists(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_table(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_commit_table(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Option<&NamespaceIdent>,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    // ---------------- Management API ----------------
    async fn check_create_warehouse(
        _: &RequestMetadata,
        _: &ProjectIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    // Return an error if the user is not authorized.
    // Return Ok(None) if the user is authorized to list all existing projects.
    // Return Ok(Some(projects)) if the user is authorized to list only the
    // specified projects.
    async fn check_list_projects(
        _: &RequestMetadata,
        _: Self::State,
    ) -> Result<Option<HashSet<ProjectIdent>>> {
        Ok(None)
    }

    async fn check_list_warehouse_in_project(
        _: &RequestMetadata,
        _: &ProjectIdent,
        _: Self::State,
    ) -> Result<Option<HashSet<WarehouseIdent>>> {
        Ok(None)
    }

    async fn check_delete_warehouse(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_get_warehouse(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_rename_warehouse(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_deactivate_warehouse(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_activate_warehouse(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_update_storage(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_view(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_view_exists(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: Option<&NamespaceIdent>,
        _view: Option<&TableIdentUuid>,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_list_views(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_view(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _table: Option<&TableIdentUuid>,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_view(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: Option<&NamespaceIdent>,
        _view: Option<&TableIdentUuid>,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_commit_view(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Option<&NamespaceIdent>,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_rename_view(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }
}
