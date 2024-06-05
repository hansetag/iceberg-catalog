use crate::{
    implementations::DEFAULT_PROJECT_ID,
    service::{
        auth::{AuthConfigHandler, AuthZHandler, UserID, UserWarehouse},
        TableIdentUuid,
    },
    ProjectIdent, WarehouseIdent,
};
use http::HeaderMap;
use iceberg_rest_service::{v1::NamespaceIdent, Result};

#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthState;

#[derive(Clone, Debug, Default)]
/// Allow absolutely, gloriously, everything.
pub struct AllowAllAuthZHandler;

#[async_trait::async_trait]
impl AuthConfigHandler<AllowAllAuthZHandler> for AllowAllAuthZHandler {
    async fn get_and_validate_user_warehouse(
        _: AllowAllAuthState,
        _: &HeaderMap,
    ) -> Result<UserWarehouse> {
        // The AuthHandler should return the user's project or warehouse if this
        // information is available. Otherwise return "None".
        // This requires the user to specify the project as part of the "warehouse" provided to the GET /config
        // endpoint.
        Ok(UserWarehouse {
            user_id: UserID::new_anonymous(),
            project_id: Some(ProjectIdent::from(DEFAULT_PROJECT_ID)),
            warehouse_id: None,
        })
    }

    async fn exchange_token_for_warehouse(
        _: AllowAllAuthState,
        _: &HeaderMap,
        _: &ProjectIdent,
        _: &WarehouseIdent,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn check_user_list_warehouse_in_project(
        _: AllowAllAuthState,
        _: &UserID,
        _: &ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_user_get_config_for_warehouse(
        _: AllowAllAuthState,
        _: &UserID,
        _: &WarehouseIdent,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuthZHandler for AllowAllAuthZHandler {
    type State = AllowAllAuthState;

    async fn check_list_namespace(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_namespace(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_namespace_metadata(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    // Should check if the user is allowed to check if a namespace exists,
    // not check if the namespace exists.
    async fn check_namespace_exists(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_namespace(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_update_namespace_properties(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_table(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_list_tables(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: &NamespaceIdent,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_rename_table(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_table(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_table_exists(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&NamespaceIdent>,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_table(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: AllowAllAuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_commit_table(
        _: &HeaderMap,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Option<&NamespaceIdent>,
        _: Self::State,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_is_warehouse_available(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _state: Self::State,
    ) -> Result<()> {
        Ok(())
    }
}
