use headers::{authorization::Bearer, Authorization, Header};
use http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use iceberg_rest_server::{
    service::auth::{
        AuthConfigHandler, AuthHandler, AuthState as AuthStateTrait, UserID, UserWarehouse,
    },
    ProjectIdent, WarehouseIdent,
};
use iceberg_rest_service::{v1::NamespaceIdent, ErrorModel, Result};

#[derive(Clone, Debug, Default)]
pub struct AnonymousAuthHandler;

#[derive(Clone, Debug, Default)]
pub struct AuthState;

impl AuthStateTrait for AuthState {}

fn authenticate_user(headers: &HeaderMap) -> Result<UserID> {
    let auth_header = headers.get(AUTHORIZATION).ok_or(
        ErrorModel::builder()
            .code(StatusCode::UNAUTHORIZED.into())
            .message("No Authorization header provided".to_string())
            .r#type("NoAuthorizationHeader".to_string())
            .build(),
    )?;
    let _auth_header =
        Authorization::<Bearer>::decode(&mut [auth_header].into_iter()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::UNAUTHORIZED.into())
                .message("Error decoding Authorization header".to_string())
                .r#type("AuthorizationHeaderDecodeError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

    Ok(UserID::new("anonymous".to_string()))
}

#[async_trait::async_trait]
impl AuthConfigHandler<AuthState> for AnonymousAuthHandler {
    async fn get_and_validate_user_warehouse(
        _state: AuthState,
        headers: &HeaderMap,
    ) -> Result<UserWarehouse> {
        let user_id = authenticate_user(headers)?;

        // The AuthHandler should return the user's project or warehouse if this
        // information is available. Otherwise return "None".
        // This requires the user to specify the project as part of the "warehouse" provided to the GET /config
        // endpoint.
        Ok(UserWarehouse {
            user_id,
            project_id: Some(ProjectIdent::from(uuid::uuid!(
                "00000000-0000-0000-0000-000000000000"
            ))),
            warehouse_id: None,
        })
    }

    async fn exchange_token_for_warehouse(
        _state: AuthState,
        _previous_headers: &HeaderMap,
        _project_id: &ProjectIdent,
        _warehouse_id: &WarehouseIdent,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn check_user_list_warehouse_in_project(
        _state: AuthState,
        _user_id: &UserID,
        _project_id: &ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    /// Check if the user is allowed to get the config for a warehouse.
    async fn check_user_get_config_for_warehouse(
        _state: AuthState,
        _user_id: &UserID,
        _warehouse_id: &WarehouseIdent,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuthHandler<AuthState> for AnonymousAuthHandler {
    async fn check_list_namespace(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _parent: &Option<NamespaceIdent>,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_create_namespace(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_load_namespace_metadata(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }

    // Should check if the user is allowed to check if a namespace exists,
    // not check if the namespace exists.
    async fn check_namespace_exists(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_drop_namespace(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }

    async fn check_update_namespace_properties(
        _headers: &HeaderMap,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: AuthState,
    ) -> Result<()> {
        Ok(())
    }
}
