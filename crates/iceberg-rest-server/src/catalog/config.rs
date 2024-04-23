use http::HeaderMap;
use http::StatusCode;
use iceberg_rest_service::v1::config::GetConfigQueryParams;
use iceberg_rest_service::v1::{
    ApiContext, CatalogConfig, ErrorModel, IcebergErrorResponse, Result,
};
use std::marker::PhantomData;
use std::str::FromStr;

use crate::{
    auth::{AuthConfigHandler, AuthState},
    state::{DBState, State},
    ProjectIdent, WarehouseIdent,
};

#[derive(Clone, Debug, Default)]
pub struct Server<D: ConfigDB, A: AuthState, T: AuthConfigHandler<A>> {
    auth_handler: PhantomData<T>,
    auth_state: PhantomData<A>,
    db: PhantomData<D>,
}

fn parse_warehouse_arg(arg: &str) -> (Option<ProjectIdent>, String) {
    // structure of the argument is <(optional uuid project_id)>/<warehouse_name which might include />

    // Split arg at first /
    let parts: Vec<&str> = arg.splitn(2, '/').collect();
    match parts.len() {
        1 => {
            // No project_id provided
            let warehouse_name = parts[0].to_string();
            (None, warehouse_name)
        }
        2 => {
            // Maybe project_id and warehouse_id provided
            // If parts[0] is a valid UUID, it is a project_id, otherwise the whole thing is a warehouse_id
            match ProjectIdent::from_str(parts[0]) {
                Ok(project_id) => {
                    let warehouse_name = parts[1].to_string();
                    (Some(project_id), warehouse_name)
                }
                Err(_) => (None, arg.to_string()),
            }
        }
        // Because of the splitn(2, ..) there can't be more than 2 parts
        _ => unreachable!(),
    }
}

// This logic is abstracted in order to re-use the ConfigServer
// in a pure Config Gateway that can delegate clients to different
// rest Servers. This gateway could offer a global domain and
// federate requests to regional deployments by overriding `uri`.
#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait ConfigDB
where
    Self: Clone + Send + Sync + 'static,
{
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectIdent,
        db_state: &DBState,
    ) -> Result<WarehouseIdent>;

    async fn get_config_for_warehouse(
        warehouse_id: &WarehouseIdent,
        db_state: &DBState,
    ) -> Result<CatalogConfig>;
}

#[async_trait::async_trait]
impl<D: ConfigDB, A: AuthState, T: AuthConfigHandler<A>>
    iceberg_rest_service::v1::config::Service<State<A>> for Server<D, A, T>
{
    #[allow(clippy::too_many_lines)]
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<State<A>>,
        headers: HeaderMap,
    ) -> Result<CatalogConfig> {
        let auth_info =
            T::get_and_validate_user_warehouse(api_context.v1_state.auth_state.clone(), &headers)
                .await?;

        let user = auth_info.user_id;
        let warehouse_from_auth = auth_info.warehouse_id;
        let warehouse_from_arg = query.warehouse;
        let project_from_auth = auth_info.project_id;

        let (project_id, warehouse_id) = match (
            warehouse_from_auth.clone(),
            warehouse_from_arg,
            project_from_auth,
        ) {
            (None, None, None) => {
                // No warehouse is provided, we need to return an error
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("No warehouse provided".to_string())
                    .r#type("GetConfigNoWarehouseProvided".to_string())
                    .build()
                    .into());
            }
            // Case nothing provided and project not provided by auth.
            // Fails because we require a project-id.
            // Single project deployments should specify a static
            // project-id via the AuthHandler.
            (Some(_w_auth), None, None) => {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("No project provided".to_string())
                    .r#type("GetConfigNoProjectProvided".to_string())
                    .build()
                    .into());
            }
            // Only user specified information is available.
            // We need to parse both the project and warehouse from the
            // user provided information.
            (None, Some(w_arg), None) => {
                let (project_id, warehouse_name) = parse_warehouse_arg(&w_arg);

                let project_id = project_id.ok_or_else(|| {
                    let e: IcebergErrorResponse = ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message("No project provided".to_string())
                        .r#type("GetConfigNoProjectProvided".to_string())
                        .build()
                        .into();
                    e
                })?;

                // This is a user-provided project-id, so we need to check if the user is allowed to access it
                T::check_user_list_warehouse_in_project(
                    api_context.v1_state.auth_state.clone(),
                    &user,
                    &project_id,
                )
                .await?;

                let warehouse_id = D::get_warehouse_by_name(
                    &warehouse_name,
                    &project_id,
                    &api_context.v1_state.db_state,
                )
                .await?;

                (project_id, warehouse_id)
            }
            (None, None, Some(_project)) => {
                // No warehouse is provided, we need to return an error
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("No warehouse provided".to_string())
                    .r#type("GetConfigNoWarehouseProvided".to_string())
                    .build()
                    .into());
            }
            // project-id and warehouse-name are provided by the AuthHandler.
            (Some(w_auth), None, Some(p_auth)) => (p_auth, w_auth),
            // User specified warehouse and project is provided by the AuthHandler.
            // There might be an ambiguity if the user also specifies the project.
            // The user specified project takes precedence but is checked for access.
            (None, Some(w_arg), Some(p_auth)) => {
                let (project_id, warehouse_name) = parse_warehouse_arg(&w_arg);
                if let Some(project_id) = &project_id {
                    if project_id != &p_auth {
                        // This is a user-provided project-id, so we need to check if the user is allowed to access it
                        T::check_user_list_warehouse_in_project(
                            api_context.v1_state.auth_state.clone(),
                            &user,
                            project_id,
                        )
                        .await?;
                    }
                }
                let project_id = project_id.unwrap_or(p_auth);

                let warehouse_id = D::get_warehouse_by_name(
                    &warehouse_name,
                    &project_id,
                    &api_context.v1_state.db_state,
                )
                .await?;

                (project_id, warehouse_id)
            }
            // This shouldn't happen, if the AuthHandler provides a warehouse, it should also provide a project
            (Some(_w_auth), Some(_w_arg), None) => {
                return Err(ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("AuthHandler provided warehouse but no project".to_string())
                    .r#type("GetConfigAuthHandlerNoProject".to_string())
                    .build()
                    .into());
            }
            // User specified information takes precedence over AuthHandler provided information.
            (Some(_w_auth), Some(w_arg), Some(p_auth)) => {
                let (project_id, warehouse_name) = parse_warehouse_arg(&w_arg);
                if let Some(project_id) = &project_id {
                    if project_id != &p_auth {
                        // This is a user-provided project-id, so we need to check if the user is allowed to access it
                        T::check_user_list_warehouse_in_project(
                            api_context.v1_state.auth_state.clone(),
                            &user,
                            project_id,
                        )
                        .await?;
                    }
                }
                let project_id = project_id.unwrap_or(p_auth);

                let warehouse_id = D::get_warehouse_by_name(
                    &warehouse_name,
                    &project_id,
                    &api_context.v1_state.db_state,
                )
                .await?;

                (project_id, warehouse_id)
            }
        };

        // At this pont the project is already validated.
        // Check the warehouse if warehouse_from_auth is None or if the
        // user-provided warehouse is different from the auth-provided one.
        if warehouse_from_auth.is_none() || warehouse_from_auth != Some(warehouse_id.clone()) {
            T::check_user_get_config_for_warehouse(
                api_context.v1_state.auth_state.clone(),
                &user,
                &warehouse_id,
            )
            .await?;
        }

        // Get config from DB and new token from AuthHandler simultaneously
        let config = D::get_config_for_warehouse(&warehouse_id, &api_context.v1_state.db_state);

        // Give the auth-handler a chance to exchange / enrich the token
        let new_token = T::exchange_token_for_warehouse(
            api_context.v1_state.auth_state.clone(),
            &headers,
            &project_id,
            &warehouse_id,
        );

        let (config, new_token) = futures::join!(config, new_token);
        let new_token = new_token?;
        let mut config = config?;

        if let Some(new_token) = new_token {
            config.overrides.insert("token".to_string(), new_token);
        }

        Ok(config)
    }
}
