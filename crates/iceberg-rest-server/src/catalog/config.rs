use http::HeaderMap;
use http::StatusCode;
use iceberg_rest_service::v1::config::GetConfigQueryParams;
use iceberg_rest_service::v1::{
    ApiContext, CatalogConfig, ErrorModel, IcebergErrorResponse, Result,
};
use std::marker::PhantomData;
use std::str::FromStr;

use crate::{
    auth::{AuthConfigHandler, AuthState, UserWarehouse},
    state::{DBState, State},
    ProjectIdent, WarehouseIdent,
};

#[derive(Clone, Debug, Default)]
pub struct Server<D: ConfigDB, A: AuthState, T: AuthConfigHandler<A>> {
    auth_handler: PhantomData<T>,
    auth_state: PhantomData<A>,
    db: PhantomData<D>,
}

fn parse_warehouse_arg(arg: &str) -> (Option<ProjectIdent>, Option<String>) {
    // structure of the argument is <(optional uuid project_id)>/<warehouse_name which might include />
    fn filter_empty_strings(s: String) -> Option<String> {
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    // Split arg at first /
    let parts: Vec<&str> = arg.splitn(2, '/').collect();
    match parts.len() {
        1 => {
            // No project_id provided
            let warehouse_name = filter_empty_strings(parts[0].to_string());
            (None, warehouse_name)
        }
        2 => {
            // Maybe project_id and warehouse_id provided
            // If parts[0] is a valid UUID, it is a project_id, otherwise the whole thing is a warehouse_id
            match ProjectIdent::from_str(parts[0]) {
                Ok(project_id) => {
                    let warehouse_name = filter_empty_strings(parts[1].to_string());
                    (Some(project_id), warehouse_name)
                }
                Err(_) => (None, filter_empty_strings(arg.to_string())),
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
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<State<A>>,
        headers: HeaderMap,
    ) -> Result<CatalogConfig> {
        let auth_info =
            T::get_and_validate_user_warehouse(api_context.v1_state.auth_state.clone(), &headers)
                .await?;

        let UserWarehouse {
            user_id,
            project_id: project_from_auth,
            warehouse_id: warehouse_from_auth,
        } = auth_info;

        let (project_from_arg, warehouse_from_arg) = query
            .warehouse
            .map_or((None, None), |arg| parse_warehouse_arg(&arg));

        if let Some(project_from_arg) = &project_from_arg {
            // This is a user-provided project-id, so we need to check if the user is allowed to access it
            T::check_user_list_warehouse_in_project(
                api_context.v1_state.auth_state.clone(),
                &user_id,
                project_from_arg,
            )
            .await?;
        }

        let project_id = project_from_arg.or(project_from_auth).ok_or_else(|| {
            let e: IcebergErrorResponse = ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("No project provided".to_string())
                .r#type("GetConfigNoProjectProvided".to_string())
                .build()
                .into();
            e
        })?;

        let warehouse_id = if let Some(warehouse_from_arg) = warehouse_from_arg {
            D::get_warehouse_by_name(
                &warehouse_from_arg,
                &project_id,
                &api_context.v1_state.db_state,
            )
            .await?
        } else {
            warehouse_from_auth.ok_or_else(|| {
                let e: IcebergErrorResponse = ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("No warehouse provided".to_string())
                    .r#type("GetConfigNoWarehouseProvided".to_string())
                    .build()
                    .into();
                e
            })?
        };

        T::check_user_get_config_for_warehouse(
            api_context.v1_state.auth_state.clone(),
            &user_id,
            &warehouse_id,
        )
        .await?;

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
