use http::HeaderMap;
use http::StatusCode;
use iceberg_rest_service::v1::config::GetConfigQueryParams;
use iceberg_rest_service::v1::{
    ApiContext, CatalogConfig, ErrorModel, IcebergErrorResponse, Result,
};
use std::marker::PhantomData;
use std::str::FromStr;

use crate::service::SecretStore;
use crate::service::{
    auth::{AuthConfigHandler, AuthZHandler, UserWarehouse},
    config::ConfigProvider,
    Catalog, ProjectIdent, State,
};
use crate::CONFIG;

#[derive(Clone, Debug)]
pub struct Server<C: ConfigProvider<D>, D: Catalog, T: AuthConfigHandler<A>, A: AuthZHandler> {
    auth_handler: PhantomData<T>,
    auth_state: PhantomData<A::State>,
    config_server: PhantomData<C>,
    catalog_state: PhantomData<D::State>,
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

#[async_trait::async_trait]
impl<
        C: ConfigProvider<D>,
        A: AuthZHandler,
        D: Catalog,
        S: SecretStore,
        T: AuthConfigHandler<A>,
    > iceberg_rest_service::v1::config::Service<State<A, D, S>> for Server<C, D, T, A>
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<State<A, D, S>>,
        headers: HeaderMap,
    ) -> Result<CatalogConfig> {
        let auth_info =
            T::get_and_validate_user_warehouse(api_context.v1_state.auth.clone(), &headers).await?;

        let UserWarehouse {
            user_id,
            project_id: project_from_auth,
            warehouse_id: warehouse_from_auth,
        } = auth_info;

        if query.warehouse.is_none() && warehouse_from_auth.is_none() {
            let e: IcebergErrorResponse = ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("No warehouse specified. Please specify the 'warehouse' parameter in the GET /config request.".to_string())
                .r#type("GetConfigNoWarehouseProvided".to_string())
                .build()
                .into();
            return Err(e);
        }

        let (project_from_arg, warehouse_from_arg) = query
            .warehouse
            .map_or((None, None), |arg| parse_warehouse_arg(&arg));

        if let Some(project_from_arg) = &project_from_arg {
            // This is a user-provided project-id, so we need to check if the user is allowed to access it
            T::check_user_list_warehouse_in_project(
                api_context.v1_state.auth.clone(),
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
            C::get_warehouse_by_name(
                &warehouse_from_arg,
                &project_id,
                api_context.v1_state.catalog.clone(),
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
            api_context.v1_state.auth.clone(),
            &user_id,
            &warehouse_id,
        )
        .await?;

        // Get config from DB and new token from AuthHandler simultaneously
        let config = C::get_config_for_warehouse(&warehouse_id, api_context.v1_state.catalog);

        // Give the auth-handler a chance to exchange / enrich the token
        let new_token = T::exchange_token_for_warehouse(
            api_context.v1_state.auth.clone(),
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

        config
            .overrides
            .insert("prefix".to_string(), CONFIG.warehouse_prefix(&warehouse_id));

        config
            .overrides
            .insert("uri".to_string(), CONFIG.base_uri.to_string());

        Ok(config)
    }
}
