use crate::api::iceberg::v1::config::GetConfigQueryParams;
use crate::api::iceberg::v1::{ApiContext, CatalogConfig, ErrorModel, Result};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{ProjectAction, WarehouseAction};
use crate::service::token_verification::AuthDetails;
use std::str::FromStr;

use crate::service::SecretStore;
use crate::service::{authz::Authorizer, Catalog, ProjectIdent, State};
use crate::CONFIG;

use super::CatalogServer;

#[async_trait::async_trait]
impl<A: Authorizer, C: Catalog, S: SecretStore>
    crate::api::iceberg::v1::config::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CatalogConfig> {
        let authorizer = api_context.v1_state.authz;
        let project_id_from_auth = &request_metadata
            .auth_details
            .as_ref()
            .and_then(AuthDetails::project_id);
        let warehouse_id_from_auth = &request_metadata
            .auth_details
            .as_ref()
            .and_then(AuthDetails::warehouse_id);

        // Arg takes precedence over auth
        let warehouse_id = if let Some(query_warehouse) = query.warehouse {
            let (project_from_arg, warehouse_from_arg) = parse_warehouse_arg(&query_warehouse);
            let project_id = project_from_arg
                .or(*project_id_from_auth)
                .or(CONFIG.default_project_id)
                .ok_or_else(|| {
                    // ToDo Christian: Split Project into separate endpoint, Use name
                    ErrorModel::bad_request(
                        "No project provided. Please provide warehouse as: <project-name>/<warehouse-name>",
                        "GetConfigNoProjectProvided", None)
                })?;
            authorizer
                .require_project_action(
                    &request_metadata,
                    project_id,
                    &ProjectAction::CanListWarehouses,
                )
                .await?;
            C::get_warehouse_by_name(
                &warehouse_from_arg,
                project_id,
                api_context.v1_state.catalog.clone(),
            )
            .await?
        } else {
            warehouse_id_from_auth.ok_or_else(|| {
                ErrorModel::bad_request("No warehouse specified. Please specify the 'warehouse' parameter in the GET /config request.".to_string(), "GetConfigNoWarehouseProvided", None)
            })?
        };

        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &WarehouseAction::CanGetConfig,
            )
            .await?;

        // Get config from DB and new token from AuthHandler simultaneously
        let mut config =
            C::get_config_for_warehouse(warehouse_id, api_context.v1_state.catalog).await?;

        config
            .overrides
            .insert("prefix".to_string(), CONFIG.warehouse_prefix(warehouse_id));

        config
            .overrides
            .insert("uri".to_string(), CONFIG.base_uri_catalog().to_string());

        Ok(config)
    }
}

fn parse_warehouse_arg(arg: &str) -> (Option<ProjectIdent>, String) {
    // structure of the argument is <(optional uuid project_id)>/<warehouse_name>
    // Warehouse names cannot include /

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
