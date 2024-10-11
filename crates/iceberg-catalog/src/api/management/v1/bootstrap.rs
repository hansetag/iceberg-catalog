// use crate::api::iceberg::types::PageToken;
// use crate::api::iceberg::v1::PaginationQuery;
// use crate::api::management::v1::ApiServer;
// use crate::api::ApiContext;
// use crate::request_metadata::RequestMetadata;
// use crate::service::authz::{Authorizer, ProjectAction, RoleAction};
// use crate::service::{Catalog, Result, RoleId, SecretStore, State, Transaction};
// use crate::{ProjectIdent, CONFIG};
// use axum::response::IntoResponse;
// use axum::Json;
// use iceberg_ext::catalog::rest::ErrorModel;
// use serde::{Deserialize, Serialize};
//
// #[derive(Debug, Deserialize, utoipa::ToSchema)]
// #[serde(rename_all = "kebab-case")]
// pub struct CreateRoleRequest {
//     /// Name of the role to create
//     pub name: String,
//     /// Description of the role
//     #[serde(default)]
//     pub description: Option<String>,
//     /// Project ID in which the role is created.
//     /// Only required if the project ID cannot be inferred from the
//     /// users token and no default project is set.
//     #[serde(default)]
//     #[schema(value_type=uuid::Uuid)]
//     pub project_id: Option<ProjectIdent>,
// }
