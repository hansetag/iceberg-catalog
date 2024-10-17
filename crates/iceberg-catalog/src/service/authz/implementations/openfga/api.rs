use super::relations::{
    APINamespaceAction as NamespaceAction, APINamespaceRelation as NamespaceRelation,
    APIProjectAction as ProjectAction, APIProjectRelation as ProjectRelation,
    APIRoleAction as RoleAction, APIRoleRelation as RoleRelation, APIServerAction as ServerAction,
    APIServerRelation as ServerRelation, APITableAction as TableAction,
    APITableRelation as TableRelation, APIViewAction as ViewAction,
    APIViewRelation as ViewRelation, APIWarehouseAction as WarehouseAction,
    APIWarehouseRelation as WarehouseRelation, Assignment, GrantableRelation, NamespaceAssignment,
    NamespaceRelation as AllNamespaceRelations, ProjectAssignment,
    ProjectRelation as AllProjectRelations, ReducedRelation, RoleAssignment,
    RoleRelation as AllRoleRelations, ServerAssignment, ServerRelation as AllServerAction,
    TableAssignment, TableRelation as AllTableRelations, UserOrRole, ViewAssignment,
    ViewRelation as AllViewRelations, WarehouseAssignment,
    WarehouseRelation as AllWarehouseRelation,
};
use super::OPENFGA_SERVER;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::implementations::openfga::entities::OpenFgaEntity;
use crate::service::authz::implementations::openfga::service_ext::MAX_TUPLES_PER_WRITE;
use crate::service::authz::implementations::openfga::{
    OpenFGAAuthorizer, OpenFGAError, OpenFGAResult,
};
use crate::service::{
    Actor, Catalog, NamespaceIdentUuid, Result, RoleId, SecretStore, State, TableIdentUuid,
    ViewIdentUuid,
};
use crate::{ProjectIdent, WarehouseIdent, CONFIG};
use axum::body::Bytes;
use axum::extract::{Path, Query, State as AxumState};
use axum::routing::get;
use axum::{Extension, Json, Router};
use http::StatusCode;
use openfga_rs::tonic::codegen::{Body, StdError};
use openfga_rs::{
    tonic, CheckRequestTupleKey, ReadRequestTupleKey, TupleKey, TupleKeyWithoutCondition,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use strum::IntoEnumIterator;
use utoipa::OpenApi;

const _MAX_ASSIGNMENTS_PER_RELATION: i32 = 200;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
struct GetAccessQuery {
    // /// The user or role to show access for.
    // /// If not specified, shows access for the current user.
    // #[serde(default)]
    // principal: Option<UserOrRole>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetRoleAccessResponse {
    allowed_actions: Vec<RoleAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetServerAccessResponse {
    allowed_actions: Vec<ServerAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetProjectAccessResponse {
    allowed_actions: Vec<ProjectAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetWarehouseAccessResponse {
    allowed_actions: Vec<WarehouseAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetNamespaceAccessResponse {
    allowed_actions: Vec<NamespaceAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetTableAccessResponse {
    allowed_actions: Vec<TableAction>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetViewAccessResponse {
    allowed_actions: Vec<ViewAction>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
struct GetRoleAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<RoleRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetRoleAssignmentsResponse {
    assignments: Vec<RoleAssignment>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
struct GetServerAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<ServerRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetServerAssignmentsResponse {
    assignments: Vec<ServerAssignment>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetProjectAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<ProjectRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetProjectAssignmentsResponse {
    assignments: Vec<ProjectAssignment>,
    #[schema(value_type = uuid::Uuid)]
    project_id: ProjectIdent,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetWarehouseAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<WarehouseRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetWarehouseAssignmentsResponse {
    assignments: Vec<WarehouseAssignment>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetNamespaceAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<NamespaceRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetNamespaceAssignmentsResponse {
    assignments: Vec<NamespaceAssignment>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetTableAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<TableRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetTableAssignmentsResponse {
    assignments: Vec<TableAssignment>,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetViewAssignmentsQuery {
    /// Relations to be loaded. If not specified, all relations are returned.
    #[serde(default)]
    relations: Option<Vec<ViewRelation>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct GetViewAssignmentsResponse {
    assignments: Vec<ViewAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateServerAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ServerAssignment>,
    #[serde(default)]
    deletes: Vec<ServerAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateProjectAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ProjectAssignment>,
    #[serde(default)]
    deletes: Vec<ProjectAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateWarehouseAssignmentsRequest {
    #[serde(default)]
    writes: Vec<WarehouseAssignment>,
    #[serde(default)]
    deletes: Vec<WarehouseAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateNamespaceAssignmentsRequest {
    #[serde(default)]
    writes: Vec<NamespaceAssignment>,
    #[serde(default)]
    deletes: Vec<NamespaceAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateTableAssignmentsRequest {
    #[serde(default)]
    writes: Vec<TableAssignment>,
    #[serde(default)]
    deletes: Vec<TableAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateViewAssignmentsRequest {
    #[serde(default)]
    writes: Vec<ViewAssignment>,
    #[serde(default)]
    deletes: Vec<ViewAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
struct UpdateRoleAssignmentsRequest {
    #[serde(default)]
    writes: Vec<RoleAssignment>,
    #[serde(default)]
    deletes: Vec<RoleAssignment>,
}

/// Get my access to the default project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/role/by-id/{role_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, body = [GetRoleAccessResponse]),
    )
)]
async fn get_role_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetRoleAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &role_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetRoleAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to the server
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/server/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Access", body = [GetServerAccessResponse]),
    )
)]
async fn get_server_access<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetServerAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations = get_allowed_actions(authorizer, metadata.actor(), &OPENFGA_SERVER).await?;

    Ok((
        StatusCode::OK,
        Json(GetServerAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to the default project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/project/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetProjectAccessResponse]),
    )
)]
async fn get_project_access<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetProjectAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let project_id = metadata
        .auth_details
        .project_id()
        .or(CONFIG.default_project_id)
        .ok_or(OpenFGAError::NoProjectId)?;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to the default project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/project/by-id/{project_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetProjectAccessResponse]),
    )
)]
async fn get_project_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(project_id): Path<ProjectIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetProjectAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a warehouse
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/warehouse/by-id/{warehouse_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, body = [GetNamespaceAccessResponse]),
    )
)]
async fn get_warehouse_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetWarehouseAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &warehouse_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetWarehouseAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a namespace
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/namespace/by-id/{namespace_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetNamespaceAccessResponse]),
    )
)]
async fn get_namespace_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(namespace_id): Path<NamespaceIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetNamespaceAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &namespace_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a table
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/table/by-id/{table_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetTableAccessResponse]),
    )
)]
async fn get_table_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(table_id): Path<TableIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetTableAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &table_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetTableAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get my access to a view
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/view/by-id/{view_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, body = [GetViewAccessResponse]),
    )
)]
async fn get_view_access_by_id<T, C: Catalog, S: SecretStore>(
    Path(view_id): Path<ViewIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(_query): Query<GetAccessQuery>,
) -> Result<(StatusCode, Json<GetViewAccessResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let relations =
        get_allowed_actions(authorizer, metadata.actor(), &view_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetViewAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get user and role assignments to the current project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/role/by-id/{role_id}/assignments",
    params(GetProjectAssignmentsQuery),
    responses(
            (status = 200, body = [GetRoleAssignmentsResponse]),
    )
)]
async fn get_role_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetRoleAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetRoleAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    authorizer
        .require_action(
            &metadata,
            AllRoleRelations::CanReadAssignments,
            &role_id.to_openfga(),
        )
        .await?;
    let assignments = get_relations(authorizer, query.relations, &role_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetRoleAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments to the current project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/server/assignments",
    params(GetServerAssignmentsQuery),
    responses(
            (status = 200, body = [GetServerAssignmentsResponse]),
    )
)]
async fn get_server_assignments<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetServerAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetServerAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    authorizer
        .require_action(
            &metadata,
            AllServerAction::CanReadAssignments,
            &OPENFGA_SERVER,
        )
        .await?;
    let assignments = get_relations(authorizer, query.relations, &OPENFGA_SERVER).await?;

    Ok((
        StatusCode::OK,
        Json(GetServerAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments to the current project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/project/assignments",
    params(GetProjectAssignmentsQuery),
    responses(
            (status = 200, body = [GetProjectAssignmentsResponse]),
    )
)]
async fn get_project_assignments<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetProjectAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetProjectAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let project_id = metadata
        .auth_details
        .project_id()
        .or(CONFIG.default_project_id)
        .ok_or(OpenFGAError::NoProjectId)?;
    authorizer
        .require_action(
            &metadata,
            AllProjectRelations::CanReadAssignments,
            &project_id.to_openfga(),
        )
        .await?;
    let assignments = get_relations(authorizer, query.relations, &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAssignmentsResponse {
            assignments,
            project_id,
        }),
    ))
}

/// Get user and role assignments to a project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/project/by-id/{project_id}/assignments",
    params(GetProjectAssignmentsQuery),
    responses(
            (status = 200, body = [GetProjectAssignmentsResponse]),
    )
)]
async fn get_project_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(project_id): Path<ProjectIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetProjectAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetProjectAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    authorizer
        .require_action(
            &metadata,
            AllProjectRelations::CanReadAssignments,
            &project_id.to_openfga(),
        )
        .await?;
    let assignments = get_relations(authorizer, query.relations, &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAssignmentsResponse {
            assignments,
            project_id,
        }),
    ))
}

/// Get user and role assignments for a warehouse
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/warehouse/by-id/{warehouse_id}/assignments",
    params(GetWarehouseAssignmentsQuery),
    responses(
            (status = 200, body = [GetWarehouseAssignmentsResponse]),
    )
)]
async fn get_warehouse_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetWarehouseAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetWarehouseAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let object = warehouse_id.to_openfga();
    authorizer
        .require_action(&metadata, AllWarehouseRelation::CanReadAssignments, &object)
        .await?;
    let assignments = get_relations(authorizer, query.relations, &object).await?;

    Ok((
        StatusCode::OK,
        Json(GetWarehouseAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a namespace
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/namespace/by-id/{namespace_id}/assignments",
    params(GetNamespaceAssignmentsQuery),
    responses(
            (status = 200, body = [GetNamespaceAssignmentsResponse]),
    )
)]
async fn get_namespace_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(namespace_id): Path<NamespaceIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetNamespaceAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetNamespaceAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let object = namespace_id.to_openfga();
    authorizer
        .require_action(
            &metadata,
            AllNamespaceRelations::CanReadAssignments,
            &object,
        )
        .await?;
    let assignments = get_relations(authorizer, query.relations, &object).await?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a table
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/table/by-id/{namespace_id}/assignments",
    params(GetTableAssignmentsQuery),
    responses(
            (status = 200, body = [GetTableAssignmentsResponse]),
    )
)]
async fn get_table_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(table_id): Path<TableIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetTableAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetTableAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let object = table_id.to_openfga();
    authorizer
        .require_action(&metadata, AllTableRelations::CanReadAssignments, &object)
        .await?;
    let assignments = get_relations(authorizer, query.relations, &object).await?;

    Ok((
        StatusCode::OK,
        Json(GetTableAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a view
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/table/by-id/{namespace_id}/assignments",
    params(GetViewAssignmentsQuery),
    responses(
            (status = 200, body = [GetViewAssignmentsResponse]),
    )
)]
async fn get_view_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(view_id): Path<ViewIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Query(query): Query<GetViewAssignmentsQuery>,
) -> Result<(StatusCode, Json<GetViewAssignmentsResponse>)>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let object = view_id.to_openfga();
    authorizer
        .require_action(&metadata, AllViewRelations::CanReadAssignments, &object)
        .await?;
    let assignments = get_relations(authorizer, query.relations, &object).await?;

    Ok((
        StatusCode::OK,
        Json(GetViewAssignmentsResponse { assignments }),
    ))
}

/// Update permissions for this server
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/server/assignments",
    request_body = UpdateServerAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_server_assignments<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateServerAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &OPENFGA_SERVER,
    )
    .await?;

    Ok(())
}

/// Update permissions for the default project
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/project/assignments",
    request_body = UpdateProjectAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_project_assignments<T, C: Catalog, S: SecretStore>(
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateProjectAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    let project_id = metadata
        .auth_details
        .project_id()
        .or(CONFIG.default_project_id)
        .ok_or(OpenFGAError::NoProjectId)?;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &project_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a project
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/project/by-id/{project_id}/assignments",
    request_body = UpdateProjectAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_project_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(project_id): Path<ProjectIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateProjectAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &project_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a project
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/warehouse/by-id/{warehouse_id}/assignments",
    request_body = UpdateWarehouseAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_warehouse_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(warehouse_id): Path<WarehouseIdent>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateWarehouseAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &warehouse_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a namespace
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/namespace/by-id/{namespace_id}/assignments",
    request_body = UpdateNamespaceAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_namespace_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(namespace_id): Path<NamespaceIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateNamespaceAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &namespace_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a table
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/table/by-id/{table_id}/assignments",
    request_body = UpdateTableAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_table_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(table_id): Path<TableIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateTableAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &table_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a view
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/view/by-id/{view_id}/assignments",
    request_body = UpdateViewAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_view_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(view_id): Path<ViewIdentUuid>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateViewAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &view_id.to_openfga(),
    )
    .await?;

    Ok(())
}

/// Update permissions for a view
#[utoipa::path(
    post,
    tag = "permissions",
    path = "/management/v1/permissions/role/by-id/{role_id}/assignments",
    request_body = UpdateRoleAssignmentsRequest,
    responses(
            (status = 200, description = "Permissions updated successfully"),
    )
)]
async fn update_role_assignments_by_id<T, C: Catalog, S: SecretStore>(
    Path(role_id): Path<RoleId>,
    AxumState(api_context): AxumState<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdateRoleAssignmentsRequest>,
) -> Result<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let authorizer = api_context.v1_state.authz;
    checked_write(
        authorizer,
        metadata.actor(),
        request.writes,
        request.deletes,
        &role_id.to_openfga(),
    )
    .await?;

    Ok(())
}

#[derive(Debug, OpenApi)]
#[openapi(
    tags(
        (name = "permissions", description = "Manage Permissions"),
    ),
    paths(
        get_namespace_access_by_id,
        get_namespace_assignments_by_id,
        get_project_access,
        get_project_access_by_id,
        get_project_assignments,
        get_project_assignments_by_id,
        get_role_access_by_id,
        get_role_assignments_by_id,
        get_server_access,
        get_server_assignments,
        get_table_access_by_id,
        get_table_assignments_by_id,
        get_view_access_by_id,
        get_view_assignments_by_id,
        get_warehouse_access_by_id,
        get_warehouse_assignments_by_id,
        update_namespace_assignments_by_id,
        update_project_assignments,
        update_project_assignments_by_id,
        update_role_assignments_by_id,
        update_server_assignments,
        update_table_assignments_by_id,
        update_view_assignments_by_id,
        update_warehouse_assignments_by_id,
    ),
    components(schemas(
        GetNamespaceAccessResponse,
        GetNamespaceAssignmentsResponse,
        GetProjectAccessResponse,
        GetProjectAssignmentsResponse,
        GetRoleAccessResponse,
        GetServerAccessResponse,
        GetServerAssignmentsResponse,
        GetTableAccessResponse,
        GetTableAssignmentsResponse,
        GetViewAccessResponse,
        GetViewAssignmentsResponse,
        GetWarehouseAccessResponse,
        GetWarehouseAssignmentsResponse,
        NamespaceAction,
        NamespaceAssignment,
        NamespaceRelation,
        ProjectAction,
        ProjectAssignment,
        ProjectRelation,
        RoleAction,
        ServerAction,
        ServerAssignment,
        ServerRelation,
        TableAction,
        TableAssignment,
        TableRelation,
        UpdateNamespaceAssignmentsRequest,
        UpdateProjectAssignmentsRequest,
        UpdateRoleAssignmentsRequest,
        UpdateServerAssignmentsRequest,
        UpdateTableAssignmentsRequest,
        UpdateViewAssignmentsRequest,
        UpdateWarehouseAssignmentsRequest,
        UserOrRole,
        ViewAction,
        ViewAssignment,
        ViewRelation,
        WarehouseAction,
        WarehouseAssignment,
        WarehouseRelation,
    ))
)]
pub(crate) struct ApiDoc;

pub(super) fn new_v1_router<T, C: Catalog, S: SecretStore>(
) -> Router<ApiContext<State<OpenFGAAuthorizer<T>, C, S>>>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    Router::new()
        .route(
            "/permissions/role/by-id/{role_id}/access",
            get(get_role_access_by_id),
        )
        .route("/permissions/server/access", get(get_server_access))
        .route("/permissions/project/access", get(get_project_access))
        .route(
            "/permissions/warehouse/by-id/{warehouse_id}/access",
            get(get_warehouse_access_by_id),
        )
        .route(
            "/permissions/project/by-id/{project_id}/access",
            get(get_project_access_by_id),
        )
        .route(
            "/permissions/namespace/by-id/{namespace_id}/access",
            get(get_namespace_access_by_id),
        )
        .route(
            "/permissions/table/by-id/{table_id}/access",
            get(get_table_access_by_id),
        )
        .route(
            "/permissions/view/by-id/{table_id}/access",
            get(get_view_access_by_id),
        )
        .route(
            "/permissions/role/by-id/{role_id}/assignments",
            get(get_role_assignments_by_id).post(update_role_assignments_by_id),
        )
        .route(
            "/permissions/server/assignments",
            get(get_server_assignments).post(update_server_assignments),
        )
        .route(
            "/permissions/project/assignments",
            get(get_project_assignments).post(update_project_assignments),
        )
        .route(
            "/permissions/project/by-id/{project_id}/assignments",
            get(get_project_assignments_by_id).post(update_project_assignments_by_id),
        )
        .route(
            "/permissions/warehouse/by-id/{warehouse_id}/assignments",
            get(get_warehouse_assignments_by_id).post(update_warehouse_assignments_by_id),
        )
        .route(
            "/permissions/namespace/by-id/{namespace_id}/assignments",
            get(get_namespace_assignments_by_id).post(update_namespace_assignments_by_id),
        )
        .route(
            "/permissions/table/by-id/{table_id}/assignments",
            get(get_table_assignments_by_id).post(update_table_assignments_by_id),
        )
        .route(
            "/permissions/view/by-id/{table_id}/assignments",
            get(get_view_assignments_by_id).post(update_view_assignments_by_id),
        )
}

async fn get_relations<T, RA: Assignment>(
    authorizer: OpenFGAAuthorizer<T>,
    query_relations: Option<Vec<RA::Relation>>,
    object: &str,
) -> Result<Vec<RA>>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let relations = query_relations.unwrap_or_else(|| RA::Relation::iter().collect());

    let relations = relations.iter().map(|relation| async {
        authorizer
            .clone()
            .read_all(ReadRequestTupleKey {
                user: String::new(),
                relation: relation.to_openfga().to_string(),
                object: object.to_string(),
            })
            .await?
            .into_iter()
            .filter_map(|t| t.key)
            .map(|t| RA::try_from_user(&t.user, relation))
            .collect::<OpenFGAResult<Vec<RA>>>()
    });

    let relations = futures::future::try_join_all(relations)
        .await?
        .into_iter()
        .flatten()
        .collect();

    Ok(relations)
}

async fn get_allowed_actions<T, A: ReducedRelation + IntoEnumIterator>(
    authorizer: OpenFGAAuthorizer<T>,
    actor: &Actor,
    object: &str,
) -> OpenFGAResult<Vec<A>>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    // Fail fast
    if actor == &Actor::Anonymous {
        return Err(OpenFGAError::AuthenticationRequired);
    }

    let openfga_actor = actor.to_openfga();
    let openfga_object = object.to_string();
    let actions = A::iter().collect::<Vec<_>>();

    let actions = actions.iter().map(|action| async {
        let key = CheckRequestTupleKey {
            user: openfga_actor.clone(),
            relation: action.to_openfga().to_string(),
            object: openfga_object.clone(),
        };

        let allowed = authorizer.clone().check(key).await?;

        OpenFGAResult::Ok(Some(*action).filter(|_| allowed))
    });
    let actions = futures::future::try_join_all(actions)
        .await?
        .into_iter()
        .flatten()
        .collect();

    Ok(actions)
}

async fn checked_write<T, RA: Assignment>(
    authorizer: OpenFGAAuthorizer<T>,
    actor: &Actor,
    writes: Vec<RA>,
    deletes: Vec<RA>,
    object: &str,
) -> OpenFGAResult<()>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    // Fail fast
    if actor == &Actor::Anonymous {
        return Err(OpenFGAError::AuthenticationRequired);
    }
    let all_modifications = writes.iter().chain(deletes.iter()).collect::<Vec<_>>();
    // Fail fast for too many writes
    let num_modifications = i32::try_from(all_modifications.len()).unwrap_or(i32::MAX);
    if num_modifications > MAX_TUPLES_PER_WRITE {
        return Err(OpenFGAError::TooManyWrites {
            actual: num_modifications,
            max: MAX_TUPLES_PER_WRITE,
        });
    }

    // ---------------------------- AUTHZ CHECKS ----------------------------
    let openfga_actor = actor.to_openfga();

    let grant_relations = all_modifications
        .iter()
        .map(|action| action.relation().grant_relation())
        .collect::<HashSet<_>>();

    futures::future::try_join_all(grant_relations.iter().map(|relation| async {
        let key = CheckRequestTupleKey {
            user: openfga_actor.clone(),
            relation: relation.to_string(),
            object: object.to_string(),
        };

        let allowed = authorizer.clone().check(key).await?;
        if allowed {
            Ok(())
        } else {
            Err(OpenFGAError::Unauthorized {
                user: openfga_actor.clone(),
                relation: relation.to_string(),
                object: object.to_string(),
            })
        }
    }))
    .await?;

    // ---------------------- APPLY WRITE OPERATIONS -----------------------
    let writes = writes
        .into_iter()
        .map(|ra| TupleKey {
            user: ra.openfga_user(),
            relation: ra.relation().to_openfga().to_string(),
            object: object.to_string(),
            condition: None,
        })
        .collect();
    let deletes = deletes
        .into_iter()
        .map(|ra| TupleKeyWithoutCondition {
            user: ra.openfga_user(),
            relation: ra.relation().to_openfga().to_string(),
            object: object.to_string(),
        })
        .collect();
    authorizer.write(Some(writes), Some(deletes)).await
}

#[cfg(test)]
mod tests {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authz::implementations::openfga::migration::tests::authorizer_for_empty_store;
        use crate::service::UserId;
        use openfga_rs::TupleKey;

        #[tokio::test]
        async fn test_get_relations() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &OPENFGA_SERVER)
                    .await
                    .unwrap();
            assert!(relations.is_empty());

            let user_id = UserId::new(&uuid::Uuid::now_v7().to_string()).unwrap();
            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ServerRelation::GlobalAdmin.to_openfga().to_string(),
                        object: OPENFGA_SERVER.to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &OPENFGA_SERVER)
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 1);
            assert_eq!(
                relations,
                vec![ServerAssignment::GlobalAdmin(user_id.into())]
            );
        }

        #[tokio::test]
        async fn test_get_allowed_actions() {
            let (_, authorizer) = authorizer_for_empty_store().await;
            let user_id = UserId::new(&uuid::Uuid::now_v7().to_string()).unwrap();
            let actor = Actor::Principal(user_id.clone());
            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &OPENFGA_SERVER)
                    .await
                    .unwrap();
            assert!(access.is_empty());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user_id.to_openfga(),
                        relation: ServerRelation::GlobalAdmin.to_openfga().to_string(),
                        object: OPENFGA_SERVER.to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let access: Vec<ServerAction> =
                get_allowed_actions(authorizer.clone(), &actor, &OPENFGA_SERVER)
                    .await
                    .unwrap();
            for action in ServerAction::iter() {
                assert!(access.contains(&action));
            }
        }

        #[tokio::test]
        async fn test_checked_write() {
            let (_, authorizer) = authorizer_for_empty_store().await;

            let user1_id = UserId::new(&uuid::Uuid::now_v7().to_string()).unwrap();
            let user2_id = UserId::new(&uuid::Uuid::now_v7().to_string()).unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: user1_id.to_openfga(),
                        relation: ServerRelation::GlobalAdmin.to_openfga().to_string(),
                        object: OPENFGA_SERVER.to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            checked_write(
                authorizer.clone(),
                &Actor::Principal(user1_id.clone()),
                vec![ServerAssignment::GlobalAdmin(user2_id.into())],
                vec![],
                &OPENFGA_SERVER,
            )
            .await
            .unwrap();

            let relations: Vec<ServerAssignment> =
                get_relations(authorizer.clone(), None, &OPENFGA_SERVER)
                    .await
                    .unwrap();
            assert_eq!(relations.len(), 2);
        }
    }
}
