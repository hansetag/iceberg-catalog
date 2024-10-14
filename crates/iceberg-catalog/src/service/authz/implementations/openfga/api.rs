use super::relations::{
    APINamespaceAction as NamespaceAction, APINamespaceRelation as NamespaceRelation,
    APIProjectAction as ProjectAction, APIProjectRelation as ProjectRelation,
    APIServerAction as ServerAction, APIServerRelation as ServerRelation,
    APITableAction as TableAction, APITableRelation as TableRelation, APIViewAction as ViewAction,
    APIViewRelation as ViewRelation, APIWarehouseAction as WarehouseAction,
    APIWarehouseRelation as WarehouseRelation, Assignment, NamespaceAssignment,
    NamespaceRelation as AllNamespaceRelations, ProjectAssignment,
    ProjectRelation as AllProjectRelations, ReducedRelation, ServerAssignment,
    ServerRelation as AllServerAction, TableAssignment, TableRelation as AllTableRelations,
    UserOrRole, ViewAssignment, ViewRelation as AllViewRelations, WarehouseAssignment,
    WarehouseRelation as AllWarehouseRelation,
};
use super::OPENFGA_SERVER;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::implementations::openfga::entities::OpenFgaEntity;
use crate::service::authz::implementations::openfga::{
    OpenFGAAuthorizer, OpenFGAError, OpenFGAResult,
};
use crate::service::{
    Actor, Catalog, NamespaceIdentUuid, Result, SecretStore, State, TableIdentUuid, ViewIdentUuid,
};
use crate::{ProjectIdent, WarehouseIdent, CONFIG};
use axum::body::Bytes;
use axum::extract::{Path, Query, State as AxumState};
use axum::routing::get;
use axum::{Extension, Json, Router};
use http::StatusCode;
use openfga_rs::tonic::codegen::{Body, StdError};
use openfga_rs::{tonic, CheckRequestTupleKey, ReadRequestTupleKey};
use serde::{Deserialize, Serialize};
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

/// Get a users or roles access to the server
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
    let relations = get_allowed_actions(authorizer, &metadata, &OPENFGA_SERVER).await?;

    Ok((
        StatusCode::OK,
        Json(GetServerAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get a users or roles access to the default project
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
    let relations = get_allowed_actions(authorizer, &metadata, &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get a users or roles access to the default project
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
    let relations = get_allowed_actions(authorizer, &metadata, &project_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetProjectAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get a users or roles access to a namespace
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
    let relations = get_allowed_actions(authorizer, &metadata, &namespace_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetNamespaceAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get a users or roles access to a table
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/table/by-id/{table_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetTableAccessResponse]),
    )
)]
async fn get_table_access<T, C: Catalog, S: SecretStore>(
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
    let relations = get_allowed_actions(authorizer, &metadata, &table_id.to_openfga()).await?;

    Ok((
        StatusCode::OK,
        Json(GetTableAccessResponse {
            allowed_actions: relations,
        }),
    ))
}

/// Get a users or roles access to a view
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/view/by-id/{table_id}/access",
    params(GetAccessQuery),
    responses(
            (status = 200, description = "Server Relations", body = [GetViewAccessResponse]),
    )
)]
async fn get_view_access<T, C: Catalog, S: SecretStore>(
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
    let relations = get_allowed_actions(authorizer, &metadata, &view_id.to_openfga()).await?;

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
    path = "/management/v1/permissions/server/assignments",
    params(GetServerAssignmentsQuery),
    responses(
            (status = 200, description = "Project Relations", body = [GetServerAssignmentsResponse]),
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
            (status = 200, description = "Project Relations", body = [GetProjectAssignmentsResponse]),
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
        Json(GetProjectAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments to a project
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/project/by-id/{project_id}/assignments",
    params(GetProjectAssignmentsQuery),
    responses(
            (status = 200, description = "Project Relations", body = [GetProjectAssignmentsResponse]),
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
        Json(GetProjectAssignmentsResponse { assignments }),
    ))
}

/// Get user and role assignments for a warehouse
#[utoipa::path(
    get,
    tag = "permissions",
    path = "/management/v1/permissions/warehouse/by-id/{warehouse_id}/assignments",
    params(GetWarehouseAssignmentsQuery),
    responses(
            (status = 200, description = "Warehouse Relations", body = [GetWarehouseAssignmentsResponse]),
    )
)]
async fn get_warehouse_assignments<T, C: Catalog, S: SecretStore>(
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
            (status = 200, description = "Namespace Relations", body = [GetNamespaceAssignmentsResponse]),
    )
)]
async fn get_namespace_assignments<T, C: Catalog, S: SecretStore>(
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
            (status = 200, description = "Table Relations", body = [GetTableAssignmentsResponse]),
    )
)]
async fn get_table_assignments<T, C: Catalog, S: SecretStore>(
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
            (status = 200, description = "View Relations", body = [GetViewAssignmentsResponse]),
    )
)]
async fn get_view_assignments<T, C: Catalog, S: SecretStore>(
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

#[derive(Debug, OpenApi)]
#[openapi(
    tags(
        (name = "permissions", description = "Manage Permissions"),
    ),
    paths(
        get_namespace_access_by_id,
        get_namespace_assignments,
        get_project_access,
        get_project_access_by_id,
        get_project_assignments,
        get_project_assignments_by_id,
        get_server_access,
        get_server_assignments,
        get_table_access,
        get_table_assignments,
        get_view_access,
        get_view_assignments,
        get_warehouse_assignments,
    ),
    components(schemas(
        GetNamespaceAccessResponse,
        GetProjectAccessResponse,
        GetProjectAssignmentsResponse,
        GetServerAccessResponse,
        GetServerAssignmentsResponse,
        GetTableAccessResponse,
        GetViewAccessResponse,
        GetWarehouseAccessResponse,
        GetNamespaceAssignmentsResponse,
        GetTableAssignmentsResponse,
        GetViewAssignmentsResponse,
        GetWarehouseAssignmentsResponse,
        NamespaceAction,
        NamespaceAssignment,
        NamespaceRelation,
        ProjectAction,
        ProjectAssignment,
        ProjectRelation,
        ServerAction,
        ServerAssignment,
        ServerRelation,
        TableAction,
        TableAssignment,
        TableRelation,
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
        .route("/permissions/server/access", get(get_server_access))
        .route("/permissions/project/access", get(get_project_access))
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
            get(get_table_access),
        )
        .route(
            "/permissions/view/by-id/{table_id}/access",
            get(get_view_access),
        )
        .route(
            "/permissions/server/assignments",
            get(get_server_assignments),
        )
        .route(
            "/permissions/project/assignments",
            get(get_project_assignments),
        )
        .route(
            "/permissions/project/by-id/{project_id}/assignments",
            get(get_project_assignments_by_id),
        )
        .route(
            "/permissions/warehouse/by-id/{warehouse_id}/assignments",
            get(get_warehouse_assignments),
        )
        .route(
            "/permissions/namespace/by-id/{namespace_id}/assignments",
            get(get_namespace_assignments),
        )
        .route(
            "/permissions/table/by-id/{table_id}/assignments",
            get(get_table_assignments),
        )
        .route(
            "/permissions/view/by-id/{table_id}/assignments",
            get(get_view_assignments),
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
    metadata: &RequestMetadata,
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
    let actor = metadata.auth_details.actor();
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
