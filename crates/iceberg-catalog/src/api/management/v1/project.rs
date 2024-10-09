use crate::api::management::v1::ApiServer;
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
pub use crate::service::storage::{
    AzCredential, AzdlsProfile, GcsCredential, GcsProfile, GcsServiceKey, S3Credential, S3Profile,
    StorageCredential, StorageProfile,
};

pub use crate::service::WarehouseStatus;
use crate::service::{
    authz::{Authorizer, ListProjectsResponse as AuthZListProjectsResponse},
    secrets::SecretStore,
    Catalog, State, Transaction,
};
use crate::ProjectIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct GetProjectResponse {
    /// ID of the project.
    pub project_id: uuid::Uuid,
    /// Name of the project
    pub project_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameProjectRequest {
    /// New name for the project.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListProjectsResponse {
    /// List of projects
    pub projects: Vec<GetProjectResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateProjectRequest {
    /// Name of the project to create.
    pub project_name: String,
    /// Request a specific project ID - optional.
    /// If not provided, a new project ID will be generated (recommended).
    pub project_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateProjectResponse {
    /// ID of the created project.
    pub project_id: uuid::Uuid,
}

impl axum::response::IntoResponse for CreateProjectResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::CREATED, axum::Json(self)).into_response()
    }
}

impl axum::response::IntoResponse for GetProjectResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl<C: Catalog, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn create_project(
        request: CreateProjectRequest,
        context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<CreateProjectResponse> {
        // ------------------- AuthZ -------------------
        // Todo: AuthZ

        // ------------------- Business Logic -------------------
        let CreateProjectRequest {
            project_name,
            project_id,
        } = request;
        validate_project_name(&project_name)?;
        let mut t = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let project_id: ProjectIdent = project_id.unwrap_or(uuid::Uuid::now_v7()).into();
        C::create_project(project_id, project_name, t.transaction()).await?;
        t.commit().await?;

        Ok(CreateProjectResponse {
            project_id: *project_id,
        })
    }

    async fn rename_project(
        warehouse_id: ProjectIdent,
        request: RenameProjectRequest,
        context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        // ToDo AuthZ

        // ------------------- Business Logic -------------------
        validate_project_name(&request.new_name)?;
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::rename_project(warehouse_id, &request.new_name, transaction.transaction()).await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn get_project(
        project_id: uuid::Uuid,
        context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<GetProjectResponse> {
        // ------------------- AuthZ -------------------
        // Todo: AuthZ

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let project = C::get_project(project_id.into(), t.transaction())
            .await?
            .ok_or(ErrorModel::not_found(
                format!("Project with id {project_id} not found."),
                "ProjectNotFound",
                None,
            ))?;

        Ok(GetProjectResponse {
            project_id,
            project_name: project.name,
        })
    }

    async fn delete_project(
        project_id: uuid::Uuid,
        context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        // Todo: AuthZ

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::delete_project(project_id.into(), transaction.transaction()).await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn list_projects(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListProjectsResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        let projects = authorizer.list_projects(&request_metadata).await?;

        // ------------------- Business Logic -------------------
        let project_id_filter = match projects {
            AuthZListProjectsResponse::All => None,
            AuthZListProjectsResponse::Projects(projects) => Some(projects),
        };
        let projects = C::list_projects(project_id_filter, context.v1_state.catalog).await?;
        Ok(ListProjectsResponse {
            projects: projects
                .into_iter()
                .map(|project| GetProjectResponse {
                    project_id: *project.project_id,
                    project_name: project.name,
                })
                .collect(),
        })
    }
}

impl axum::response::IntoResponse for ListProjectsResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

fn validate_project_name(project_name: &str) -> Result<()> {
    if project_name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Project name cannot be empty",
            "EmptyProjectName",
            None,
        )
        .into());
    }

    if project_name.len() > 128 {
        return Err(ErrorModel::bad_request(
            "Project name must be shorter than 128 chars",
            "ProjectNameTooLong",
            None,
        )
        .into());
    }
    Ok(())
}
