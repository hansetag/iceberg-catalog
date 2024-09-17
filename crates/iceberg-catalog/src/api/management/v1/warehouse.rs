use crate::api::management::v1::{
    ApiServer, DeletedTabularResponse, ListDeletedTabularsResponse, User, UserOrigin,
};
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
pub use crate::service::storage::{
    AzCredential, AzdlsProfile, S3Credential, S3Profile, StorageCredential, StorageProfile,
};

use crate::api::iceberg::v1::{PaginatedTabulars, PaginationQuery};

pub use crate::service::WarehouseStatus;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, ListFlags, State, Transaction,
};
use crate::{ProjectIdent, WarehouseIdent, CONFIG};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseRequest {
    /// Name of the warehouse to create. Must be unique
    /// within a project.
    pub warehouse_name: String,
    /// Project ID in which to create the warehouse.
    pub project_id: uuid::Uuid,
    /// Storage profile to use for the warehouse.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    pub storage_credential: Option<StorageCredential>,
    /// Profile to determine behavior upon dropping of tabulars, defaults to soft-deletion with
    /// 7 days expiration.
    #[serde(default)]
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum TabularDeleteProfile {
    Hard {},

    Soft {
        #[serde(
            deserialize_with = "crate::config::seconds_to_duration",
            serialize_with = "crate::config::duration_to_seconds"
        )]
        expiration_seconds: chrono::Duration,
    },
}

impl TabularDeleteProfile {
    pub(crate) fn expiration_seconds(&self) -> Option<chrono::Duration> {
        match self {
            Self::Soft { expiration_seconds } => Some(*expiration_seconds),
            Self::Hard {} => None,
        }
    }
}

impl Default for TabularDeleteProfile {
    fn default() -> Self {
        Self::Soft {
            expiration_seconds: CONFIG.default_tabular_expiration_delay_seconds,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseResponse {
    /// ID of the created warehouse.
    pub warehouse_id: uuid::Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseStorageRequest {
    /// Storage profile to use for the warehouse.
    /// The new profile must point to the same location as the existing profile
    /// to avoid data loss. For S3 this means that you may not change the
    /// bucket, key prefix, or region.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    /// The existing credential is not re-used. If no credential is
    /// provided, we assume that this storage does not require credentials.
    pub storage_credential: Option<StorageCredential>,
}

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "kebab-case")]
pub struct ListWarehousesRequest {
    /// Optional filter to return only warehouses
    /// with the specified status.
    /// If not provided, only active warehouses are returned.
    #[serde(default)]
    pub warehouse_status: Option<Vec<WarehouseStatus>>,
    /// The project ID to list warehouses for.
    /// Setting a warehouse is required.
    #[serde(default)]
    pub project_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameWarehouseRequest {
    /// New name for the warehouse.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ProjectResponse {
    /// ID of the project.
    pub project_id: uuid::Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListProjectsResponse {
    /// List of projects
    pub projects: Vec<ProjectResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    pub id: uuid::Uuid,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: uuid::Uuid,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListWarehousesResponse {
    /// List of warehouses in the project.
    pub warehouses: Vec<GetWarehouseResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseCredentialRequest {
    /// New storage credential to use for the warehouse.
    /// If not specified, the existing credential is removed.
    pub new_storage_credential: Option<StorageCredential>,
}

impl axum::response::IntoResponse for CreateWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::CREATED, axum::Json(self)).into_response()
    }
}

impl<C: Catalog, A: AuthZHandler, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub trait Service<C: Catalog, A: AuthZHandler, S: SecretStore> {
    async fn create_user(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<User> {
        let auth = request_metadata
            .auth_details
            .ok_or(ErrorModel::bad_request(
                "Auth details found in request metadata",
                "MissingUserId",
                None,
            ))?;
        let user_id = auth.user_id().ok_or(ErrorModel::bad_request(
            "User ID not found in auth details",
            "MissingUserId",
            None,
        ))?;

        let user_id = Uuid::new_v5(&user_id, auth.issuer().as_bytes());
        C::create_user(
            user_id,
            auth.display_name(),
            auth.name().as_deref(),
            auth.email(),
            UserOrigin::ExplicitRegistration,
            context.v1_state.catalog,
        )
        .await
    }

    async fn create_warehouse(
        request: CreateWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateWarehouseResponse> {
        let CreateWarehouseRequest {
            warehouse_name,
            project_id,
            mut storage_profile,
            storage_credential,
            delete_profile,
        } = request;
        let project_ident = ProjectIdent::from(project_id);

        // ------------------- AuthZ -------------------
        A::check_create_warehouse(&request_metadata, &project_ident, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        storage_profile.normalize()?;
        storage_profile
            .validate_access(storage_credential.as_ref(), None)
            .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        let warehouse_id = C::create_warehouse(
            warehouse_name,
            project_id.into(),
            storage_profile,
            delete_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(CreateWarehouseResponse {
            warehouse_id: *warehouse_id,
        })
    }

    async fn list_projects(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListProjectsResponse> {
        // ------------------- AuthZ -------------------
        let projects = A::check_list_projects(&request_metadata, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        if let Some(projects) = projects {
            return Ok(ListProjectsResponse {
                projects: projects
                    .into_iter()
                    .map(|project_id| ProjectResponse {
                        project_id: *project_id,
                    })
                    .collect(),
            });
        }

        let projects = C::list_projects(context.v1_state.catalog).await?;
        Ok(ListProjectsResponse {
            projects: projects
                .into_iter()
                .map(|project_id| ProjectResponse {
                    project_id: *project_id,
                })
                .collect(),
        })
    }

    async fn list_warehouses(
        request: ListWarehousesRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListWarehousesResponse> {
        // ------------------- AuthZ -------------------
        let project_id = ProjectIdent::from(
            request.project_id.ok_or(
                ErrorModel::builder()
                    .code(http::StatusCode::BAD_REQUEST.into())
                    .message("project-id is required".to_string())
                    .r#type("MissingProjectId".to_string())
                    .build(),
            )?,
        );
        let warehouses = A::check_list_warehouse_in_project(
            &request_metadata,
            project_id,
            context.v1_state.auth,
        )
        .await?;

        // ------------------- Business Logic -------------------
        let warehouses = C::list_warehouses(
            project_id,
            request.warehouse_status,
            warehouses.as_ref(),
            context.v1_state.catalog,
        )
        .await?;

        Ok(ListWarehousesResponse {
            warehouses: warehouses
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        })
    }

    async fn get_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        A::check_get_warehouse(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let warehouses = C::get_warehouse(warehouse_id, transaction.transaction()).await?;

        Ok(warehouses.into())
    }

    async fn delete_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_delete_warehouse(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::delete_warehouse(warehouse_id, transaction.transaction()).await?;

        transaction.commit().await?;

        Ok(())
    }
    async fn rename_warehouse(
        warehouse_id: WarehouseIdent,
        request: RenameWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_rename_warehouse(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::rename_warehouse(warehouse_id, &request.new_name, transaction.transaction()).await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn deactivate_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_deactivate_warehouse(&request_metadata, warehouse_id, context.v1_state.auth)
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::set_warehouse_status(
            warehouse_id,
            WarehouseStatus::Inactive,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn activate_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_activate_warehouse(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::set_warehouse_status(
            warehouse_id,
            WarehouseStatus::Active,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn update_storage(
        warehouse_id: WarehouseIdent,
        request: UpdateWarehouseStorageRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_update_storage(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let UpdateWarehouseStorageRequest {
            mut storage_profile,
            storage_credential,
        } = request;

        storage_profile.normalize()?;
        storage_profile
            .validate_access(storage_credential.as_ref(), None)
            .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let warehouse = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
        warehouse
            .storage_profile
            .can_be_updated_with(&storage_profile)?;
        let old_secret_id = warehouse.storage_secret_id;

        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        C::update_storage_profile(
            warehouse_id,
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!("Failed to delete old secret: {:?}", e.error);
                })
                .ok();
        }

        Ok(())
    }

    async fn update_credential(
        warehouse_id: WarehouseIdent,
        request: UpdateWarehouseCredentialRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        A::check_update_storage(&request_metadata, warehouse_id, context.v1_state.auth).await?;

        // ------------------- Business Logic -------------------
        let UpdateWarehouseCredentialRequest {
            new_storage_credential,
        } = request;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let warehouse = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
        let old_secret_id = warehouse.storage_secret_id;
        let storage_profile = warehouse.storage_profile;

        storage_profile
            .validate_access(new_storage_credential.as_ref(), None)
            .await?;

        let secret_id = if let Some(new_storage_credential) = new_storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(new_storage_credential)
                    .await?,
            )
        } else {
            None
        };

        C::update_storage_profile(
            warehouse_id,
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!("Failed to delete old secret: {:?}", e.error);
                })
                .ok();
        }

        Ok(())
    }

    async fn list_soft_deleted_tabulars(
        request_metadata: RequestMetadata,
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        pagination_query: PaginationQuery,
    ) -> Result<ListDeletedTabularsResponse> {
        // ------------------- AuthZ -------------------
        A::check_list_soft_deletions(&request_metadata, warehouse_id, context.v1_state.auth)
            .await?;

        // ------------------- Business Logic -------------------
        let PaginatedTabulars {
            tabulars,
            next_page_token,
        } = C::list_tabulars(
            warehouse_id,
            ListFlags::only_deleted(),
            context.v1_state.catalog,
            pagination_query,
        )
        .await?;

        Ok(ListDeletedTabularsResponse {
            tabulars: tabulars
                .into_iter()
                .map(|(k, (ident, delete_opts))| {
                    let i = ident.into_inner();
                    let deleted = delete_opts.ok_or(ErrorModel::internal(
                        "Expected delete options to be Some, but found None",
                        "InternalDatabaseError",
                        None,
                    ))?;

                    Ok(DeletedTabularResponse {
                        id: *k,
                        name: i.name,
                        namespace: i.namespace.inner(),
                        typ: k.into(),
                        warehouse_id: *warehouse_id,
                        created_at: deleted.created_at,
                        deleted_at: deleted.deleted_at,
                        expiration_date: deleted.expiration_date,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            next_page_token,
        })
    }
}

impl axum::response::IntoResponse for ListProjectsResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListWarehousesResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for GetWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl From<crate::service::GetWarehouseResponse> for GetWarehouseResponse {
    fn from(warehouse: crate::service::GetWarehouseResponse) -> Self {
        Self {
            id: warehouse.id.to_uuid(),
            name: warehouse.name,
            project_id: *warehouse.project_id,
            storage_profile: warehouse.storage_profile,
            status: warehouse.status,
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_de_create_warehouse_request() {
        let request = serde_json::json!({
            "warehouse-name": "test_warehouse",
            "project-id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "storage-profile": {
                "type": "s3",
                "bucket": "test",
                "region": "dummy",
                "path-style-access": true,
                "endpoint": "http://localhost:9000",
                "sts-enabled": true,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "test-access-key-id",
                "aws-secret-access-key": "test-secret-access-key",
            },
        });

        let request: super::CreateWarehouseRequest = serde_json::from_value(request).unwrap();
        assert_eq!(request.warehouse_name, "test_warehouse");
        assert_eq!(
            request.project_id,
            uuid::Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap()
        );
        let s3_profile = request.storage_profile.try_into_s3().unwrap();
        assert_eq!(s3_profile.bucket, "test");
        assert_eq!(s3_profile.region, "dummy");
        assert_eq!(s3_profile.path_style_access, Some(true));
    }
}
