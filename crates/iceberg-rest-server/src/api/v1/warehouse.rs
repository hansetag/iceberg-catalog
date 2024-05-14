use crate::api::ApiServer;
use crate::service::storage::{StorageCredential, StorageProfile};
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State, Transaction};
use http::HeaderMap;
use iceberg_rest_service::{ApiContext, Result};
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateWarehouseResponse {
    /// ID of the created warehouse.
    pub warehouse_id: uuid::Uuid,
}

impl axum::response::IntoResponse for CreateWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl<C: Catalog, A: AuthZHandler, S: SecretStore> WarehouseService<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait WarehouseService<C: Catalog, A: AuthZHandler, S: SecretStore> {
    async fn create_warehouse(
        request: CreateWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        _headers: HeaderMap,
    ) -> Result<CreateWarehouseResponse> {
        let CreateWarehouseRequest {
            warehouse_name,
            project_id,
            mut storage_profile,
            storage_credential,
        } = request;

        storage_profile
            .validate(storage_credential.as_ref())
            .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(S::create_secret(storage_credential, context.v1_state.secrets).await?)
        } else {
            None
        };

        let warehouse_id = C::create_warehouse_profile(
            warehouse_name,
            project_id.into(),
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(CreateWarehouseResponse {
            warehouse_id: warehouse_id.into_uuid(),
        })
    }
}
