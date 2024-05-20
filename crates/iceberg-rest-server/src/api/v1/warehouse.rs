use crate::api::ApiServer;
use crate::service::storage::{StorageCredential, StorageProfile};
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State, Transaction};
use http::HeaderMap;
use iceberg_rest_service::{ApiContext, Result};
use utoipa::ToSchema;

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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
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
        let s3_profile = request.storage_profile.try_into_s3(409).unwrap();
        assert_eq!(s3_profile.bucket, "test");
        assert_eq!(s3_profile.region, "dummy");
        assert_eq!(s3_profile.path_style_access, Some(true));
    }
}
