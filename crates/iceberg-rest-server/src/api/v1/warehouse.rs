use crate::api::ApiServer;
use crate::service::storage::{StorageCredential, StorageProfile};
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State, Transaction};
use crate::{WarehouseIdent, WarehouseStatus};
use axum::response::{IntoResponse, Response};
use http::{HeaderMap, StatusCode};
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseRequest {
    /// Name of the warehouse to create. Must be unique
    /// within a project.
    pub warehouse_id: uuid::Uuid,
    pub new_name: Option<String>,
    /// Storage profile to use for the warehouse.
    pub new_storage_profile: Option<StorageProfile>,
    /// Optional storage credential to use for the warehouse.
    pub new_storage_credential: Option<StorageCredential>,
    pub new_warehouse_status: Option<WarehouseStatus>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseResponse;

impl axum::response::IntoResponse for CreateWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for UpdateWarehouseResponse {
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

    #[allow(clippy::too_many_lines)]
    async fn update_warehouse(
        request: UpdateWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<Response> {
        let warehouse_id = &WarehouseIdent::from(request.warehouse_id);
        A::check_is_warehouse_available(&headers, warehouse_id, context.v1_state.auth.clone())
            .await?;
        let mut response_code = StatusCode::NOT_MODIFIED;
        let (old_name, old_storage_profile, old_storage_credential_id, old_status) = {
            let old_warehouse_config =
                C::get_warehouse(warehouse_id, context.v1_state.catalog.clone()).await?;
            (
                old_warehouse_config.warehouse_name,
                old_warehouse_config.storage_profile,
                old_warehouse_config.storage_credential_id,
                old_warehouse_config.warehouse_status,
            )
        };

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog.clone()).await?;

        if let Some(new_status) = request.new_warehouse_status {
            if new_status != old_status {
                C::change_warehouse_status(warehouse_id, new_status, transaction.transaction())
                    .await?;
                response_code = StatusCode::OK;
            }
        }

        if let Some(new_name) = request.new_name {
            if new_name != old_name {
                C::update_warehouse_name(warehouse_id, &new_name, transaction.transaction())
                    .await?;
                response_code = StatusCode::OK;
            }
        }

        match (request.new_storage_credential, request.new_storage_profile) {
            (None, Some(new_storage_profile)) => {
                let mut new_storage_profile = new_storage_profile;

                if old_storage_profile.can_be_updated_with(&new_storage_profile) {
                    new_storage_profile.validate(None).await?;
                    C::update_warehouse_storage_profile(
                        warehouse_id,
                        new_storage_profile,
                        transaction.transaction(),
                    )
                    .await?;
                }

                C::update_warehouse_storage_secret_id(
                    warehouse_id,
                    None,
                    transaction.transaction(),
                )
                .await?;

                transaction.commit().await?;

                if let Some(old_cred) = old_storage_credential_id {
                    S::delete_secret(&old_cred, context.v1_state.secrets)
                        .await
                        .ok();
                }

                Ok((StatusCode::OK, UpdateWarehouseResponse).into_response())
            }
            (Some(new_storage_credential), Some(new_storage_profile)) => {
                let mut new_storage_profile = new_storage_profile;

                if old_storage_profile.can_be_updated_with(&new_storage_profile) {
                    new_storage_profile
                        .validate(Some(&new_storage_credential))
                        .await?;
                    C::update_warehouse_storage_profile(
                        warehouse_id,
                        new_storage_profile,
                        transaction.transaction(),
                    )
                    .await?;
                }

                let new_secret_id = Some(
                    S::create_secret(new_storage_credential, context.v1_state.secrets.clone())
                        .await?,
                );

                C::update_warehouse_storage_secret_id(
                    warehouse_id,
                    new_secret_id,
                    transaction.transaction(),
                )
                .await?;

                transaction.commit().await?;

                if let Some(old_cred) = old_storage_credential_id {
                    S::delete_secret(&old_cred, context.v1_state.secrets)
                        .await
                        .ok();
                }

                Ok((StatusCode::OK, UpdateWarehouseResponse).into_response())
            }
            (Some(new_storage_credential), None) => {
                let new_secret_id = Some(
                    S::create_secret(new_storage_credential, context.v1_state.secrets.clone())
                        .await?,
                );

                C::update_warehouse_storage_secret_id(
                    warehouse_id,
                    new_secret_id,
                    transaction.transaction(),
                )
                .await?;

                transaction.commit().await?;

                if let Some(old_cred) = old_storage_credential_id {
                    S::delete_secret(&old_cred, context.v1_state.secrets)
                        .await
                        .ok();
                }

                Ok((StatusCode::OK, UpdateWarehouseResponse).into_response())
            }
            (None, None) => {
                transaction.commit().await?;
                Ok((response_code, UpdateWarehouseResponse).into_response())
            }
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
