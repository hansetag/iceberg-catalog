use crate::service::config::ConfigProvider;
use crate::{service::storage::StorageProfile, ProjectIdent, SecretIdent, WarehouseIdent};
use http::StatusCode;
use iceberg_rest_service::{CatalogConfig, ErrorModel, Result};

use super::dbutils::DBErrorHandler as _;

use super::{Catalog, CatalogState};
use sqlx::types::Json;

#[async_trait::async_trait]
impl ConfigProvider<Catalog> for super::Catalog {
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectIdent,
        catalog_state: CatalogState,
    ) -> Result<WarehouseIdent> {
        let warehouse_id = sqlx::query_scalar!(
            r#"
            SELECT 
                warehouse_id
            FROM warehouse
            WHERE warehouse_name = $1 AND project_id = $2
            "#,
            warehouse_name.to_string(),
            project_id.as_uuid()
        )
        .fetch_one(&catalog_state.read_pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Warehouse not found".to_string())
                .r#type("WarehouseNotFound".to_string())
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching warehouse".to_string())
                .r#type("WarehouseFetchError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
        })?;

        Ok(warehouse_id.into())
    }

    async fn get_config_for_warehouse(
        warehouse_id: &WarehouseIdent,
        catalog_state: CatalogState,
    ) -> Result<CatalogConfig> {
        let storage_profile = sqlx::query_scalar!(
            r#"
            SELECT 
                storage_profile as "storage_profile: Json<StorageProfile>"
            FROM warehouse
            WHERE warehouse_id = $1
            "#,
            warehouse_id.as_uuid()
        )
        .fetch_one(&catalog_state.read_pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Warehouse not found".to_string())
                .r#type("WarehouseNotFound".to_string())
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching warehouse".to_string())
                .r#type("WarehouseFetchError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
        })?;

        Ok(storage_profile.generate_catalog_config())
    }
}

pub(crate) async fn create_warehouse_profile<'a>(
    warehouse_name: String,
    project_id: ProjectIdent,
    storage_profile: StorageProfile,
    storage_secret_id: Option<SecretIdent>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<WarehouseIdent> {
    validate_warehouse_name(&warehouse_name)?;
    let storage_profile_ser = serde_json::to_value(storage_profile).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing storage profile".to_string())
            .r#type("StorageProfileSerializationError".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let warehouse_id = sqlx::query_scalar!(
        r#"
        INSERT INTO warehouse (warehouse_name, project_id, storage_profile, storage_secret_id)
        VALUES ($1, $2, $3, $4)
        RETURNING warehouse_id
        "#,
        warehouse_name,
        project_id.as_uuid(),
        storage_profile_ser,
        storage_secret_id.map(|id| id.into_uuid())
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err) => match db_err.constraint() {
            // ToDo: Get constarint name from const
            Some("unique_warehouse_name_in_project") => ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Warehouse with this name already exists in the project.".to_string())
                .r#type("WarehouseNameAlreadyExists".to_string())
                .build(),
            _ => e.into_error_model("Error creating Warehouse".into()),
        },
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Error creating Warehouse.".to_string())
            .r#type("WarehouseNotReturnedAfterCreation".to_string())
            .build(),
        _ => e.into_error_model("Error creating Warehouse".into()),
    })?;

    Ok(warehouse_id.into())
}

fn validate_warehouse_name(warehouse_name: &str) -> Result<()> {
    if warehouse_name.is_empty() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Warehouse name cannot be empty".to_string())
            .r#type("EmptyWarehouseName".to_string())
            .build()
            .into());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{
        implementations::postgres::PostgresTransaction,
        service::{storage::S3Profile, Catalog as _, Transaction as _},
    };

    pub(crate) async fn initialize_warehouse(
        state: CatalogState,
        storage_profile: Option<StorageProfile>,
    ) -> crate::WarehouseIdent {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let storage_profile = storage_profile.unwrap_or(StorageProfile::S3(S3Profile {
            bucket: "test_bucket".to_string(),
            endpoint: None,
            region: None,
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
        }));

        let warehouse_id = Catalog::create_warehouse_profile(
            "test_warehouse".to_string(),
            ProjectIdent::from(uuid::Uuid::nil()),
            storage_profile,
            None,
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();
        warehouse_id
    }

    #[sqlx::test]
    async fn test_get_warehouse_by_name(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };
        let warehouse_id = initialize_warehouse(state.clone(), None).await;

        let fetched_warehouse_id = Catalog::get_warehouse_by_name(
            "test_warehouse",
            &ProjectIdent::from(uuid::Uuid::nil()),
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(warehouse_id, fetched_warehouse_id);
    }
}
