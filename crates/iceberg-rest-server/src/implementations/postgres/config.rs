use super::CatalogState;
use crate::service::{
    config::ConfigProvider, storage::StorageProfile, ProjectIdent, WarehouseIdent,
};
use http::StatusCode;
use iceberg_rest_service::{CatalogConfig, ErrorModel, Result};
use sqlx::types::Json;

#[async_trait::async_trait]
impl ConfigProvider<CatalogState> for super::PostgresCatalog {
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

#[cfg(test)]
mod tests {
    use super::super::PostgresCatalog;
    use super::*;
    use crate::service::storage::S3Profile;
    use crate::service::Catalog;
    use crate::CONFIG;

    #[sqlx::test]
    async fn test_get_warehouse_by_name(pool: sqlx::PgPool) {
        let catalog_state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool,
        };

        let warehouse_name = "test-warehouse".to_string();
        let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
        let storage_profile = StorageProfile::S3(S3Profile {
            bucket: "my-bucket".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            assume_role_arn: None,
        });

        let warehouse_id = PostgresCatalog::create_warehouse_profile(
            warehouse_name.clone(),
            project_id.clone(),
            storage_profile.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap();

        let r_warehouse_id =
            PostgresCatalog::get_warehouse_by_name("test-warehouse", &project_id, catalog_state)
                .await
                .unwrap();

        assert_eq!(warehouse_id, r_warehouse_id);
    }

    #[sqlx::test]
    async fn test_get_config_for_warehouse(pool: sqlx::PgPool) {
        let catalog_state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool,
        };

        let warehouse_name = "test-warehouse".to_string();
        let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
        let storage_profile = StorageProfile::S3(S3Profile {
            bucket: "my-bucket".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            assume_role_arn: None,
        });

        let warehouse_id = PostgresCatalog::create_warehouse_profile(
            warehouse_name.clone(),
            project_id.clone(),
            storage_profile.clone(),
            catalog_state.clone(),
        )
        .await
        .unwrap();

        let config = PostgresCatalog::get_config_for_warehouse(&warehouse_id, catalog_state)
            .await
            .unwrap();

        assert_eq!(
            CONFIG.s3_signer_uri().to_string(),
            config.overrides["s3.signer.url"]
        );
    }
}
