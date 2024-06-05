use crate::service::config::ConfigProvider;
use crate::{
    service::storage::StorageProfile, ProjectIdent, SecretIdent, WarehouseIdent, WarehouseStatus,
};
use http::StatusCode;
use iceberg_rest_service::{CatalogConfig, ErrorModel, Result};
use serde_json::Value;
use uuid::Uuid;

use super::dbutils::DBErrorHandler as _;

use super::{Catalog, CatalogState};
use crate::service::GetWarehouseResponse;
use sqlx::types::Json;

#[async_trait::async_trait]
impl ConfigProvider<Catalog> for super::Catalog {
    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ProjectIdent,
        catalog_state: CatalogState,
    ) -> Result<WarehouseIdent> {
        let warehouse_data = sqlx::query!(
            r#"
            SELECT 
                warehouse_id AS "warehouse_id: WarehouseIdent",
                status AS "status: WarehouseStatus"
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

        let is_warehouse_available = match warehouse_data.status {
            WarehouseStatus::Active => true,
            WarehouseStatus::Inactive => false,
        };

        if !is_warehouse_available {
            let msg = format!("Warehouse '{warehouse_name}' is not available.");
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(msg)
                .r#type("WarehouseIsNotAvailable")
                .build()
                .into());
        }

        Ok(warehouse_data.warehouse_id)
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

        Ok(storage_profile.generate_catalog_config(warehouse_id))
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
    let storage_profile_ser = serialize_storage_profile(storage_profile)?;

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

pub(crate) async fn get_warehouse_status(
    warehouse_id: &WarehouseIdent,
    catalog_state: CatalogState,
) -> Result<WarehouseStatus> {
    let status = sqlx::query!(
        r#"
                SELECT 
                    status AS "status: WarehouseStatus"
                FROM 
                   warehouse
                WHERE  
                    warehouse_id = $1
            "#,
        warehouse_id.as_uuid(),
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?
    .status;

    Ok(status)
}

// Cannot update `storage_secret_id`, `storage_profile`.
// Can update `name`.
// In S3Profile cannot update `bucket`, `key_prefix`, `region`.

pub(crate) async fn update_warehouse_storage_secret_id<'a>(
    warehouse_id: &WarehouseIdent,
    storage_secret_id: Option<SecretIdent>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query!(
        r#"
            UPDATE
                warehouse
            SET
                storage_secret_id = $1
            WHERE
                warehouse_id = $2
        "#,
        storage_secret_id.map(|id| id.into_uuid()),
        warehouse_id.as_uuid(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?;

    Ok(())
}

pub(crate) async fn update_warehouse_storage_profile<'a>(
    warehouse_id: &WarehouseIdent,
    storage_profile: StorageProfile,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let storage_profile = serialize_storage_profile(storage_profile)?;

    sqlx::query!(
        r#"
            UPDATE
                warehouse
            SET
                storage_profile = $1
            WHERE
                warehouse_id = $2
        "#,
        storage_profile,
        warehouse_id.as_uuid(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?;

    Ok(())
}

pub(crate) async fn update_warehouse_name<'a>(
    warehouse_id: &WarehouseIdent,
    warehouse_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    validate_warehouse_name(warehouse_name)?;

    sqlx::query!(
        r#"
            UPDATE
                warehouse
            SET
                warehouse_name = $1
            WHERE
                warehouse_id = $2
        "#,
        warehouse_name,
        warehouse_id.as_uuid(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?;

    Ok(())
}

pub(crate) async fn get_warehouse(
    warehouse_id: &WarehouseIdent,
    catalog_state: CatalogState,
) -> Result<GetWarehouseResponse> {
    let res = sqlx::query!(
        r#"
                SELECT 
                    warehouse_id AS "warehouse_ident: WarehouseIdent",
                    warehouse_name AS "warehouse_name: String",
                    storage_profile AS "storage_profile: Value",
                    storage_secret_id AS "storage_secret_id: Uuid",
                    status AS "warehouse_status: WarehouseStatus"
                FROM 
                   warehouse
                WHERE  
                    warehouse_id = $1
    
        "#,
        warehouse_id.as_uuid(),
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?;

    Ok(GetWarehouseResponse {
        warehouse_ident: res.warehouse_ident,
        warehouse_name: res.warehouse_name,
        storage_profile: deserialize_storage_profile(res.storage_profile)?,
        warehouse_status: res.warehouse_status,
        storage_credential_id: res.storage_secret_id.map(SecretIdent::from),
    })
}

pub(crate) async fn change_warehouse_status<'a>(
    warehouse_id: &WarehouseIdent,
    new_status: WarehouseStatus,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query!(
        r#"
            UPDATE 
                warehouse
            SET 
                status = $1
            WHERE
                warehouse_id = $2
        "#,
        new_status as WarehouseStatus,
        warehouse_id.as_uuid(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => warehouse_not_found(warehouse_id),
        _ => e.into_error_model("Error fetching warehouses".to_string()),
    })?;

    Ok(())
}

fn warehouse_not_found(warehouse_id: &WarehouseIdent) -> ErrorModel {
    ErrorModel::builder()
        .code(StatusCode::NOT_FOUND.into())
        .message(format!("Warehouse not found: '{warehouse_id:?}'"))
        .r#type("WarehouseNotFound")
        .build()
}

fn serialize_storage_profile(storage_profile: StorageProfile) -> Result<Value, ErrorModel> {
    serde_json::to_value(storage_profile).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing storage profile")
            .r#type("StorageProfileSerializationError")
            .stack(Some(vec![e.to_string()]))
            .build()
    })
}

fn deserialize_storage_profile(storage_profile: Value) -> Result<StorageProfile, ErrorModel> {
    serde_json::from_value(storage_profile).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error deserializing storage profile")
            .r#type("StorageProfileDeserializationError")
            .stack(Some(vec![e.to_string()]))
            .build()
    })
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
            region: "us-east-1".to_string(),
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
