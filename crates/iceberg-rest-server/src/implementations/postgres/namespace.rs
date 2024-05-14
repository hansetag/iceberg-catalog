use http::StatusCode;
use iceberg_rest_service::{
    v1::{ListNamespacesQuery, NamespaceIdent},
    CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesResponse, Result, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
};
use sqlx::types::Json;
use std::{collections::HashMap, ops::Deref};

use crate::{
    catalog::namespace::MAX_NAMESPACE_DEPTH,
    service::{storage::StorageProfile, GetStorageConfigResult, NamespaceIdentUuid},
    SecretIdent, WarehouseIdent,
};

use super::{dbutils::DBErrorHandler, CatalogState};

pub(crate) async fn get_namespace_metadata(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GetNamespaceResponse> {
    let properties = sqlx::query_scalar!(
        r#"
        SELECT 
            properties as "properties: Json<HashMap<String, String>>"
        FROM namespace
        WHERE warehouse_id = $1 AND "name" = $2
        "#,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Namespace not found".to_string())
            .r#type("NamespaceNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    Ok(GetNamespaceResponse {
        namespace: namespace.to_owned(),
        properties: Some(properties.deref().clone()),
    })
}

pub(crate) async fn list_namespaces(
    warehouse_id: &WarehouseIdent,
    query: &ListNamespacesQuery,
    catalog_state: CatalogState,
) -> Result<ListNamespacesResponse> {
    let ListNamespacesQuery {
        page_token: _,
        page_size: _,
        parent,
    } = query;

    // Treat empty parent as None
    let parent = parent
        .as_ref()
        .and_then(|p| if p.is_empty() { None } else { Some(p.clone()) });

    let namespaces = if let Some(parent) = parent {
        // If it doesn't fit in a i32 it is way too large. Validation would have failed
        // already in the catalog.
        let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

        // Namespace name field is an array.
        // Get all namespaces where the "name" array has
        // length(parent) + 1 elements, and the first length(parent)
        // elements are equal to parent.
        sqlx::query_scalar!(
            r#"
            SELECT
                "name"[$2 + 1:] as "name: Vec<String>"
            FROM namespace
            WHERE warehouse_id = $1
            AND array_length("name", 1) = $2 + 1
            AND "name"[1:$2] = $3
            "#,
            warehouse_id.as_uuid(),
            parent_len,
            &*parent
        )
        .fetch_all(&catalog_state.read_pool)
        .await
        .map_err(|e| e.into_error_model("Error fetching Namespace".into()))?
        .into_iter()
        .flatten()
        .collect()
    } else {
        sqlx::query_scalar!(
            r#"
            SELECT
                "name" as "name: Vec<String>"
            FROM namespace
            WHERE warehouse_id = $1
            "#,
            warehouse_id.as_uuid()
        )
        .fetch_all(&catalog_state.read_pool)
        .await
        .map_err(|e| e.into_error_model("Error fetching Namespace".into()))?
    };

    // Convert Vec<Vec<String>> to Vec<NamespaceIdent>
    let namespaces: Result<Vec<NamespaceIdent>> = namespaces
        .iter()
        .map(|n| {
            NamespaceIdent::from_vec(n.to_owned()).map_err(|e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error converting namespace".to_string())
                    .r#type("NamespaceConversionError".to_string())
                    .stack(Some(vec![e.to_string()]))
                    .build()
                    .into()
            })
        })
        .collect();

    Ok(ListNamespacesResponse {
        next_page_token: None,
        namespaces: namespaces?,
    })
}

pub(crate) async fn create_namespace(
    warehouse_id: &WarehouseIdent,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateNamespaceResponse> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;

    let _namespace_id = sqlx::query_scalar!(
        r#"
        INSERT INTO namespace (warehouse_id, "name", properties)
        VALUES ($1, $2, $3)
        RETURNING namespace_id
        "#,
        warehouse_id.as_uuid(),
        &*namespace,
        serde_json::to_value(properties.clone()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error serializing namespace properties".to_string())
                .r#type("NamespacePropertiesSerializationError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message("Namespace already exists".to_string())
                    .r#type("NamespaceAlreadyExists".to_string())
                    .build()
            } else if db_error.is_foreign_key_violation() {
                ErrorModel::builder()
                    .code(StatusCode::NOT_FOUND.into())
                    .message("Warehouse not found".to_string())
                    .r#type("WarehouseNotFound".to_string())
                    .build()
            } else {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error creating namespace".to_string())
                    .r#type("NamespaceCreateError".to_string())
                    .stack(Some(vec![db_error.to_string()]))
                    .build()
            }
        }
        _ => e.into_error_model("Error creating Namespace".into()),
    })?;

    // If inner is empty, return None
    let properties = properties.and_then(|h| if h.is_empty() { None } else { Some(h) });
    Ok(CreateNamespaceResponse {
        namespace,
        // Return None if properties is empty
        properties,
    })
}

pub(crate) async fn namespace_ident_to_id(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    catalog_state: CatalogState,
) -> Result<Option<NamespaceIdentUuid>> {
    let namespace_id = sqlx::query_scalar!(
        r#"
        SELECT "namespace_id"
        FROM namespace
        WHERE warehouse_id = $1 AND "name" = $2
        "#,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => None,
        _ => Some(e.into_error_model("Error fetching namespace".to_string())),
    });

    match namespace_id {
        Ok(namespace_id) => Ok(Some(namespace_id.into())),
        Err(Some(e)) => Err(e.into()),
        Err(None) => Ok(None),
    }
}

pub(crate) async fn drop_namespace(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    // Return 404 not found if namespace does not exist
    let row_count = sqlx::query_scalar!(
        r#"
        WITH deleted AS (
            DELETE FROM namespace
            WHERE warehouse_id = $1 AND "name" = $2
            RETURNING *
        )
        SELECT count(*) FROM deleted
        "#,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Namespace not found".to_string())
            .r#type("NamespaceNotFound".to_string())
            .build(),
        sqlx::Error::Database(db_error) => {
            if db_error.is_foreign_key_violation() {
                ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message("Namespace is not empty".to_string())
                    .r#type("NamespaceNotEmpty".to_string())
                    .build()
            } else {
                e.into_error_model("Error deleting namespace".to_string())
            }
        }
        _ => e.into_error_model("Error deleting namespace".to_string()),
    })?;

    if row_count == Some(0) {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Namespace not found".to_string())
            .r#type("NamespaceNotFound".to_string())
            .build()
            .into());
    }

    Ok(())
}

pub(crate) async fn update_namespace_properties(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    request: UpdateNamespacePropertiesRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<UpdateNamespacePropertiesResponse> {
    let UpdateNamespacePropertiesRequest { removals, updates } = request;

    let mut properties = get_namespace_metadata(warehouse_id, namespace, &mut *transaction)
        .await?
        .properties
        .unwrap_or_default();

    let mut updated = vec![];
    let mut removed = vec![];
    let mut missing = vec![];

    if let Some(removals) = removals {
        for key in removals {
            if properties.remove(&key).is_some() {
                removed.push(key.clone());
            } else {
                missing.push(key.clone());
            }
        }
    }

    if let Some(updates) = updates {
        for (key, value) in updates {
            // Push to updated if the value for the key is different.
            // Also push on insert
            if properties.insert(key.clone(), value.clone()) != Some(value.clone()) {
                updated.push(key.clone());
            }
        }
    }

    let properties = serde_json::to_value(properties).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing namespace properties".to_string())
            .r#type("NamespacePropertiesSerializationError".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    sqlx::query!(
        r#"
        UPDATE namespace
        SET properties = $1
        WHERE warehouse_id = $2 AND "name" = $3
        "#,
        properties,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error updating namespace properties".to_string()))?;

    Ok(UpdateNamespacePropertiesResponse {
        updated,
        removed,
        missing: if missing.is_empty() {
            None
        } else {
            Some(missing)
        },
    })
}

pub(crate) async fn get_storage_config(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    catalog_state: CatalogState,
) -> Result<GetStorageConfigResult> {
    let storage = sqlx::query!(
        r#"
            SELECT
                n.namespace_id,
                w.storage_profile as "storage_profile: Json<StorageProfile>",
                w."storage_secret_id"
            FROM namespace n
            inner join warehouse w on n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND n."name" = $2
            "#,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Namespace not found".to_string())
            .r#type("NamespaceNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    let storage_profile = storage.storage_profile.deref().clone();
    let storage_secret_ident = storage.storage_secret_id.map(SecretIdent::from);
    let namespace_id = storage.namespace_id;

    Ok(GetStorageConfigResult {
        storage_profile,
        storage_secret_ident,
        namespace_id: namespace_id.into(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::implementations::postgres::PostgresTransaction;
    use crate::service::{Catalog as _, Transaction as _};

    use super::super::table::tests::initialize_table;
    use super::super::warehouse::test::initialize_warehouse;
    use super::super::Catalog;
    use super::*;

    pub(crate) async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> CreateNamespaceResponse {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = Catalog::create_namespace(
            warehouse_id,
            CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties: properties.clone(),
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        response
    }

    #[sqlx::test]
    async fn test_namespace_lifecycle(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None).await;

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response =
            initialize_namespace(state.clone(), &warehouse_id, &namespace, properties.clone())
                .await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response =
            Catalog::get_namespace_metadata(&warehouse_id, &namespace, transaction.transaction())
                .await
                .unwrap();

        drop(transaction);

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response = Catalog::namespace_ident_to_id(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap()
            .is_some();

        assert!(response);

        let response = Catalog::list_namespaces(
            &warehouse_id,
            &ListNamespacesQuery {
                page_token: iceberg_rest_service::v1::PageToken::NotSpecified,
                page_size: None,
                parent: None,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(response.namespaces, vec![namespace.clone()]);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = Catalog::update_namespace_properties(
            &warehouse_id,
            &namespace,
            UpdateNamespacePropertiesRequest {
                removals: Some(vec!["nonexistant".to_string(), "key1".to_string()]),
                updates: Some(HashMap::from_iter(vec![
                    ("key2".to_string(), "updated_value".to_string()),
                    ("new_key".to_string(), "new_value".to_string()),
                ])),
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        let mut response_updated = response.updated.clone();
        response_updated.sort();
        assert_eq!(
            response_updated,
            vec![String::from("key2"), String::from("new_key")]
        );
        assert_eq!(response.removed, vec![String::from("key1")]);
        assert_eq!(response.missing, Some(vec![String::from("nonexistant")]));

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        Catalog::drop_namespace(&warehouse_id, &namespace, transaction.transaction())
            .await
            .expect("Error dropping namespace");
    }

    #[sqlx::test]
    async fn test_cannot_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None).await;
        let staged = false;
        let table = initialize_table(&warehouse_id, state.clone(), staged).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(&warehouse_id, &table.namespace, transaction.transaction())
            .await
            .unwrap_err();

        assert_eq!(result.error.code, StatusCode::CONFLICT);
    }
}
