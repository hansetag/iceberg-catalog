use crate::service::{
    CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use http::StatusCode;
use sqlx::types::Json;
use std::{collections::HashMap, ops::Deref};

use crate::{catalog::namespace::MAX_NAMESPACE_DEPTH, service::NamespaceIdentUuid, WarehouseIdent};

use super::{dbutils::DBErrorHandler, CatalogState};

pub(crate) async fn get_namespace(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GetNamespaceResponse> {
    let row = sqlx::query!(
        r#"
        SELECT 
            namespace_id,
            n.warehouse_id,
            namespace_properties as "properties: Json<HashMap<String, String>>"
        FROM namespace n
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1 AND n.namespace_name = $2
        AND w.status = 'active'
        "#,
        warehouse_id.as_uuid(),
        &**namespace
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("Namespace not found: {:?}", namespace.as_ref()))
            .r#type("NamespaceNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    Ok(GetNamespaceResponse {
        namespace: namespace.to_owned(),
        properties: Some(row.properties.deref().clone()),
        namespace_id: row.namespace_id.into(),
        warehouse_id: row.warehouse_id.into(),
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
                "namespace_name"[$2 + 1:] as "namespace_name: Vec<String>"
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND array_length("namespace_name", 1) = $2 + 1
            AND "namespace_name"[1:$2] = $3
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
                "namespace_name" as "namespace_name: Vec<String>"
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
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
        INSERT INTO namespace (warehouse_id, namespace_name, namespace_properties)
        (
            SELECT $1, $2, $3
            WHERE EXISTS (
                SELECT 1
                FROM warehouse
                WHERE warehouse_id = $1
                AND status = 'active'
        ))
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
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Warehouse not found".to_string())
            .r#type("WarehouseNotFound".to_string())
            .build(),
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
        SELECT namespace_id
        FROM namespace n
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1 AND namespace_name = $2
        AND w.status = 'active'
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
            WHERE warehouse_id = $1 
            AND namespace_name = $2
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
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
            .message(format!("Namespace not found: {:?}", namespace.as_ref()))
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
            .message(format!("Namespace not found: {:?}", namespace.as_ref()))
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

    let mut properties = get_namespace(warehouse_id, namespace, &mut *transaction)
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
        SET namespace_properties = $1
        WHERE warehouse_id = $2 AND namespace_name = $3
        AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
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

#[cfg(test)]
pub(crate) mod tests {
    use crate::implementations::postgres::PostgresTransaction;
    use crate::service::{Catalog as _, Transaction as _};

    use super::super::warehouse::test::initialize_warehouse;
    use super::super::Catalog;
    use super::*;
    use crate::implementations::postgres::tabular::table::tests::initialize_table;

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;

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

        let response = Catalog::get_namespace(&warehouse_id, &namespace, transaction.transaction())
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
                page_token: crate::api::iceberg::v1::PageToken::NotSpecified,
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

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
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

    #[sqlx::test]
    async fn test_case_insensitive_but_preserve_case(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace_1 = NamespaceIdent::from_vec(vec!["Test".to_string()]).unwrap();
        let namespace_2 = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = Catalog::create_namespace(
            &warehouse_id,
            CreateNamespaceRequest {
                namespace: namespace_1.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Check that the namespace is created with the correct case
        assert_eq!(response.namespace, namespace_1);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = Catalog::create_namespace(
            &warehouse_id,
            CreateNamespaceRequest {
                namespace: namespace_2.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::CONFLICT);
        assert_eq!(response.error.r#type, "NamespaceAlreadyExists");
    }
}
