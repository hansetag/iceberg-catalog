use super::{dbutils::DBErrorHandler, CatalogState};
use crate::api::iceberg::v1::MAX_PAGE_SIZE;
use crate::implementations::postgres::pagination::{PaginateToken, V1PaginateToken};
use crate::service::{
    CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent, Result,
};
use crate::{catalog::namespace::MAX_NAMESPACE_DEPTH, service::NamespaceIdentUuid, WarehouseIdent};
use chrono::Utc;
use http::StatusCode;
use sqlx::types::Json;
use std::{collections::HashMap, ops::Deref};
use uuid::Uuid;

pub(crate) async fn get_namespace(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GetNamespaceResponse> {
    let row = sqlx::query!(
        r#"
        SELECT 
            namespace_id,
            n.warehouse_id,
            namespace_properties as "properties: Json<Option<HashMap<String, String>>>"
        FROM namespace n
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1 AND n.namespace_name = $2
        AND w.status = 'active'
        "#,
        *warehouse_id,
        namespace.as_ref()
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
        properties: row.properties.deref().clone(),
        namespace_id: row.namespace_id.into(),
        warehouse_id: row.warehouse_id.into(),
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_namespaces(
    warehouse_id: WarehouseIdent,
    ListNamespacesQuery {
        page_token,
        page_size,
        parent,
    }: &ListNamespacesQuery,
    catalog_state: CatalogState,
) -> Result<ListNamespacesResponse> {
    let page_size = page_size
        .map(i64::from)
        .map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

    // Treat empty parent as None
    let parent = parent
        .as_ref()
        .and_then(|p| if p.is_empty() { None } else { Some(p.clone()) });
    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();

    let namespaces: Vec<(Uuid, Vec<String>, chrono::DateTime<Utc>)> = if let Some(parent) = parent {
        // If it doesn't fit in a i32 it is way too large. Validation would have failed
        // already in the catalog.
        let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

        // Namespace name field is an array.
        // Get all namespaces where the "name" array has
        // length(parent) + 1 elements, and the first length(parent)
        // elements are equal to parent.
        sqlx::query!(
            r#"
            SELECT
                n.namespace_id,
                "namespace_name"[$2 + 1:] as "namespace_name: Vec<String>",
                n.created_at
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND array_length("namespace_name", 1) = $2 + 1
            AND "namespace_name"[1:$2] = $3
            --- PAGINATION
            AND ((n.created_at > $4 OR $4 IS NULL) OR (n.created_at = $4 AND n.namespace_id > $5))
            ORDER BY n.created_at, n.namespace_id ASC
            LIMIT $6
            "#,
            *warehouse_id,
            parent_len,
            &*parent,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&catalog_state.read_pool())
        .await
        .map_err(|e| e.into_error_model("Error fetching Namespace".into()))?
        .into_iter()
        .filter_map(|r| match r.namespace_name {
            Some(n) => Some((r.namespace_id, n, r.created_at)),
            None => None,
        })
        .collect()
    } else {
        sqlx::query!(
            r#"
            SELECT
                n.namespace_id,
                "namespace_name" as "namespace_name: Vec<String>",
                n.created_at
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND ((n.created_at > $2 OR $2 IS NULL) OR (n.created_at = $2 AND n.namespace_id > $3))
            ORDER BY n.created_at, n.namespace_id ASC
            LIMIT $4
            "#,
            *warehouse_id,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&catalog_state.read_pool())
        .await
        .map_err(|e| e.into_error_model("Error fetching Namespace".into()))?
        .into_iter()
        .map(|r| (r.namespace_id, r.namespace_name, r.created_at))
        .collect()
    };
    let next_page_token = namespaces.last().map(|(id, _, ts)| {
        PaginateToken::V1(V1PaginateToken {
            id: *id,
            created_at: *ts,
        })
        .to_string()
    });

    // Convert Vec<Vec<String>> to Vec<NamespaceIdent>
    let namespaces: Result<Vec<NamespaceIdent>> = namespaces
        .iter()
        .map(|(_, n, _)| {
            NamespaceIdent::from_vec(n.to_owned()).map_err(|e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error converting namespace".to_string())
                    .r#type("NamespaceConversionError".to_string())
                    .source(Some(Box::new(e)))
                    .build()
                    .into()
            })
        })
        .collect();
    let namespaces = namespaces?;

    Ok(ListNamespacesResponse {
        next_page_token,
        namespaces,
    })
}

pub(crate) async fn create_namespace(
    warehouse_id: WarehouseIdent,
    namespace_id: NamespaceIdentUuid,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateNamespaceResponse> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;

    let _namespace_id = sqlx::query_scalar!(
        r#"
        INSERT INTO namespace (warehouse_id, namespace_id, namespace_name, namespace_properties)
        (
            SELECT $1, $2, $3, $4
            WHERE EXISTS (
                SELECT 1
                FROM warehouse
                WHERE warehouse_id = $1
                AND status = 'active'
        ))
        RETURNING namespace_id
        "#,
        *warehouse_id,
        *namespace_id,
        &*namespace,
        serde_json::to_value(properties.clone()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error serializing namespace properties".to_string())
                .r#type("NamespacePropertiesSerializationError".to_string())
                .source(Some(Box::new(e)))
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
                    .source(Some(Box::new(db_error)))
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
    warehouse_id: WarehouseIdent,
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
        *warehouse_id,
        &**namespace
    )
    .fetch_one(&catalog_state.read_pool())
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
    warehouse_id: WarehouseIdent,
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
        *warehouse_id,
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
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    properties: HashMap<String, String>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let properties = serde_json::to_value(properties).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing namespace properties".to_string())
            .r#type("NamespacePropertiesSerializationError".to_string())
            .source(Some(Box::new(e)))
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
        *warehouse_id,
        &**namespace
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error updating namespace properties".to_string()))?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {

    use crate::implementations::postgres::PostgresTransaction;
    use crate::service::{Catalog as _, Transaction as _};

    use super::super::warehouse::test::initialize_warehouse;
    use super::super::PostgresCatalog;
    use super::*;
    use crate::implementations::postgres::tabular::table::tests::initialize_table;

    pub(crate) async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> CreateNamespaceResponse {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let namespace_id = NamespaceIdentUuid::default();

        let response = PostgresCatalog::create_namespace(
            warehouse_id,
            namespace_id,
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
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response =
            PostgresCatalog::get_namespace(warehouse_id, &namespace, transaction.transaction())
                .await
                .unwrap();

        drop(transaction);

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response =
            PostgresCatalog::namespace_ident_to_id(warehouse_id, &namespace, state.clone())
                .await
                .unwrap()
                .is_some();

        assert!(response);

        let response = PostgresCatalog::list_namespaces(
            warehouse_id,
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

        let new_props = HashMap::from_iter(vec![
            ("key2".to_string(), "updated_value".to_string()),
            ("new_key".to_string(), "new_value".to_string()),
        ]);
        PostgresCatalog::update_namespace_properties(
            warehouse_id,
            &namespace,
            new_props.clone(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let response = PostgresCatalog::get_namespace(warehouse_id, &namespace, t.transaction())
            .await
            .unwrap();
        drop(t);
        assert_eq!(response.properties, Some(new_props));

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresCatalog::drop_namespace(warehouse_id, &namespace, transaction.transaction())
            .await
            .expect("Error dropping namespace");
    }

    #[sqlx::test]
    async fn test_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response1 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let namespace = NamespaceIdent::from_vec(vec!["test2".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let response2 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;
        let namespace = NamespaceIdent::from_vec(vec!["test3".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let response3 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let ListNamespacesResponse {
            namespaces,
            next_page_token,
        } = PostgresCatalog::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: crate::api::iceberg::v1::PageToken::NotSpecified,
                page_size: Some(1),
                parent: None,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces, vec![response1.namespace.clone()]);

        let ListNamespacesResponse {
            namespaces,
            next_page_token,
        } = PostgresCatalog::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    crate::api::iceberg::v1::PageToken::Empty,
                    crate::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(2),
                parent: None,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(namespaces.len(), 2);
        assert!(next_page_token.is_some());
        assert_eq!(
            namespaces,
            vec![response2.namespace.clone(), response3.namespace.clone()]
        );

        // last page is empty
        let ListNamespacesResponse {
            namespaces,
            next_page_token,
        } = PostgresCatalog::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    crate::api::iceberg::v1::PageToken::Empty,
                    crate::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(3),
                parent: None,
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(next_page_token, None);
        assert_eq!(namespaces, vec![]);
    }

    #[sqlx::test]
    async fn test_cannot_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(warehouse_id, &table.namespace, transaction.transaction())
            .await
            .unwrap_err();

        assert_eq!(result.error.code, StatusCode::CONFLICT);
    }

    #[sqlx::test]
    async fn test_case_insensitive_but_preserve_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace_1 = NamespaceIdent::from_vec(vec!["Test".to_string()]).unwrap();
        let namespace_2 = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresCatalog::create_namespace(
            warehouse_id,
            NamespaceIdentUuid::default(),
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

        let response = PostgresCatalog::create_namespace(
            warehouse_id,
            NamespaceIdentUuid::default(),
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
