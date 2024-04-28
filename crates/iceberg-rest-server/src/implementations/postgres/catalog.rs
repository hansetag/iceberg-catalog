use std::{ops::Deref, vec};

use super::CatalogState;
use crate::service::{storage::StorageProfile, Catalog, ProjectIdent, WarehouseIdent};
use http::StatusCode;
use iceberg_rest_service::{
    v1::{
        CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesQuery,
        ListNamespacesResponse, NamespaceIdent, Result, UpdateNamespacePropertiesRequest,
        UpdateNamespacePropertiesResponse,
    },
    ErrorModel,
};
use sqlx::types::Json;
use std::collections::HashMap;

// If this is increased, we need to modify namespace creation and deletion
// to take care of the hierarchical structure.
const MAX_NAMESPACE_DEPTH: i32 = 1;

fn validate_namespace(namespace: &NamespaceIdent) -> Result<()> {
    if namespace.len() > MAX_NAMESPACE_DEPTH as usize {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message(format!(
                "Namespace exceeds maximum depth of {MAX_NAMESPACE_DEPTH}",
            ))
            .r#type("NamespaceDepthExceeded".to_string())
            .build()
            .into());
    }

    Ok(())
}

#[async_trait::async_trait]
impl Catalog<CatalogState> for super::PostgresCatalog {
    async fn create_warehouse_profile(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        state: CatalogState,
    ) -> Result<WarehouseIdent> {
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
            INSERT INTO warehouse (warehouse_name, project_id, storage_profile)
            VALUES ($1, $2, $3)
            RETURNING warehouse_id
            "#,
            warehouse_name,
            project_id.as_uuid(),
            storage_profile_ser
        )
        .fetch_one(&state.write_pool)
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

    async fn list_namespaces(
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
            let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

            validate_namespace(&parent)?;

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
            .map_err(|e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error fetching namespace".to_string())
                    .r#type("NamespaceFetchError".to_string())
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?
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
            .map_err(|e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error fetching namespace".to_string())
                    .r#type("NamespaceFetchError".to_string())
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?
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

    async fn create_namespace(
        warehouse_id: &WarehouseIdent,
        request: CreateNamespaceRequest,
        state: CatalogState,
    ) -> Result<CreateNamespaceResponse> {
        let CreateNamespaceRequest {
            namespace,
            properties,
        } = request;

        validate_namespace(&namespace)?;
        if namespace.iter().any(std::string::String::is_empty) {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Namespace parts cannot be empty".to_string())
                .r#type("EmptyNamespacePart".to_string())
                .build()
                .into());
        }

        // If "location" extra property is set, throw a not supported error
        if let Some(properties) = &properties {
            if properties.contains_key("location") {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("Specifying the 'location' property for Namespaces is not supported. 'location' is managed by the catalog.".to_string())
                    .r#type("LocationPropertyNotSupported".to_string())
                    .build()
                    .into());
            }
        }

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
        .fetch_one(&state.write_pool)
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
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error creating namespace".to_string())
                .r#type("NamespaceCreateError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
        })?;

        // If inner is empty, return None
        let properties = properties.and_then(|h| if h.is_empty() { None } else { Some(h) });
        Ok(CreateNamespaceResponse {
            namespace,
            // Return None if properties is empty
            properties,
        })
    }

    async fn get_namespace_metadata(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: CatalogState,
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
        .fetch_one(&state.read_pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace not found".to_string())
                .r#type("NamespaceNotFound".to_string())
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching namespace".to_string())
                .r#type("NamespaceFetchError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
        })?;

        Ok(GetNamespaceResponse {
            namespace: namespace.to_owned(),
            properties: Some(properties.deref().clone()),
        })
    }

    async fn namespace_exists(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: CatalogState,
    ) -> Result<bool> {
        validate_namespace(namespace)?;

        let exists = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM namespace
                WHERE warehouse_id = $1 AND "name" = $2
            ) as "exists"
            "#,
            warehouse_id.as_uuid(),
            &**namespace
        )
        .fetch_one(&catalog_state.read_pool)
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching namespace".to_string())
                .r#type("NamespaceFetchError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
        .unwrap_or(false);

        Ok(exists)
    }

    async fn drop_namespace(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: CatalogState,
    ) -> Result<()> {
        validate_namespace(namespace)?;

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
        .fetch_one(&catalog_state.write_pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace not found".to_string())
                .r#type("NamespaceNotFound".to_string())
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error deleting namespace".to_string())
                .r#type("NamespaceDeleteError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
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

    async fn update_namespace_properties(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        request: UpdateNamespacePropertiesRequest,
        catalog_state: CatalogState,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        let UpdateNamespacePropertiesRequest { removals, updates } = request;
        validate_namespace(namespace)?;

        let mut transaction = catalog_state.write_pool.begin().await.map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error starting transaction".to_string())
                .r#type("TransactionStartError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

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
        .fetch_one(&mut *transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace not found".to_string())
                .r#type("NamespaceNotFound".to_string())
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching namespace".to_string())
                .r#type("NamespaceFetchError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build(),
        })?;

        let mut updated = vec![];
        let mut removed = vec![];
        let mut missing = vec![];

        let mut properties = properties.deref().clone();
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
        .execute(&mut *transaction)
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error updating namespace properties".to_string())
                .r#type("NamespacePropertiesUpdateError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

        transaction.commit().await.map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error committing transaction".to_string())
                .r#type("TransactionCommitError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

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
}

#[cfg(test)]
mod tests {
    use crate::service::storage::S3Profile;

    use super::super::PostgresCatalog;
    use super::*;

    #[sqlx::test]
    async fn test_namespace(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = PostgresCatalog::create_warehouse_profile(
            "test_warehouse".to_string(),
            ProjectIdent::from(uuid::Uuid::new_v4()),
            StorageProfile::S3(S3Profile {
                bucket: "test_bucket".to_string(),
                endpoint: None,
                region: None,
                assume_role_arn: None,
            }),
            state.clone(),
        )
        .await
        .unwrap();

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]
            .into_iter(),
        ));

        let response = PostgresCatalog::create_namespace(
            &warehouse_id,
            CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties: properties.clone(),
            },
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response =
            PostgresCatalog::get_namespace_metadata(&warehouse_id, &namespace, state.clone())
                .await
                .unwrap();

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let response = PostgresCatalog::namespace_exists(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap();

        assert!(response);

        let response = PostgresCatalog::list_namespaces(
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

        let response = PostgresCatalog::update_namespace_properties(
            &warehouse_id,
            &namespace,
            UpdateNamespacePropertiesRequest {
                removals: Some(vec!["nonexistant".to_string(), "key1".to_string()]),
                updates: Some(HashMap::from_iter(
                    vec![
                        ("key2".to_string(), "updated_value".to_string()),
                        ("new_key".to_string(), "new_value".to_string()),
                    ]
                    .into_iter(),
                )),
            },
            state.clone(),
        )
        .await
        .unwrap();

        let mut response_updated = response.updated.clone();
        response_updated.sort();
        assert_eq!(
            response_updated,
            vec![String::from("key2"), String::from("new_key")]
        );
        assert_eq!(response.removed, vec![String::from("key1")]);
        assert_eq!(response.missing, Some(vec![String::from("nonexistant")]));

        let response = PostgresCatalog::drop_namespace(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap();

        assert_eq!(response, ());
    }
}
