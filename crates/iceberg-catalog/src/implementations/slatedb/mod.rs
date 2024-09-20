#![allow(unused_variables)]

use futures::future::join_all;
use tokio::task::JoinSet;
use http::StatusCode;
use uuid::Uuid;
use std::collections::{HashMap, HashSet};
use slatedb::error::SlateDBError;
use std::sync::Arc;
use iceberg_ext::catalog::rest::ErrorModel;

use iceberg::spec::ViewMetadata;
use crate::service::{health::{HealthExt, HealthStatus, Health}, Catalog, Transaction, Result, TableCommit, IcebergErrorResponse};
use crate::{
    service::{
        storage::StorageProfile, CreateTableResponse, GetNamespaceResponse,
        GetTableMetadataResponse, LoadTableResponse, NamespaceIdentUuid, ProjectIdent,
        TableIdentUuid, WarehouseIdent,
    },
    SecretIdent,
};
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::api::iceberg::v1::{PaginatedTabulars, PaginationQuery};
use crate::api::iceberg::types::PageToken;
use crate::service::tabular_idents::{TabularIdentOwned, TabularIdentUuid};
use crate::service::{
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, DeletionDetails,
    GetWarehouseResponse, ListFlags, ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent,
    TableIdent, WarehouseStatus,
};


#[derive(Clone)]
pub struct SlateDbCatalog {}

#[derive(Clone)]
pub struct SlateDbTransaction {
    state: SlateDbState
}

#[derive(Clone)]
pub struct SlateDbState {
    db: Arc<slatedb::db::Db>
}

#[async_trait::async_trait]
impl HealthExt for SlateDbState {
    async fn health(&self) -> Vec<Health> {
        vec![Health::now("slatedb", HealthStatus::Healthy)]
    }
    async fn update_health(&self) {} 
}

#[async_trait::async_trait]
impl Transaction<SlateDbState> for SlateDbTransaction {
    type Transaction<'a> = SlateDbState;

    async fn begin_write(state: SlateDbState) -> Result<Self> {
        Ok(Self { state })
    }

    async fn begin_read(state: SlateDbState) -> Result<Self> {
        Ok(Self { state })
    }

    async fn commit(self) -> Result<()> {
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Not implemented".to_string())
            .r#type("NotImplemented".to_string())
            .build()
            .into())
    }

    fn transaction(&mut self) -> Self::Transaction<'_> { self.state.clone() } 
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IdList(Vec<uuid::Uuid>);

type Warehouse = crate::api::management::v1::warehouse::GetWarehouseResponse;
#[derive(serde::Serialize, serde::Deserialize)]
struct Namespace {
    namespace_id: Uuid,
    namespace: NamespaceIdent,
    properties: Option<HashMap<String, String>>,
}



pub(crate) trait SlateDbErrorHandler
where
    Self: ToString + Sized + Send + Sync + std::error::Error + 'static,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        ErrorModel::internal(message, "DatabaseError", Some(Box::new(self)))
    }
}

impl SlateDbErrorHandler for SlateDBError { }
impl SlateDbErrorHandler for serde_json::Error { }

#[async_trait::async_trait]
impl Catalog for SlateDbCatalog {
    type Transaction = SlateDbTransaction;
    type State = SlateDbState;

    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<WarehouseIdent> {
        let state = transaction;
        // Generate UUID
        let wid = WarehouseIdent::from(uuid::Uuid::new_v4());
        // Convert payload to binary format (serialize GetWarehouseResponse)
        // let payload = Warehouse::from(Warehouse::new(wid, warehouse_name, WarehouseStatus::Active, project_id, storage_profile));
        let payload = Warehouse {
            id: *wid,
            name: warehouse_name,
            status: WarehouseStatus::Active,
            project_id: *project_id,
            storage_profile,
        };
        let value = serde_json::to_vec(&payload).map_err(|e| {
            e.into_error_model(String::from("Failed to serialize warehouse"))
        })?;
        // Put into db
        // Update warehouse list
        let key = format!("warehouse/{}", wid.to_string());
        state.db.put(key.as_bytes(), &value).await;
        
        let key = format!("warehouse/_all_");
        let value = state.db.get(key.as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read warehouse list"))
        })?;
        
        let mut list: Vec<uuid::Uuid> = match value {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize warehouse list"))
            })?,
            None => Vec::new(),
        };
        list.push(wid.0);
        
        let value = serde_json::to_vec(&list).map_err(|e| {
            e.into_error_model(String::from("Failed to serialize warehouse list"))
        })?;
        state.db.put(key.as_bytes(), &value).await;

        Ok(wid)
    }

    async fn get_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<GetWarehouseResponse> {
        let state = transaction;
        let value = state.db.get(format!("warehouse/{}", warehouse_id.to_string()).as_bytes()).await;
        let payload: Warehouse = serde_json::from_slice(&value.unwrap().unwrap()).map_err(|e| {
            e.into_error_model(String::from("Failed to deserialize warehouse"))
        })?;
        Ok(GetWarehouseResponse {
            id: WarehouseIdent::from(payload.id),
            name: payload.name,
            status: payload.status,
            project_id: ProjectIdent::from(payload.project_id),
            storage_profile: payload.storage_profile,
            tabular_delete_profile: TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(5),
            },
            storage_secret_id: None,
        })
    }

    async fn get_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse> {
        let state = transaction;

        let namespace = namespace.to_url_string();
        let key = format!("namespace/{warehouse_id}/{namespace}");
        let value = state.db.get(key.as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read namespace"))
        })?;

        let payload: Namespace = match value {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize namespace"))
            })?,
            None => return Err(ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Namespace not found".to_string())
                .r#type("NotFound".to_string())
                .build()
                .into())
        };
        Ok(GetNamespaceResponse {
            warehouse_id,
            namespace: payload.namespace,
            namespace_id: NamespaceIdentUuid::from(payload.namespace_id),
            properties: payload.properties,
        })
    }

    async fn list_namespaces(
        warehouse_id: WarehouseIdent,
        query: &ListNamespacesQuery,
        catalog_state: Self::State,
    ) -> Result<ListNamespacesResponse> {
        let state = catalog_state;

        let key = format!("namespace/{warehouse_id}/_all_");
        let value = state.db.get(key.as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read namespace list"))
        })?;

        let list: Vec<NamespaceIdent> = match value {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize namespace list"))
            })?,
            None => Vec::new(),
        };


        let mut set = JoinSet::new();
        list.into_iter().map(|ns| {
            let state = state.clone();
            set.spawn(async move {
                Self::get_namespace(warehouse_id, &ns, state.clone()).await
            })
        }).collect::<Vec::<_>>();
        let results = set.join_all().await.into_iter().map(|r| r.unwrap().namespace);

        Ok(ListNamespacesResponse { next_page_token: None, namespaces: results.collect() })
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse> {
        let state = transaction;
        // Convert payload to binary format (serialize GetNamespaceResponse)
        let namespace_name = request.namespace.to_url_string();
        let payload = Namespace {
            namespace: request.namespace.clone(),
            namespace_id: *namespace_id,
            properties: request.properties.clone(),
        };
        let value = serde_json::to_vec(&payload).map_err(|e| {
            e.into_error_model(String::from("Failed to serialize namespace"))
        })?;
        // Put into db
        let key = format!("namespace/{warehouse_id}/{namespace_name}");
        state.db.put(key.as_bytes(), &value).await;
        
        // Save into list
        let key = format!("namespace/{warehouse_id}/_all_");
        let value = state.db.get(key.as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read namespace list"))
        })?;
        let mut list: Vec<NamespaceIdent> = match value {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize namespace list"))
            })?,
            None => Vec::new(),
        };
        list.push(request.namespace);
        let value = serde_json::to_vec(&list).map_err(|e| {
            e.into_error_model(String::from("Failed to serialize namespace list"))
        })?;
        state.db.put(key.as_bytes(), &value).await;

        Ok(CreateNamespaceResponse {
            namespace: payload.namespace,
            properties: payload.properties,
        })
    }

    async fn namespace_ident_to_id(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: Self::State,
    ) -> Result<Option<NamespaceIdentUuid>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn create_table<'a>(
        namespace_id: NamespaceIdentUuid,
        table: &TableIdent,
        table_id: TableIdentUuid,
        request: CreateTableRequest,
        // Metadata location may be none if stage-create is true
        metadata_location: Option<&str>,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<CreateTableResponse> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn list_tables(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TableIdentUuid, TableIdent>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    // Should also load staged tables but not tables of inactive warehouses
    async fn load_tables<'a>(
        warehouse_id: WarehouseIdent,
        tables: impl IntoIterator<Item = TableIdentUuid> + Send,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseIdent,
        table: TableIdentUuid,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseIdent,
        location: &str,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn table_ident_to_id(
        warehouse_id: WarehouseIdent,
        table: &TableIdent,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn rename_table<'a>(
        warehouse_id: WarehouseIdent,
        source_id: TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn drop_table<'a>(
        table_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn table_idents_to_ids(
        warehouse_id: WarehouseIdent,
        tables: HashSet<&TableIdent>,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseIdent,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    // ---------------- Management API ----------------
    async fn list_projects(catalog_state: Self::State) -> Result<HashSet<ProjectIdent>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn list_warehouses(
        project_id: ProjectIdent,
        include_inactive: Option<Vec<WarehouseStatus>>,
        warehouse_id_filter: Option<&HashSet<WarehouseIdent>>,
        catalog_state: Self::State,
    ) -> Result<Vec<GetWarehouseResponse>> {
        let value = catalog_state.db.get("warehouse/_all_".as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read warehouse list"))
        });

        let list: Vec<uuid::Uuid> = match value {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize warehouse list"))
            })?,
            _ => Vec::new(),
        };

        // Now we need to spawn a task for each warehouse to get the warehouse details
        let mut tasks = Vec::new();
        for wid in list {
            let state = catalog_state.clone();
            tasks.push(tokio::spawn(async move {
                Self::get_warehouse(WarehouseIdent::from(wid), state).await
            }));
        }

        let res = join_all(tasks).await;
        res.into_iter().map(|r| r.unwrap()).collect()
    }

    async fn delete_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        let state = transaction;
        // Delete warehouse
        state.db.delete(format!("warehouse/{warehouse_id}").as_bytes()).await;
        // Update warehouse list
        let key = String::from("warehouse/_all_");
        let value = state.db.get(key.as_bytes()).await.map_err(|e| {
            e.into_error_model(String::from("Failed to read warehouse list"))
        })?;
        
        let mut list: Vec<uuid::Uuid> = match value {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                e.into_error_model(String::from("Failed to deserialize warehouse list"))
            })?,
            None => Vec::new(),
        };
        list.retain(|&x| x != warehouse_id.0);
        
        let value = serde_json::to_vec(&list).map_err(|e| {
            e.into_error_model(String::from("Failed to serialize warehouse list"))
        })?;
        state.db.put(key.as_bytes(), &value).await;

        Ok(())
    }

    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseIdent,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseIdent,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<SlateDbState>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn create_view<'a>(
        namespace_id: NamespaceIdentUuid,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn view_ident_to_id(
        warehouse_id: WarehouseIdent,
        view: &TableIdent,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn load_view<'a>(
        view_id: TableIdentUuid,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<crate::implementations::postgres::tabular::view::ViewMetadataWithLocation> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn list_views(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        catalog_state: Self::State,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TableIdentUuid, TableIdent>> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn update_view_metadata(
        namespace_id: NamespaceIdentUuid,
        view_id: TableIdentUuid,
        view: &TableIdent,
        metadata_location: &str,
        metadata: ViewMetadata,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn rename_view(
        warehouse_id: WarehouseIdent,
        source_id: TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn drop_view<'a>(
        table_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn list_tabulars(
        warehouse_id: WarehouseIdent,
        list_flags: ListFlags,
        catalog_state: Self::State,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TabularIdentUuid, (TabularIdentOwned, Option<DeletionDetails>)>>
    {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }

    async fn mark_tabular_as_deleted(
        table_id: TabularIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        Err(ErrorModel::builder()
        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
        .message("Not implemented".to_string())
        .r#type("NotImplemented".to_string())
        .build()
        .into())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tokio;
    use crate::service::storage::{S3Flavor, S3Profile};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use slatedb::db::Db;
    use slatedb::config::DbOptions;

    async fn create_state() -> SlateDbState {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        SlateDbState {
            db: Arc::new(Db::open_with_opts(Path::from("/tmp/kv_store"), options, object_store).await.unwrap())
        }
    }

    async fn create_warehouse(state: SlateDbState, name: Option<String>) -> WarehouseIdent {

        let project_id = ProjectIdent::from(uuid::Uuid::nil());
        let storage_profile = StorageProfile::S3(S3Profile {
            bucket: "test_bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Minio,
        });

        let mut txn = SlateDbTransaction::begin_write(state.clone()).await.unwrap();

        SlateDbCatalog::create_warehouse(
            name.unwrap_or("test_warehouse".to_string()),  
            project_id,
            storage_profile,
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(5),
            },
            None,
            txn.transaction(),
        )
            .await
            .expect("Failed to create warehouse")
    }
    
    async fn create_namespace(state: SlateDbState, warehouse_id: WarehouseIdent, name: Option<String>) -> CreateNamespaceResponse {
        let mut txn = SlateDbTransaction::begin_write(state.clone()).await.unwrap();
        let namespace_id = NamespaceIdentUuid::from(uuid::Uuid::new_v4());
        let request = CreateNamespaceRequest {
            namespace: NamespaceIdent::new(name.unwrap_or("test_namespace".to_string())),
            properties: None,
        };
        SlateDbCatalog::create_namespace(
            warehouse_id,
            namespace_id,
            request,
            txn.transaction(),
        )
            .await
            .expect("Failed to create namespace")
    }

    #[tokio::test]
    async fn test_namespace_create() {
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        let ns = create_namespace(state.clone(), wh, None).await;
        assert_eq!(ns.namespace.to_url_string(), "test_namespace");
    }

    #[tokio::test]
    async fn test_namespace_get() {
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        let ns = create_namespace(state.clone(), wh, Some(String::from("ns1"))).await;
        let result = SlateDbCatalog::get_namespace(wh, &ns.namespace, state).await.expect("Failed to get namespace");
        assert_eq!(result.namespace.to_url_string(), "ns1");
    }

    #[tokio::test]
    async fn test_namespace_list() {
        let empty_query = ListNamespacesQuery {
            page_token: PageToken::Empty, // Assuming PageToken::Empty is a valid empty state
            page_size: None,
            parent: None,
        };
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        let ns1 = create_namespace(state.clone(), wh, Some(String::from("ns1"))).await;
        let ns2 = create_namespace(state.clone(), wh, Some(String::from("ns2"))).await;
        let result = SlateDbCatalog::list_namespaces(wh, &empty_query, state).await.expect("Failed to list namespaces");
        assert_eq!(result.namespaces.len(), 2);
        assert_eq!(result.namespaces[0].to_url_string(), "ns1");
        assert_eq!(result.namespaces[1].to_url_string(), "ns2");
    }

    #[tokio::test]
    async fn test_namespace_delete() {
        let empty_query = ListNamespacesQuery {
            page_token: PageToken::Empty, // Assuming PageToken::Empty is a valid empty state
            page_size: None,
            parent: None,
        };
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        let ns = create_namespace(state.clone(), wh, None).await;
        SlateDbCatalog::drop_namespace(wh, &ns.namespace, state.clone()).await.expect("Failed to delete namespace");

        let result = SlateDbCatalog::list_namespaces(wh, &empty_query, state).await.expect("Failed to list namespaces");
        assert_eq!(result.namespaces.len(), 0);
    }


    #[tokio::test]
    async fn test_warehouse_create() {
        let state = create_state().await;
        let wh = create_warehouse(state, None).await;
        assert_ne!(wh.0.to_string(), "");
    }

    #[tokio::test]
    async fn test_warehouse_get() {
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        let result = SlateDbCatalog::get_warehouse(wh, state).await.expect("Failed to get warehouse");
        assert_eq!(result.id, wh);
        assert_eq!(result.name, "test_warehouse");
        if let StorageProfile::S3(s3_profile) = &result.storage_profile {
            assert_eq!(s3_profile.bucket, "test_bucket");
            assert_eq!(s3_profile.region, "us-east-1");
        } else {
            panic!("Unexpected storage profile type");
        }
    }

    #[tokio::test]
    async fn test_warehouse_list() {
        let state = create_state().await;
        let wh1 = create_warehouse(state.clone(), Some(String::from("wh1"))).await;
        let wh2 = create_warehouse(state.clone(), Some(String::from("wh2"))).await;
        let result = SlateDbCatalog::list_warehouses(
            ProjectIdent::from(uuid::Uuid::nil()),
            None,
            None,
            state,
        )
            .await
            .expect("Failed to list warehouses");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, wh1);
        assert_eq!(result[0].name, "wh1");
        assert_eq!(result[1].id, wh2);
        assert_eq!(result[1].name, "wh2");
    }

    #[tokio::test]
    async fn test_warehouse_delete() {
        let state = create_state().await;
        let wh = create_warehouse(state.clone(), None).await;
        SlateDbCatalog::delete_warehouse(wh, state.clone()).await.expect("Failed to delete warehouse");

        let result = SlateDbCatalog::list_warehouses(
            ProjectIdent::from(uuid::Uuid::nil()),
            None,
            None,
            state,
        )
            .await
            .expect("Failed to list warehouses");
        assert_eq!(result.len(), 0);
    }
}