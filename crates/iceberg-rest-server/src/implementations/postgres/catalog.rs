
use std::collections::{HashMap, HashSet};

use super::{
    namespace::{
        create_namespace, drop_namespace, get_namespace_metadata, get_storage_config,
        list_namespaces, namespace_ident_to_id, update_namespace_properties,
    },
    table::{
        commit_table_transaction, create_table, drop_table, get_table_metadata_by_id,
        get_table_metadata_by_s3_location, list_tables, load_table, rename_table,
        table_ident_to_id, table_idents_to_ids,
    },
    warehouse::create_warehouse_profile,
    CatalogState, PostgresTransaction,
};
use crate::implementations::postgres::warehouse::{
    deactivate_warehouse, get_warehouse, get_warehouse_status, reactivate_warehouse,
    update_warehouse_name, update_warehouse_storage_profile, update_warehouse_storage_secret_id,
};
use crate::service::GetWarehouseResponse;
use crate::{
    service::{
        storage::StorageProfile, Catalog, CommitTableResponseExt, CreateTableResult,
        GetStorageConfigResult, GetTableMetadataResult, LoadTableResult, NamespaceIdentUuid,
        ProjectIdent, TableIdentUuid, Transaction, WarehouseIdent,
    },
    SecretIdent, WarehouseStatus,
};
use iceberg_rest_service::{
    v1::{
        CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesQuery,
        ListNamespacesResponse, NamespaceIdent, Result, TableIdent,
        UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
    },
    CommitTransactionRequest, CreateTableRequest,
};

#[async_trait::async_trait]
impl Catalog for super::Catalog {
    type Transaction = PostgresTransaction;
    type State = CatalogState;

    async fn create_warehouse_profile<'a>(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<WarehouseIdent> {
        create_warehouse_profile(
            warehouse_name,
            project_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn get_storage_config(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: CatalogState,
    ) -> Result<GetStorageConfigResult> {
        get_storage_config(warehouse_id, namespace, catalog_state).await
    }

    async fn get_warehouse(
        warehouse_id: &WarehouseIdent,
        catalog_state: CatalogState,
    ) -> Result<GetWarehouseResponse> {
        get_warehouse(warehouse_id, catalog_state).await
    }

    async fn is_warehouse_available(
        warehouse_id: &WarehouseIdent,
        catalog_state: CatalogState,
    ) -> Result<bool> {
        match get_warehouse_status(warehouse_id, catalog_state).await? {
            WarehouseStatus::Active => Ok(true),
            WarehouseStatus::Inactive => Ok(false),
        }
    }

    async fn get_namespace_metadata<'a>(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse> {
        get_namespace_metadata(warehouse_id, namespace, transaction).await
    }

    async fn list_namespaces(
        warehouse_id: &WarehouseIdent,
        query: &ListNamespacesQuery,
        catalog_state: CatalogState,
    ) -> Result<ListNamespacesResponse> {
        list_namespaces(warehouse_id, query, catalog_state).await
    }

    async fn create_namespace<'a>(
        warehouse_id: &WarehouseIdent,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse> {
        create_namespace(warehouse_id, request, transaction).await
    }

    async fn namespace_ident_to_id(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: CatalogState,
    ) -> Result<Option<NamespaceIdentUuid>> {
        namespace_ident_to_id(warehouse_id, namespace, catalog_state).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        drop_namespace(warehouse_id, namespace, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        request: UpdateNamespacePropertiesRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        update_namespace_properties(warehouse_id, namespace, request, transaction).await
    }

    async fn create_table<'a>(
        namespace_id: &NamespaceIdentUuid,
        table: &TableIdent,
        table_id: &TableIdentUuid,
        request: CreateTableRequest,
        // Metadata location may be none if stage-create is true
        metadata_location: Option<&String>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateTableResult> {
        create_table(
            namespace_id,
            table,
            table_id,
            request,
            metadata_location,
            transaction,
        )
        .await
    }

    async fn list_tables(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        include_staged: bool,
        catalog_state: CatalogState,
    ) -> Result<HashMap<TableIdentUuid, TableIdent>> {
        list_tables(warehouse_id, namespace, include_staged, catalog_state).await
    }

    async fn load_table(
        warehouse_id: &WarehouseIdent,
        table: &TableIdent,
        catalog_state: CatalogState,
    ) -> Result<LoadTableResult> {
        load_table(warehouse_id, table, catalog_state).await
    }

    async fn get_table_metadata_by_id(
        warehouse_id: &WarehouseIdent,
        table: &TableIdentUuid,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResult> {
        get_table_metadata_by_id(warehouse_id, table, include_staged, catalog_state).await
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: &WarehouseIdent,
        location: &str,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResult> {
        get_table_metadata_by_s3_location(warehouse_id, location, include_staged, catalog_state)
            .await
    }

    async fn table_ident_to_id(
        warehouse_id: &WarehouseIdent,
        table: &TableIdent,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        table_ident_to_id(
            warehouse_id,
            table,
            include_staged,
            &catalog_state.read_pool,
        )
        .await
    }

    async fn rename_table<'a>(
        warehouse_id: &WarehouseIdent,
        source_id: &TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_table(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_table<'a>(
        warehouse_id: &WarehouseIdent,
        table_id: &TableIdentUuid,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        drop_table(warehouse_id, table_id, transaction).await
    }

    async fn table_idents_to_ids(
        warehouse_id: &WarehouseIdent,
        tables: HashSet<&TableIdent>,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>> {
        table_idents_to_ids(
            warehouse_id,
            tables,
            include_staged,
            &catalog_state.read_pool,
        )
        .await
    }

    async fn commit_table_transaction<'a>(
        warehouse_id: &WarehouseIdent,
        request: CommitTransactionRequest,
        table_ids: &HashMap<TableIdent, TableIdentUuid>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<Vec<CommitTableResponseExt>> {
        commit_table_transaction(warehouse_id, request, table_ids, transaction).await
    }

    async fn deactivate_warehouse(
        warehouse_id: &WarehouseIdent,
        catalog_state: Self::State,
    ) -> Result<()> {
        deactivate_warehouse(warehouse_id, catalog_state).await
    }

    async fn reactivate_warehouse(
        warehouse_id: &WarehouseIdent,
        catalog_state: Self::State,
    ) -> Result<()> {
        reactivate_warehouse(warehouse_id, catalog_state).await
    }

    // Should delete warehouse record. Only if it marked as `inactive`.
    async fn delete_warehouse(_: &WarehouseIdent, _: Self::State) -> Result<()> {
        unimplemented!()
    }

    async fn update_warehouse_name<'a>(
        warehouse_id: &WarehouseIdent,
        warehouse_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        update_warehouse_name(warehouse_id, warehouse_name, transaction).await
    }

    async fn update_warehouse_storage_profile<'a>(
        warehouse_id: &WarehouseIdent,
        storage_profile: StorageProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        update_warehouse_storage_profile(warehouse_id, storage_profile, transaction).await
    }

    async fn update_warehouse_storage_secret_id<'a>(
        warehouse_id: &WarehouseIdent,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        update_warehouse_storage_secret_id(warehouse_id, storage_secret_id, transaction).await
    }
}
