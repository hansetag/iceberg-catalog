use std::collections::{HashMap, HashSet};

use super::{
    namespace::{
        create_namespace, drop_namespace, get_namespace, list_namespaces, namespace_ident_to_id,
        update_namespace_properties,
    },
    tabular::table::{
        commit_table_transaction, create_table, drop_table, get_table_metadata_by_id,
        get_table_metadata_by_s3_location, list_tables, load_table, rename_table,
        table_ident_to_id, table_idents_to_ids,
    },
    warehouse::{
        create_warehouse, delete_warehouse, get_warehouse, list_projects, list_warehouses,
        rename_warehouse, set_warehouse_status, update_storage_profile,
    },
    CatalogState, PostgresTransaction,
};
use crate::implementations::postgres::tabular::view::view_ident_to_id;
use crate::service::{
    CommitTransactionRequest, CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest,
    GetWarehouseResponse, ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent, Result,
    TableIdent, UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
    WarehouseStatus,
};
use crate::{
    service::{
        storage::StorageProfile, Catalog, CommitTableResponseExt, CreateTableResponse,
        GetNamespaceResponse, GetTableMetadataResponse, LoadTableResponse, NamespaceIdentUuid,
        ProjectIdent, TableIdentUuid, Transaction, WarehouseIdent,
    },
    SecretIdent,
};

#[async_trait::async_trait]
impl Catalog for super::Catalog {
    type Transaction = PostgresTransaction;
    type State = CatalogState;

    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<WarehouseIdent> {
        create_warehouse(
            warehouse_name,
            project_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn get_warehouse<'a>(
        warehouse_id: &WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetWarehouseResponse> {
        get_warehouse(warehouse_id, transaction).await
    }

    async fn get_namespace<'a>(
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse> {
        get_namespace(warehouse_id, namespace, transaction).await
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
    ) -> Result<CreateTableResponse> {
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
    ) -> Result<LoadTableResponse> {
        load_table(warehouse_id, table, catalog_state).await
    }

    async fn get_table_metadata_by_id(
        warehouse_id: &WarehouseIdent,
        table: &TableIdentUuid,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
        get_table_metadata_by_id(warehouse_id, table, include_staged, catalog_state).await
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: &WarehouseIdent,
        location: &str,
        include_staged: bool,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
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

    // ---------------- Management API ----------------
    async fn list_projects(catalog_state: Self::State) -> Result<HashSet<ProjectIdent>> {
        list_projects(catalog_state).await
    }

    async fn list_warehouses(
        project_id: &ProjectIdent,
        include_inactive: Option<Vec<WarehouseStatus>>,
        warehouse_id_filter: Option<&HashSet<WarehouseIdent>>,
        catalog_state: Self::State,
    ) -> Result<Vec<GetWarehouseResponse>> {
        list_warehouses(
            project_id,
            include_inactive,
            warehouse_id_filter,
            catalog_state,
        )
        .await
    }

    async fn delete_warehouse<'a>(
        warehouse_id: &WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        delete_warehouse(warehouse_id, transaction).await
    }

    async fn rename_warehouse<'a>(
        warehouse_id: &WarehouseIdent,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_warehouse(warehouse_id, new_name, transaction).await
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: &WarehouseIdent,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        set_warehouse_status(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile<'a>(
        warehouse_id: &WarehouseIdent,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        update_storage_profile(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn view_ident_to_id(
        warehouse_id: &WarehouseIdent,
        view: &TableIdent,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        view_ident_to_id(warehouse_id, view, &catalog_state.read_pool).await
    }
}
