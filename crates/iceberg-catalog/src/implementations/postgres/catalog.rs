use super::{
    namespace::{
        create_namespace, drop_namespace, get_namespace, list_namespaces, namespace_ident_to_id,
        update_namespace_properties,
    },
    tabular::table::{
        commit_table_transaction, create_table, drop_table, get_table_metadata_by_id,
        get_table_metadata_by_s3_location, list_tables, load_tables, rename_table,
        table_ident_to_id, table_idents_to_ids,
    },
    warehouse::{
        create_warehouse, delete_warehouse, get_warehouse, list_projects, list_warehouses,
        rename_warehouse, set_warehouse_status, update_storage_profile,
    },
    CatalogState, PostgresTransaction,
};
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::implementations::postgres::tabular::view::{
    create_view, drop_view, list_views, load_view, rename_view, view_ident_to_id,
};
use crate::implementations::postgres::tabular::{list_tabulars, mark_tabular_as_deleted};
use crate::service::tabular_idents::{TabularIdentOwned, TabularIdentUuid};
use crate::service::{
    CreateNamespaceRequest, CreateNamespaceResponse, DeletionDetails, GetWarehouseResponse,
    ListFlags, ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent, Result, TableCreation,
    TableIdent, WarehouseStatus,
};
use crate::{
    api::iceberg::v1::{PaginatedTabulars, PaginationQuery},
    service::TableCommit,
};
use crate::{
    service::{
        storage::StorageProfile, Catalog, CreateTableResponse, GetNamespaceResponse,
        GetTableMetadataResponse, LoadTableResponse, NamespaceIdentUuid, ProjectIdent,
        TableIdentUuid, Transaction, WarehouseIdent,
    },
    SecretIdent,
};
use iceberg::spec::ViewMetadata;
use iceberg_ext::configs::Location;
use std::collections::{HashMap, HashSet};

#[async_trait::async_trait]
impl Catalog for super::PostgresCatalog {
    type Transaction = PostgresTransaction;
    type State = CatalogState;

    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: ProjectIdent,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretIdent>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<WarehouseIdent> {
        create_warehouse(
            warehouse_name,
            project_id,
            storage_profile,
            tabular_delete_profile,
            storage_secret_id,
            transaction,
        )
        .await
    }

    async fn get_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetWarehouseResponse> {
        get_warehouse(warehouse_id, transaction).await
    }

    async fn get_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<GetNamespaceResponse> {
        get_namespace(warehouse_id, namespace, transaction).await
    }

    async fn list_namespaces(
        warehouse_id: WarehouseIdent,
        query: &ListNamespacesQuery,
        catalog_state: CatalogState,
    ) -> Result<ListNamespacesResponse> {
        list_namespaces(warehouse_id, query, catalog_state).await
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateNamespaceResponse> {
        create_namespace(warehouse_id, namespace_id, request, transaction).await
    }

    async fn namespace_ident_to_id(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        catalog_state: CatalogState,
    ) -> Result<Option<NamespaceIdentUuid>> {
        namespace_ident_to_id(warehouse_id, namespace, catalog_state).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        drop_namespace(warehouse_id, namespace, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        update_namespace_properties(warehouse_id, namespace, properties, transaction).await
    }

    async fn create_table<'a>(
        table_creation: TableCreation<'_>,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<CreateTableResponse> {
        create_table(table_creation, transaction).await
    }

    async fn list_tables(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        list_flags: crate::service::ListFlags,
        catalog_state: CatalogState,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TableIdentUuid, TableIdent>> {
        list_tables(
            warehouse_id,
            namespace,
            list_flags,
            catalog_state,
            pagination_query,
        )
        .await
    }

    // Should also load staged tables but not tables of inactive warehouses
    async fn load_tables<'a>(
        warehouse_id: WarehouseIdent,
        tables: impl IntoIterator<Item = TableIdentUuid> + Send,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
        load_tables(warehouse_id, tables, include_deleted, transaction).await
    }

    async fn get_table_metadata_by_id(
        warehouse_id: WarehouseIdent,
        table: TableIdentUuid,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
        get_table_metadata_by_id(warehouse_id, table, list_flags, catalog_state).await
    }

    async fn get_table_metadata_by_s3_location(
        warehouse_id: WarehouseIdent,
        location: &Location,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<GetTableMetadataResponse> {
        get_table_metadata_by_s3_location(warehouse_id, location, list_flags, catalog_state).await
    }

    async fn table_ident_to_id(
        warehouse_id: WarehouseIdent,
        table: &TableIdent,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        table_ident_to_id(warehouse_id, table, list_flags, &catalog_state.read_pool()).await
    }

    async fn rename_table<'a>(
        warehouse_id: WarehouseIdent,
        source_id: TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_table(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_table<'a>(
        table_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_table(table_id, transaction).await
    }

    async fn table_idents_to_ids(
        warehouse_id: WarehouseIdent,
        tables: HashSet<&TableIdent>,
        list_flags: crate::service::ListFlags,
        catalog_state: Self::State,
    ) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>> {
        table_idents_to_ids(warehouse_id, tables, list_flags, &catalog_state.read_pool()).await
    }

    async fn commit_table_transaction<'a>(
        warehouse_id: WarehouseIdent,
        commits: impl IntoIterator<Item = TableCommit> + Send,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        commit_table_transaction(warehouse_id, commits, transaction).await
    }

    // ---------------- Management API ----------------
    async fn list_projects(catalog_state: Self::State) -> Result<HashSet<ProjectIdent>> {
        list_projects(catalog_state).await
    }

    async fn list_warehouses(
        project_id: ProjectIdent,
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
        warehouse_id: WarehouseIdent,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        delete_warehouse(warehouse_id, transaction).await
    }

    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseIdent,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        rename_warehouse(warehouse_id, new_name, transaction).await
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseIdent,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<CatalogState>>::Transaction<'a>,
    ) -> Result<()> {
        set_warehouse_status(warehouse_id, status, transaction).await
    }

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseIdent,
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

    async fn create_view<'a>(
        namespace_id: NamespaceIdentUuid,
        view: &TableIdent,
        request: ViewMetadata,
        metadata_location: &'_ Location,
        location: &'_ Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<()> {
        create_view(
            namespace_id,
            metadata_location,
            transaction,
            view.name.as_str(),
            request,
            location,
        )
        .await
    }

    async fn view_ident_to_id(
        warehouse_id: WarehouseIdent,
        view: &TableIdent,
        catalog_state: Self::State,
    ) -> Result<Option<TableIdentUuid>> {
        view_ident_to_id(warehouse_id, view, false, &catalog_state.read_pool()).await
    }

    async fn load_view<'a>(
        view_id: TableIdentUuid,
        include_deleted: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<crate::implementations::postgres::tabular::view::ViewMetadataWithLocation> {
        load_view(view_id, include_deleted, &mut *transaction).await
    }

    async fn list_views(
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        include_deleted: bool,
        catalog_state: Self::State,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TableIdentUuid, TableIdent>> {
        list_views(
            warehouse_id,
            namespace,
            include_deleted,
            catalog_state,
            pagination_query,
        )
        .await
    }

    async fn update_view_metadata(
        namespace_id: NamespaceIdentUuid,
        view_id: TableIdentUuid,
        view: &TableIdent,
        metadata_location: &Location,
        metadata: ViewMetadata,
        location: &Location,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        drop_view(view_id, transaction).await?;
        create_view(
            namespace_id,
            metadata_location,
            transaction,
            &view.name,
            metadata,
            location,
        )
        .await
    }

    async fn rename_view(
        warehouse_id: WarehouseIdent,
        source_id: TableIdentUuid,
        source: &TableIdent,
        destination: &TableIdent,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        rename_view(warehouse_id, source_id, source, destination, transaction).await
    }

    async fn drop_view<'a>(
        table_id: TableIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<String> {
        drop_view(table_id, transaction).await
    }

    async fn list_tabulars(
        warehouse_id: WarehouseIdent,
        list_flags: ListFlags,
        catalog_state: CatalogState,
        pagination_query: PaginationQuery,
    ) -> Result<PaginatedTabulars<TabularIdentUuid, (TabularIdentOwned, Option<DeletionDetails>)>>
    {
        list_tabulars(
            warehouse_id,
            None,
            list_flags,
            &catalog_state.read_pool(),
            None,
            pagination_query,
        )
        .await
    }

    async fn mark_tabular_as_deleted(
        table_id: TabularIdentUuid,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<()> {
        mark_tabular_as_deleted(table_id, transaction).await
    }
}
