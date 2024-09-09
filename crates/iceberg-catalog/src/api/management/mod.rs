pub mod v1 {
    pub mod warehouse;
    use axum::{Extension, Json, Router};
    use utoipa::OpenApi;

    use crate::api::{ApiContext, Result};
    use crate::request_metadata::RequestMetadata;
    use crate::service::auth::AuthZHandler;
    use std::marker::PhantomData;

    use crate::api::iceberg::v1::PaginationQuery;

    use crate::service::tabular_idents::TabularIdentUuid;
    use crate::service::{storage::S3Flavor, Catalog, SecretStore, State};
    use axum::extract::{Path, Query, State as AxumState};
    use axum::routing::{get, post};
    use serde::Serialize;
    use warehouse::{
        AzCredential, AzdlsProfile, CreateWarehouseRequest, CreateWarehouseResponse,
        GetWarehouseResponse, ListProjectsResponse, ListWarehousesRequest, ListWarehousesResponse,
        ProjectResponse, RenameWarehouseRequest, S3Credential, S3Profile, Service,
        StorageCredential, StorageProfile, UpdateWarehouseCredentialRequest,
        UpdateWarehouseStorageRequest, WarehouseStatus,
    };

    #[derive(Debug, OpenApi)]
    #[openapi(
        tags(
            (name = "management", description = "Warehouse management operations")
        ),
        paths(
            activate_warehouse,
            create_warehouse,
            deactivate_warehouse,
            delete_warehouse,
            get_warehouse,
            list_projects,
            list_warehouses,
            rename_warehouse,
            update_storage_credential,
            update_storage_profile,
            list_deleted_tabulars
        ),
        components(schemas(
            AzCredential,
            AzdlsProfile,
            CreateWarehouseRequest,
            CreateWarehouseResponse,
            GetWarehouseResponse,
            ListProjectsResponse,
            ListWarehousesRequest,
            ListWarehousesResponse,
            ProjectResponse,
            RenameWarehouseRequest,
            S3Credential,
            S3Profile,
            S3Flavor,
            StorageCredential,
            StorageProfile,
            UpdateWarehouseCredentialRequest,
            UpdateWarehouseStorageRequest,
            WarehouseStatus,
            ListDeletedTabularsResponse,
            DeletedTabularResponse,
            TabularType,
            DeleteKind,
        ))
    )]
    pub struct ManagementApiDoc;

    #[derive(Clone, Debug)]

    pub struct ApiServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
        auth_handler: PhantomData<A>,
        config_server: PhantomData<C>,
        secret_store: PhantomData<S>,
    }

    /// Create a new warehouse.
    ///
    /// Create a new warehouse in the given project. The project
    /// of a warehouse cannot be changed after creation.
    /// The storage configuration is validated by this method.
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse",
        request_body = CreateWarehouseRequest,
        responses(
            (status = 201, description = "Warehouse created successfully", body = [CreateWarehouseResponse]),
        )
    )]
    async fn create_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateWarehouseRequest>,
    ) -> Result<CreateWarehouseResponse> {
        ApiServer::<C, A, S>::create_warehouse(request, api_context, metadata).await
    }

    /// List all existing projects
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/project",
        responses(
            (status = 200, description = "List of projects", body = [ListProjectsResponse])
        )
    )]
    async fn list_projects<C: Catalog, A: AuthZHandler, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListProjectsResponse> {
        ApiServer::<C, A, S>::list_projects(api_context, metadata).await
    }

    /// List all warehouses in a project
    ///
    /// By default, this endpoint does not return deactivated warehouses.
    /// To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/warehouse",
        params(ListWarehousesRequest),
        responses(
            (status = 200, description = "List of warehouses", body = [ListWarehousesResponse])
        )
    )]
    async fn list_warehouses<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Query(request): Query<ListWarehousesRequest>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListWarehousesResponse> {
        ApiServer::<C, A, S>::list_warehouses(request, api_context, metadata).await
    }

    /// Get a warehouse by ID
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}",
        responses(
            (status = 200, description = "Warehouse details", body = [GetWarehouseResponse])
        )
    )]
    async fn get_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::get_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Delete a warehouse by ID
    #[utoipa::path(
        delete,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}",
        responses(
            (status = 200, description = "Warehouse deleted successfully")
        )
    )]
    async fn delete_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::delete_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Rename a warehouse
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/rename",
        request_body = RenameWarehouseRequest,
        responses(
            (status = 200, description = "Warehouse renamed successfully")
        )
    )]
    async fn rename_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameWarehouseRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::rename_warehouse(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// Deactivate a warehouse
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/deactivate",
        responses(
            (status = 200, description = "Warehouse deactivated successfully")
        )
    )]
    async fn deactivate_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::deactivate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Activate a warehouse
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/activate",
        responses(
            (status = 200, description = "Warehouse activated successfully")
        )
    )]
    async fn activate_warehouse<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::activate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Update the storage profile of a warehouse
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/storage",
        request_body = UpdateWarehouseStorageRequest,
        responses(
            (status = 200, description = "Storage profile updated successfully")
        )
    )]
    async fn update_storage_profile<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseStorageRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_storage(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// Update the storage credential of a warehouse
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/storage-credential",
        request_body = UpdateWarehouseCredentialRequest,
        responses(
            (status = 200, description = "Storage credential updated successfully")
        )
    )]
    async fn update_storage_credential<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseCredentialRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_credential(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// List soft-deleted tabulars
    ///
    /// List all soft-deleted tabulars in the warehouse that are visible to you.
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/deleted_tabulars",
        responses(
            (status = 200, description = "List of soft-deleted tabulars", body = [ListDeletedTabularsResponse])
        )
    )]
    async fn list_deleted_tabulars<C: Catalog, A: AuthZHandler, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        Query(pagination): Query<PaginationQuery>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<Json<ListDeletedTabularsResponse>> {
        ApiServer::<C, A, S>::list_soft_deleted_tabulars(
            metadata,
            warehouse_id.into(),
            api_context,
            pagination,
        )
        .await
        .map(Json)
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct ListDeletedTabularsResponse {
        /// List of tabulars
        pub tabulars: Vec<DeletedTabularResponse>,
        /// Token to fetch the next page
        pub next_page_token: Option<String>,
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct DeletedTabularResponse {
        /// Unique identifier of the tabular
        pub id: uuid::Uuid,
        /// Name of the tabular
        pub name: String,
        /// List of namespace parts the tabular belongs to
        pub namespace: Vec<String>,
        /// Type of the tabular
        pub typ: TabularType,
        /// Warehouse ID where the tabular is stored
        pub warehouse_id: uuid::Uuid,
        /// Date when the tabular was created
        pub created_at: chrono::DateTime<chrono::Utc>,
        /// Date when the tabular was deleted
        pub deleted_at: chrono::DateTime<chrono::Utc>,
        /// Date when the tabular will not be recoverable anymore
        pub expiration_date: chrono::DateTime<chrono::Utc>,
    }

    /// Type of tabular
    #[derive(Debug, Serialize, Clone, Copy, utoipa::ToSchema, strum::Display, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    pub enum TabularType {
        Table,
        View,
    }

    impl From<TabularIdentUuid> for TabularType {
        fn from(ident: TabularIdentUuid) -> Self {
            match ident {
                TabularIdentUuid::Table(_) => TabularType::Table,
                TabularIdentUuid::View(_) => TabularType::View,
            }
        }
    }

    #[derive(Debug, Serialize, utoipa::ToSchema, Clone, Copy, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DeleteKind {
        Default,
        Purge,
    }

    impl<C: Catalog, A: AuthZHandler, S: SecretStore> ApiServer<C, A, S> {
        pub fn new_v1_router() -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
                // Create a new warehouse
                .route("/warehouse", post(create_warehouse))
                // List all projects
                .route("/project", get(list_projects))
                .route(
                    "/warehouse",
                    // List all warehouses within a project
                    get(list_warehouses),
                )
                .route(
                    "/warehouse/:warehouse_id",
                    get(get_warehouse).delete(delete_warehouse),
                )
                // Rename warehouse
                .route("/warehouse/:warehouse_id/rename", post(rename_warehouse))
                // Deactivate warehouse
                .route(
                    "/warehouse/:warehouse_id/deactivate",
                    post(deactivate_warehouse),
                )
                .route(
                    "/warehouse/:warehouse_id/activate",
                    post(activate_warehouse),
                )
                // Update storage profile and credential.
                // The old credential is not re-used. If credentials are not provided,
                // we assume that this endpoint does not require a secret.
                .route(
                    "/warehouse/:warehouse_id/storage",
                    post(update_storage_profile),
                )
                // Update only the storage credential - keep the storage profile as is
                .route(
                    "/warehouse/:warehouse_id/storage-credential",
                    post(update_storage_credential),
                )
                .route(
                    "/warehouse/:warehouse_id/deleted_tabulars",
                    get(list_deleted_tabulars),
                )
        }
    }
}
