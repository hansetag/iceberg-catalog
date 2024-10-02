pub mod v1 {
    pub mod warehouse;
    use axum::{Extension, Json, Router};
    use utoipa::OpenApi;

    use crate::api::{ApiContext, Result};
    use crate::request_metadata::RequestMetadata;
    use crate::service::authz::Authorizer;
    use std::marker::PhantomData;

    use crate::api::iceberg::v1::PaginationQuery;

    use crate::service::token_verification::UserId;
    use crate::service::TabularIdentUuid;
    use crate::service::{storage::S3Flavor, Catalog, SecretStore, State};
    use axum::extract::{Path, Query, State as AxumState};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{delete, get, post};
    use serde::{Deserialize, Serialize};
    use warehouse::{
        AzCredential, AzdlsProfile, CreateWarehouseRequest, CreateWarehouseResponse,
        GetWarehouseResponse, ListProjectsResponse, ListWarehousesRequest, ListWarehousesResponse,
        ProjectResponse, RenameWarehouseRequest, S3Credential, S3Profile, Service,
        StorageCredential, StorageProfile, TabularDeleteProfile, UpdateWarehouseCredentialRequest,
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
            TabularDeleteProfile,
        ))
    )]
    pub struct ManagementApiDoc;

    #[derive(Clone, Debug)]

    pub struct ApiServer<C: Catalog, A: Authorizer, S: SecretStore> {
        auth_handler: PhantomData<A>,
        config_server: PhantomData<C>,
        secret_store: PhantomData<S>,
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    #[serde(rename_all = "kebab-case")]
    pub enum UserOrigin {
        ImplicitViaConfigCall,
        ExplicitViaRegisterCall,
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct User {
        pub name: String,
        pub user_origin: UserOrigin,
        pub email: Option<String>,
        pub id: String,
        pub created_at: chrono::DateTime<chrono::Utc>,
        pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    impl IntoResponse for User {
        fn into_response(self) -> axum::response::Response {
            (http::StatusCode::CREATED, Json(self)).into_response()
        }
    }

    /// Register a new user
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/user",
        responses(
            (status = 201, description = "User successfully registered", body = [User]),
        )
    )]
    async fn register_user<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<User> {
        ApiServer::<C, A, S>::register_user(api_context, metadata).await
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct ListUsersResponse {
        pub users: Vec<User>,
        pub next_page_token: Option<String>,
    }

    impl IntoResponse for ListUsersResponse {
        fn into_response(self) -> axum::response::Response {
            (http::StatusCode::OK, Json(self)).into_response()
        }
    }

    #[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
    pub struct ListUsersQuery {
        pub name: Option<String>,
        #[serde(default)]
        pub include_deleted: bool,
    }

    /// List Registered Users
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/user",
        params(
            ListUsersQuery
        ),
        responses(
            (status = 200, description = "List of users", body = [ListUsersResponse]),
        )
    )]
    async fn list_users<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Query(ListUsersQuery {
            name,
            include_deleted,
        }): Query<ListUsersQuery>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListUsersResponse> {
        ApiServer::<C, A, S>::list_users(api_context, metadata, include_deleted, name.as_deref())
            .await
    }

    #[derive(Debug, Deserialize, utoipa::ToSchema)]
    pub struct UpdateUserRequest {
        pub name: String,
        pub email: Option<String>,
    }

    /// Update user details
    #[utoipa::path(
        put,
        tag = "management",
        path = "/management/v1/user/{issuer}/{id}",
        responses(
            (status = 200, description = "User details updated successfully", body = [User]),
        )
    )]
    async fn update_user<C: Catalog, A: Authorizer, S: SecretStore>(
        Path((issuer, id)): Path<(String, String)>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateUserRequest>,
    ) -> Response {
        let id = UserId::from_parts(issuer.as_str(), id.as_str());
        match ApiServer::<C, A, S>::update_user(id, request, api_context, metadata).await {
            Ok(user) => (http::StatusCode::OK, Json(user)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    /// Delete user
    ///
    /// This endpoint is used to delete a user. The user will be soft-deleted and can be recovered
    /// Deleting a user will only remove it from this service. The user will still be able to register
    /// again as long as the user exists in the identity provider.
    #[utoipa::path(
        delete,
        tag = "management",
        path = "/management/v1/user/{issuer}/{id}",
        responses(
            (status = 200, description = "User deleted successfully"),
        )
    )]
    async fn delete_user<C: Catalog, A: Authorizer, S: SecretStore>(
        Path((issuer, id)): Path<(String, String)>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Response {
        let id = UserId::from_parts(issuer.as_str(), id.as_str());
        (
            http::StatusCode::OK,
            Json(ApiServer::<C, A, S>::delete_user(api_context, metadata, id).await),
        )
            .into_response()
    }

    #[derive(Debug, Deserialize, utoipa::ToSchema)]
    pub struct CreateRoleRequest {
        pub id: String,
        pub name: String,
        pub description: Option<String>,
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct Role {
        pub id: String,
        pub name: String,
        pub description: Option<String>,
        pub created_at: chrono::DateTime<chrono::Utc>,
        pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    impl IntoResponse for Role {
        fn into_response(self) -> Response {
            (http::StatusCode::CREATED, Json(self)).into_response()
        }
    }

    /// Create a new role
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/role",
        request_body = CreateRoleRequest,
        responses(
            (status = 201, description = "Role successfully created", body = [Role]),
        )
    )]
    async fn create_role<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateRoleRequest>,
    ) -> Result<Role> {
        ApiServer::<C, A, S>::create_role(request, api_context, metadata).await
    }

    #[derive(Debug, Serialize, utoipa::ToSchema)]
    pub struct ListRolesResponse {
        pub roles: Vec<Role>,
        pub next_page_token: Option<String>,
    }

    impl IntoResponse for ListRolesResponse {
        fn into_response(self) -> Response {
            (http::StatusCode::OK, Json(self)).into_response()
        }
    }

    /// List existing roles
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/role",
        responses(
            (status = 200, description = "List of roles", body = [ListRolesResponse]),
        )
    )]
    async fn list_roles<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListRolesResponse> {
        ApiServer::<C, A, S>::list_roles(api_context, metadata).await
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
    async fn create_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn list_projects<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn list_warehouses<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn get_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn delete_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn rename_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn deactivate_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
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
    async fn activate_warehouse<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::activate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Update the storage profile of a warehouse including its storage credential.
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/storage",
        request_body = UpdateWarehouseStorageRequest,
        responses(
            (status = 200, description = "Storage profile updated successfully")
        )
    )]
    async fn update_storage_profile<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseStorageRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_storage(warehouse_id.into(), request, api_context, metadata)
            .await
    }

    /// Update the storage credential of a warehouse. The storage profile is not modified.
    /// This can be used to update credentials before expiration.
    #[utoipa::path(
        post,
        tag = "management",
        path = "/management/v1/warehouse/{warehouse_id}/storage-credential",
        request_body = UpdateWarehouseCredentialRequest,
        responses(
            (status = 200, description = "Storage credential updated successfully")
        )
    )]
    async fn update_storage_credential<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateWarehouseCredentialRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_storage_credential(
            warehouse_id.into(),
            request,
            api_context,
            metadata,
        )
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
    async fn list_deleted_tabulars<C: Catalog, A: Authorizer, S: SecretStore>(
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

    impl<C: Catalog, A: Authorizer, S: SecretStore> ApiServer<C, A, S> {
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
                .route("/user", post(register_user).get(list_users))
                .route("user/:issuer/:id", delete(delete_user).put(update_user))
                .route("/role", post(create_role).get(list_roles))
        }
    }
}
