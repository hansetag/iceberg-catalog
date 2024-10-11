pub mod v1 {
    pub mod project;
    pub mod role;
    pub mod user;
    pub mod warehouse;
    use axum::{Extension, Json, Router};
    use utoipa::OpenApi;

    use crate::api::{ApiContext, Result};
    use crate::request_metadata::RequestMetadata;
    use std::marker::PhantomData;

    use crate::api::iceberg::v1::PaginationQuery;

    use crate::api::management::v1::user::{ListUsersQuery, ListUsersResponse};
    use crate::service::{
        authz::Authorizer, storage::S3Flavor, Catalog, RoleId, SecretStore, State,
        TabularIdentUuid, UserId,
    };
    use axum::extract::{Path, Query, State as AxumState};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use http::StatusCode;
    use project::{
        CreateProjectRequest, CreateProjectResponse, GetProjectResponse, ListProjectsResponse,
        RenameProjectRequest, Service as _,
    };
    use role::{
        CreateRoleRequest, ListRolesQuery, ListRolesResponse, Role, Service as _, UpdateRoleRequest,
    };
    use serde::Serialize;
    use user::{
        SearchUser, SearchUserRequest, SearchUserResponse, Service as _, UpdateUserRequest, User,
        UserLastUpdatedWith,
    };
    use warehouse::{
        AzCredential, AzdlsProfile, CreateWarehouseRequest, CreateWarehouseResponse, GcsCredential,
        GcsProfile, GcsServiceKey, GetWarehouseResponse, ListWarehousesRequest,
        ListWarehousesResponse, RenameWarehouseRequest, S3Credential, S3Profile, Service as _,
        StorageCredential, StorageProfile, TabularDeleteProfile, UpdateWarehouseCredentialRequest,
        UpdateWarehouseStorageRequest, WarehouseStatus,
    };

    #[derive(Debug, OpenApi)]
    #[openapi(
        tags(
            (name = "project", description = "Manage Projects"),
            (name = "warehouse", description = "Manage Warehouses"),
            (name = "user", description = "Manage Users")
        ),
        paths(
            activate_warehouse,
            create_project,
            create_warehouse,
            deactivate_warehouse,
            delete_project,
            delete_warehouse,
            get_project,
            get_warehouse,
            list_deleted_tabulars,
            list_projects,
            list_warehouses,
            rename_project,
            rename_warehouse,
            update_storage_credential,
            update_storage_profile,
            create_or_update_user_from_token,
            search_user,
            get_user,
            update_user,
            list_user,
            delete_user
        ),
        components(schemas(
            AzCredential,
            AzdlsProfile,
            CreateProjectRequest,
            CreateProjectResponse,
            CreateRoleRequest,
            CreateRoleRequest,
            CreateWarehouseRequest,
            CreateWarehouseResponse,
            DeleteKind,
            DeletedTabularResponse,
            GcsCredential,
            GcsProfile,
            GcsServiceKey,
            GetProjectResponse,
            GetWarehouseResponse,
            ListDeletedTabularsResponse,
            ListProjectsResponse,
            ListRolesResponse,
            ListRolesResponse,
            ListUsersQuery,
            ListUsersResponse,
            ListWarehousesRequest,
            ListWarehousesResponse,
            RenameProjectRequest,
            RenameWarehouseRequest,
            Role,
            S3Credential,
            S3Flavor,
            S3Profile,
            SearchUser,
            SearchUserRequest,
            SearchUserResponse,
            StorageCredential,
            StorageProfile,
            TabularDeleteProfile,
            TabularType,
            UpdateUserRequest,
            UpdateWarehouseCredentialRequest,
            UpdateWarehouseStorageRequest,
            User,
            UserLastUpdatedWith,
            WarehouseStatus,
        ))
    )]
    pub struct ManagementApiDoc;

    #[derive(Clone, Debug)]
    pub struct ApiServer<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
        auth_handler: PhantomData<A>,
        config_server: PhantomData<C>,
        secret_store: PhantomData<S>,
    }

    /// Creates the user in the catalog if it does not exist.
    /// If the user exists, it updates the users' metadata from the token.
    /// The token sent to this endpoint should have "profile" and "email" scopes.
    #[utoipa::path(
        post,
        tag = "user",
        path = "/management/v1/user/from-token",
        responses(
            (status = 200, description = "User updated", body = [User]),
            (status = 201, description = "User created", body = [User]),
        )
    )]
    async fn create_or_update_user_from_token<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<User>)> {
        ApiServer::<C, A, S>::create_or_update_user_from_token(api_context, metadata)
            .await
            .map(|r| {
                if r.created {
                    (StatusCode::CREATED, Json(r.user))
                } else {
                    (StatusCode::OK, Json(r.user))
                }
            })
    }

    /// Search for users (Fuzzy)
    #[utoipa::path(
        post,
        tag = "user",
        path = "/management/v1/search/user",
        request_body = SearchUserRequest,
        responses(
            (status = 200, description = "List of users", body = [SearchUserResponse]),
        )
    )]
    async fn search_user<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<SearchUserRequest>,
    ) -> Result<SearchUserResponse> {
        ApiServer::<C, A, S>::search_user(api_context, metadata, request).await
    }

    /// Get a user by ID
    #[utoipa::path(
        get,
        tag = "user",
        path = "/management/v1/user/{id}",
        responses(
            (status = 200, description = "User details", body = [User]),
        )
    )]
    async fn get_user<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<User>)> {
        ApiServer::<C, A, S>::get_user(api_context, metadata, id)
            .await
            .map(|user| (StatusCode::OK, Json(user)))
    }

    /// Update details of a user. Replaces the current details with the new details.
    /// If a field is not provided, it is set to `None`.
    #[utoipa::path(
        put,
        tag = "user",
        path = "/management/v1/user/{id}",
        request_body = UpdateUserRequest,
        responses(
            (status = 200, description = "User details updated successfully"),
        )
    )]
    async fn update_user<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateUserRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::update_user(api_context, metadata, id, request).await
    }

    /// List users
    #[utoipa::path(
        get,
        tag = "user",
        path = "/management/v1/user",
        params(ListUsersQuery),
        responses(
            (status = 200, description = "List of users", body = [ListUsersResponse]),
        )
    )]
    async fn list_user<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Query(query): Query<ListUsersQuery>,
    ) -> Result<ListUsersResponse> {
        ApiServer::<C, A, S>::list_user(api_context, metadata, query).await
    }

    /// Delete user
    ///
    /// All permissions of the user are permanently removed and need to be re-added
    /// if the user is re-registered.
    #[utoipa::path(
        delete,
        tag = "user",
        path = "/management/v1/user/{id}",
        responses(
            (status = 200, description = "User deleted successfully"),
        )
    )]
    async fn delete_user<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<UserId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Response {
        (
            StatusCode::OK,
            Json(ApiServer::<C, A, S>::delete_user(api_context, metadata, id).await),
        )
            .into_response()
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
    ) -> Response {
        match ApiServer::<C, A, S>::create_role(request, api_context, metadata).await {
            Ok(role) => (StatusCode::CREATED, Json(role)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    /// List roles in a project
    #[utoipa::path(
        get,
        tag = "management",
        path = "/management/v1/role",
        params(ListRolesQuery),
        responses(
            (status = 200, description = "List of roles", body = [ListRolesResponse]),
        )
    )]
    async fn list_roles<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Query(query): Query<ListRolesQuery>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListRolesResponse> {
        ApiServer::<C, A, S>::list_roles(api_context, query, metadata).await
    }

    /// Delete role
    ///
    /// All permissions of the role are permanently removed.
    #[utoipa::path(
        delete,
        tag = "user",
        path = "/management/v1/role/{id}",
        responses(
            (status = 200, description = "Role deleted successfully"),
        )
    )]
    async fn delete_role<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, ())> {
        ApiServer::<C, A, S>::delete_role(api_context, metadata, id)
            .await
            .map(|()| (StatusCode::OK, ()))
    }

    /// Get a role
    #[utoipa::path(
        get,
        tag = "user",
        path = "/management/v1/role/{id}",
        responses(
            (status = 200, description = "Role details", body = [Role]),
        )
    )]
    async fn get_role<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<(StatusCode, Json<Role>)> {
        ApiServer::<C, A, S>::get_role(api_context, metadata, id)
            .await
            .map(|role| (StatusCode::OK, Json(role)))
    }

    /// Update a role
    #[utoipa::path(
        post,
        tag = "user",
        path = "/management/v1/role/{id}",
        request_body = UpdateRoleRequest,
        responses(
            (status = 200, description = "Role updated successfully", body = [Role]),
        )
    )]
    async fn update_role<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(id): Path<RoleId>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<UpdateRoleRequest>,
    ) -> Result<(StatusCode, Json<Role>)> {
        ApiServer::<C, A, S>::update_role(api_context, metadata, id, request)
            .await
            .map(|role| (StatusCode::OK, Json(role)))
    }

    /// Create a new warehouse.
    ///
    /// Create a new warehouse in the given project. The project
    /// of a warehouse cannot be changed after creation.
    /// The storage configuration is validated by this method.
    #[utoipa::path(
        post,
        tag = "warehouse",
        path = "/management/v1/warehouse",
        request_body = CreateWarehouseRequest,
        responses(
            (status = 201, description = "Warehouse created successfully", body = [CreateWarehouseResponse]),
        )
    )]
    async fn create_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateWarehouseRequest>,
    ) -> Result<CreateWarehouseResponse> {
        ApiServer::<C, A, S>::create_warehouse(request, api_context, metadata).await
    }

    /// List all projects the requesting user has access to
    #[utoipa::path(
        get,
        tag = "warehouse",
        path = "/management/v1/project",
        responses(
            (status = 200, description = "List of projects", body = [ListProjectsResponse])
        )
    )]
    async fn list_projects<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListProjectsResponse> {
        ApiServer::<C, A, S>::list_projects(api_context, metadata).await
    }

    /// Create a new project
    #[utoipa::path(
        post,
        tag = "project",
        path = "/management/v1/project",
        responses(
            (status = 201, description = "Project created successfully", body = [CreateProjectResponse])
        )
    )]
    async fn create_project<C: Catalog, A: Authorizer, S: SecretStore>(
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<CreateProjectRequest>,
    ) -> Result<CreateProjectResponse> {
        ApiServer::<C, A, S>::create_project(request, api_context, metadata).await
    }

    /// Get a Project by ID
    #[utoipa::path(
        get,
        tag = "project",
        path = "/management/v1/project/{project_id}",
        responses(
            (status = 200, description = "Project details", body = [GetProjectResponse])
        )
    )]
    async fn get_project<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetProjectResponse> {
        ApiServer::<C, A, S>::get_project(project_id, api_context, metadata).await
    }

    /// Delete a project by ID
    ///
    /// No warehouses must be present in the project to delete it.
    #[utoipa::path(
        delete,
        tag = "project",
        path = "/management/v1/project/{project_id}",
        responses(
            (status = 200, description = "Project deleted successfully")
        )
    )]
    async fn delete_project<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::delete_project(project_id, api_context, metadata).await
    }

    /// Rename a project
    #[utoipa::path(
        post,
        tag = "project",
        path = "/management/v1/project/{project_id}/rename",
        responses(
            (status = 200, description = "Project renamed successfully")
        )
    )]
    async fn rename_project<C: Catalog, A: Authorizer, S: SecretStore>(
        Path(project_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
        Json(request): Json<RenameProjectRequest>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::rename_project(project_id.into(), request, api_context, metadata)
            .await
    }

    /// List all warehouses in a project
    ///
    /// By default, this endpoint does not return deactivated warehouses.
    /// To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
    #[utoipa::path(
        get,
        tag = "warehouse",
        path = "/management/v1/warehouse",
        params(ListWarehousesRequest),
        responses(
            (status = 200, description = "List of warehouses", body = [ListWarehousesResponse])
        )
    )]
    async fn list_warehouses<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        Query(request): Query<ListWarehousesRequest>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<ListWarehousesResponse> {
        ApiServer::<C, A, S>::list_warehouses(request, api_context, metadata).await
    }

    /// Get a warehouse by ID
    #[utoipa::path(
        get,
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}",
        responses(
            (status = 200, description = "Warehouse details", body = [GetWarehouseResponse])
        )
    )]
    async fn get_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<GetWarehouseResponse> {
        ApiServer::<C, A, S>::get_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Delete a warehouse by ID
    #[utoipa::path(
        delete,
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}",
        responses(
            (status = 200, description = "Warehouse deleted successfully")
        )
    )]
    async fn delete_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::delete_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Rename a warehouse
    #[utoipa::path(
        post,
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/rename",
        request_body = RenameWarehouseRequest,
        responses(
            (status = 200, description = "Warehouse renamed successfully")
        )
    )]
    async fn rename_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
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
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/deactivate",
        responses(
            (status = 200, description = "Warehouse deactivated successfully")
        )
    )]
    async fn deactivate_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::deactivate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Activate a warehouse
    #[utoipa::path(
        post,
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/activate",
        responses(
            (status = 200, description = "Warehouse activated successfully")
        )
    )]
    async fn activate_warehouse<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
        Path(warehouse_id): Path<uuid::Uuid>,
        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
        Extension(metadata): Extension<RequestMetadata>,
    ) -> Result<()> {
        ApiServer::<C, A, S>::activate_warehouse(warehouse_id.into(), api_context, metadata).await
    }

    /// Update the storage profile of a warehouse including its storage credential.
    #[utoipa::path(
        post,
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/storage",
        request_body = UpdateWarehouseStorageRequest,
        responses(
            (status = 200, description = "Storage profile updated successfully")
        )
    )]
    async fn update_storage_profile<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
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
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/storage-credential",
        request_body = UpdateWarehouseCredentialRequest,
        responses(
            (status = 200, description = "Storage credential updated successfully")
        )
    )]
    async fn update_storage_credential<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
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
        tag = "warehouse",
        path = "/management/v1/warehouse/{warehouse_id}/deleted_tabulars",
        params(PaginationQuery),
        responses(
            (status = 200, description = "List of soft-deleted tabulars", body = [ListDeletedTabularsResponse])
        )
    )]
    async fn list_deleted_tabulars<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
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

    impl From<TabularIdentUuid> for TabularType {
        fn from(ident: TabularIdentUuid) -> Self {
            match ident {
                TabularIdentUuid::Table(_) => TabularType::Table,
                TabularIdentUuid::View(_) => TabularType::View,
            }
        }
    }

    /// Type of tabular
    #[derive(Debug, Serialize, Clone, Copy, utoipa::ToSchema, strum::Display, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    pub enum TabularType {
        Table,
        View,
    }

    #[derive(Debug, Serialize, utoipa::ToSchema, Clone, Copy, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DeleteKind {
        Default,
        Purge,
    }

    impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> ApiServer<C, A, S> {
        pub fn new_v1_router() -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
                // Role management
                .route("/role", post(create_role))
                .route("/role", get(list_roles))
                .route(
                    "/role/:id",
                    get(get_role).post(update_role).delete(delete_role),
                )
                // User management
                .route("/user/from-token", post(create_or_update_user_from_token))
                .route("/search/user", post(search_user))
                .route(
                    "/user/:user_id",
                    get(get_user).put(update_user).delete(delete_user),
                )
                .route("/user", get(list_user))
                // Create a new project
                .route("/project", post(create_project))
                .route(
                    "/project/:project_id",
                    get(get_project).delete(delete_project),
                )
                .route("/project/:project_id/rename", post(rename_project))
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
