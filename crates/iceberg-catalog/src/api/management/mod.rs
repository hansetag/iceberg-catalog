use crate::service::{auth::AuthZHandler, Catalog, SecretStore};
use std::marker::PhantomData;
#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ApiServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

pub mod v1 {
    use axum::{Extension, Json, Router};

    pub mod warehouse;
    use crate::api::ApiContext;
    use crate::request_metadata::RequestMetadata;
    use crate::service::auth::AuthZHandler;

    use crate::service::{Catalog, SecretStore, State};
    use axum::extract::{Path, State as AxumState, Query};
    use axum::routing::{get, post, put};
    use warehouse::Service;

    impl<C: Catalog, A: AuthZHandler, S: SecretStore> super::ApiServer<C, A, S> {
        pub fn new_v1_router() -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
                // Create a new warehouse
                .route(
                    "/warehouse",
                    post(
                        |AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>,
                         Json(request): Json<warehouse::CreateWarehouseRequest>| {
                            Self::create_warehouse(request, api_context, metadata)
                        },
                    ),
                )
                // List all projects
                .route(
                    "/project",
                    get(
                        |AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>| {
                            Self::list_projects(api_context, metadata)
                        },
                    ),
                )
                
                .route(
                    "/project/:project_id/warehouses",
                    // List all warehouses within a project
                    get(
                        |Path(project_id): Path<uuid::Uuid>,
                        Query(request): Query<warehouse::ListWarehouseRequest>,
                         AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>| {
                            Self::list_warehouses(project_id.into(), request, api_context, metadata)
                        },
                    )
                )
                // Delete warehouse - must be empty
                .route("/warehouse/:warehouse_id", 
                get(
                    |Path(warehouse_id): Path<uuid::Uuid>,
                     AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                     Extension(metadata): Extension<RequestMetadata>| {
                        Self::get_warehouse(warehouse_id.into(), api_context, metadata)
                    },
                )
                .delete(
                    |Path(warehouse_id): Path<uuid::Uuid>,
                     AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                     Extension(metadata): Extension<RequestMetadata>| {
                        Self::delete_warehouse(warehouse_id.into(), api_context, metadata)
                    },
                ))
                // Rename warehouse
                .route(
                    "/warehouse/:warehouse_id/rename",
                    post(
                        |
                        Path(warehouse_id): Path<uuid::Uuid>,
                        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>,
                         Json(request): Json<warehouse::RenameWarehouseRequest>| {
                            Self::rename_warehouse(warehouse_id.into(), request, api_context, metadata)
                        },
                    ),
                )
                // Deactivate warehouse
                .route(
                    "/warehouse/:warehouse_id/deactivate",
                    post(
                        |
                        Path(warehouse_id): Path<uuid::Uuid>,
                        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>| {
                            Self::deactivate_warehouse(warehouse_id.into(), api_context, metadata)
                        },
                    ),
                )
                .route(
                    "/warehouse/:warehouse_id/activate",
                    post(
                        |
                        Path(warehouse_id): Path<uuid::Uuid>,
                        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>| {
                            Self::activate_warehouse(warehouse_id.into(), api_context, metadata)
                        },
                    ),
                )
                // Update storage profile and credential.
                // The old credential is not re-used. If credentials are not provided,
                // we assume that this endpoint does not require a secret.
                .route(
                    "/warehouse/:warehouse_id/storage",
                    post(
                        |
                        Path(warehouse_id): Path<uuid::Uuid>,
                        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>,
                         Json(request): Json<warehouse::UpdateWarehouseStorageRequest>| {
                            Self::update_storage(warehouse_id.into(), request, api_context, metadata)
                        },
                    ),
                )
                // Update only the storage credential - keep the storage profile as is
                .route(
                    "/warehouse/:warehouse_id/storage-credential",
                    post(
                        |
                        Path(warehouse_id): Path<uuid::Uuid>,
                        AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>,
                         Json(request): Json<warehouse::UpdateWarehouseCredentialRequest>| {
                            Self::update_credential(warehouse_id.into(), request, api_context, metadata)
                        },
                    ),
                )
        }
    }
}
