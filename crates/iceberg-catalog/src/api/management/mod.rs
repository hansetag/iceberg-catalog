use crate::service::auth::AuthZHandler;
use crate::service::{Catalog, SecretStore};
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
    use axum::extract::State as AxumState;
    use axum::routing::post;
    use warehouse::WarehouseService;

    impl<C: Catalog, A: AuthZHandler, S: SecretStore> super::ApiServer<C, A, S> {
        pub fn new_v1_router() -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
                // List Namespaces
                .route(
                    "/warehouse",
                    // List Namespaces
                    post(
                        |AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                         Extension(metadata): Extension<RequestMetadata>,
                         Json(request): Json<warehouse::CreateWarehouseRequest>| {
                            Self::create_warehouse(request, api_context, metadata)
                        },
                    ),
                )
        }
    }
}
