use std::marker::PhantomData;

use axum::{extract::State as AxumState, routing::post, Json, Router};
use http::HeaderMap;
use iceberg_rest_service::ApiContext;

use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State};

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ApiServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

pub mod v1 {
    use super::{
        post, ApiContext, AuthZHandler, AxumState, Catalog, HeaderMap, Json, Router, SecretStore,
        State,
    };
    pub mod warehouse;
    use warehouse::WarehouseService;

    impl<C: Catalog, A: AuthZHandler, S: SecretStore> super::ApiServer<C, A, S> {
        pub fn v1_router() -> Router<ApiContext<State<A, C, S>>> {
            Router::new()
            // List Namespaces
            .route(
                "/warehouse",
                // List Namespaces
                post(
                    |AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                     headers: HeaderMap,
                     Json(request): Json<warehouse::CreateWarehouseRequest>| {
                        Self::create_warehouse(request, api_context, headers)
                    },
                ),
            )
        }
    }
}
