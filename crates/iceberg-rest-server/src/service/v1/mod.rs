mod config;
use crate::service::*;
use axum::extract::{Query, State};
pub use axum::routing::{get, post, put};
pub use config::*;

#[axum::async_trait]
pub trait V1RestServer<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    async fn get_config(
        Query(query): Query<GetConfigQueryParams>,
        State(_api_context): State<ApiContext<S>>,
    ) -> Result<CatalogConfig>;
}
