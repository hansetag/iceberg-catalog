mod config;
mod oauth;

pub use config::*;
pub use oauth::*;

use crate::service::*;
use axum::extract::{Form, Query, State};
pub use axum::routing::{get, post, put};
use http::HeaderMap;

#[axum::async_trait]
pub trait V1RestServer<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    async fn get_config(
        Query(query): Query<GetConfigQueryParams>,
        State(api_context): State<ApiContext<S>>,
        headers: HeaderMap,
    ) -> Result<CatalogConfig>;

    async fn get_token(
        State(api_context): State<ApiContext<S>>,
        headers: HeaderMap,
        Form(request): Form<OAuthTokenRequest>,
    ) -> Result<OAuthTokenResponse>;
}
