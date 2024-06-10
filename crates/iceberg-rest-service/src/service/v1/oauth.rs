use super::{
    async_trait, post, ApiContext, Form, OAuthTokenRequest, OAuthTokenResponse, Result, Router,
    State,
};
use crate::RequestMetadata;
use axum::Extension;

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + 'static,
{
    async fn get_token(
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
        // application/x-www-form-urlencoded
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse>;
}

pub fn router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new().route(
        "/oauth/tokens",
        post(
            |State(api_context): State<ApiContext<S>>,
             Extension(metadata): Extension<RequestMetadata>,
             // application/x-www-form-urlencoded
             Form(request): Form<OAuthTokenRequest>| {
                I::get_token(api_context, metadata, request)
            },
        ),
    )
}
