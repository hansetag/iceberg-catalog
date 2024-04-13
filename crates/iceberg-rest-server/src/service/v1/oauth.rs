use super::{
    async_trait, post, ApiContext, Form, HeaderMap, OAuthTokenRequest, OAuthTokenResponse, Result,
    Router, State,
};

#[async_trait]
pub trait OAuthService<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    async fn get_token(
        state: ApiContext<S>,
        headers: HeaderMap,
        // application/x-www-form-urlencoded
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse>;
}

pub(crate) fn oauth_router<I: OAuthService<S>, S: crate::service::State>() -> Router<ApiContext<S>>
{
    Router::new().route(
        "/oauth/tokens",
        post(
            |State(api_context): State<ApiContext<S>>,
             headers: HeaderMap,
             // application/x-www-form-urlencoded
             Form(request): Form<OAuthTokenRequest>| {
                I::get_token(api_context, headers, request)
            },
        ),
    )
}
