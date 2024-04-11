use super::*;

pub(crate) fn oauth_router<I: V1RestServer<S>, S: crate::service::State>() -> Router<ApiContext<S>>
{
    Router::new().route("/oauth/tokens", post(I::get_token))
}
