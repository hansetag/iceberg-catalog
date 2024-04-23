use super::{post, ApiContext, HeaderMap, Json, Path, Prefix, Result, Router, State};
use axum::async_trait;
use axum::response::IntoResponse;
use http::StatusCode;

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        prefix: Option<Prefix>,
        request: serde_json::Value,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;
}

pub fn router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables/{table}/metrics
        .route(
            "/:prefix/metrics",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<serde_json::Value>| async {
                    { I::report_metrics(Some(prefix), request, api_context, headers) }
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        .route(
            "/metrics",
            post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<serde_json::Value>| async {
                    { I::report_metrics(None, request, api_context, headers) }
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
