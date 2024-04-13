use super::{post, ApiContext, HeaderMap, Json, MetricsService, Path, Prefix, Router, State};
use axum::response::IntoResponse;
use http::StatusCode;

pub(crate) fn metrics_router<I: MetricsService<S>, S: crate::service::State>(
) -> Router<ApiContext<S>> {
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
