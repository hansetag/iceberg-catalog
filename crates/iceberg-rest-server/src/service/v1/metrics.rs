use super::*;
use axum::response::IntoResponse;
use http::StatusCode;

pub(crate) fn metrics_router<I: V1MetricsService<S>, S: crate::service::State>(
) -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables/{table}/metrics
        .route(
            "/metrics",
            post(
                |Path((prefix, namespace, table)): Path<(
                    Prefix,
                    NamespaceIdentUrl,
                    TableIdent,
                )>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<serde_json::Value>| async {
                    {
                        I::report_metrics(
                            TableParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                                table: table.into(),
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                    .await
                    .map(|_| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
