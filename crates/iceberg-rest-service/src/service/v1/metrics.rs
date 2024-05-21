use super::namespace::NamespaceIdentUrl;
use super::{
    post, ApiContext, HeaderMap, Json, Path, Prefix, Result, Router, State, TableParameters,
};
use axum::async_trait;
use axum::response::IntoResponse;
use http::StatusCode;
use iceberg_ext::TableIdent;

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + 'static,
{
    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        parameters: TableParameters,
        request: serde_json::Value,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;
}

pub fn router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables/{table}/metrics
        .route(
            "/:prefix/namespaces/:namespace/tables/:table/metrics",
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<serde_json::Value>| async {
                    {
                        I::report_metrics(
                            TableParameters {
                                prefix: Some(prefix),
                                table: TableIdent {
                                    namespace: namespace.into(),
                                    name: table,
                                },
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        .route(
            "/namespaces/:namespace/tables/:table/metrics",
            post(
                |State(api_context): State<ApiContext<S>>,
                 Path((namespace, table)): Path<(NamespaceIdentUrl, String)>,
                 headers: HeaderMap,
                 Json(request): Json<serde_json::Value>| async {
                    {
                        I::report_metrics(
                            TableParameters {
                                prefix: None,
                                table: TableIdent {
                                    namespace: namespace.into(),
                                    name: table,
                                },
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
