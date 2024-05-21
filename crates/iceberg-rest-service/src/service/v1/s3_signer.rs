use super::{post, ApiContext, HeaderMap, Json, Prefix, Result, Router, State};
use axum::{async_trait, extract::Path};
use iceberg_ext::catalog::rest::{S3SignRequest, S3SignResponse};

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + 'static,
{
    /// Sign an S3 request.
    /// Requests should be send to `/:prefix/namespace/:namespace/table/:table/v1/aws/s3/sign`,
    /// where :namespace and :table can be any string. Typically these strings would be
    /// ids of the namespace and table, respectively - not their names.
    /// For clients to use this route, the server implementation should specify "s3.signer.uri"
    /// accordingly on `load_table` and other methods that require data access.
    ///
    /// If a request is recieved at `/aws/s3/sign`, table and namespace will be `None`.
    async fn sign(
        prefix: Option<Prefix>,
        namespace: Option<String>,
        table: Option<String>,
        request: S3SignRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<S3SignResponse>;
}

pub fn router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/aws/s3/sign",
            post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<S3SignRequest>| {
                    {
                        I::sign(None, None, None, request, api_context, headers)
                    }
                },
            ),
        )
        .route(
            "/:prefix/namespace/:namespace/table/:table/aws/s3/sign",
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, String, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<S3SignRequest>| {
                    {
                        I::sign(
                            Some(prefix),
                            Some(namespace),
                            Some(table),
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            ),
        )
        .route(
            "/:prefix/namespace/:namespace/table/:table/v1/aws/s3/sign",
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, String, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<S3SignRequest>| {
                    {
                        I::sign(
                            Some(prefix),
                            Some(namespace),
                            Some(table),
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            ),
        )
}
