use axum::middleware::Next;
use axum::response::Response;
use http::HeaderMap;
use iceberg_rest_service::RequestMetadata;
use std::str::FromStr;
use uuid::Uuid;

pub(crate) async fn set_request_metadata(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = headers
        .get("x-request-id")
        .and_then(|hv| {
            hv.to_str()
                .map(Uuid::from_str)
                .ok()
                .transpose()
                .ok()
                .flatten()
        })
        .unwrap_or(Uuid::now_v7());
    request
        .extensions_mut()
        .insert(RequestMetadata { request_id });
    next.run(request).await
}
