use crate::api::ApiContext;
use crate::api::Result;
use crate::request_metadata::RequestMetadata;
use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Extension, Router};
use iceberg_ext::catalog::rest::CatalogConfig;

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<CatalogConfig>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetConfigQueryParams {
    /// Warehouse location or identifier to request from the service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new().route(
        "/config",
        get(
            |Query(query): Query<GetConfigQueryParams>,
             State(api_context): State<ApiContext<S>>,
             Extension(metadata): Extension<RequestMetadata>| {
                I::get_config(query, api_context, metadata)
            },
        ),
    )
}
