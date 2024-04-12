use super::*;

#[async_trait]
pub trait V1ConfigService<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<CatalogConfig>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetConfigQueryParams {
    /// Warehouse location or identifier to request from the service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

pub(crate) fn config_router<I: V1ConfigService<S>, S: crate::service::State>(
) -> Router<ApiContext<S>> {
    Router::new().route(
        "/config",
        get(
            |Query(query): Query<GetConfigQueryParams>,
             State(api_context): State<ApiContext<S>>,
             headers: HeaderMap| I::get_config(query, api_context, headers),
        ),
    )
}
