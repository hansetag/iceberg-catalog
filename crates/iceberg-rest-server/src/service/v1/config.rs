use super::*;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetConfigQueryParams {
    /// Warehouse location or identifier to request from the service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

pub(crate) fn config_router<I: V1RestServer<S>, S: crate::service::State>() -> Router<ApiContext<S>>
{
    Router::new().route("/config", get(I::get_config))
}
