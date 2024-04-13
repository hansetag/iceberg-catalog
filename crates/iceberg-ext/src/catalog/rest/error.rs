// macro to implement IntoResponse

pub use iceberg::Error;

#[cfg(feature = "axum")]
macro_rules! impl_into_response {
    ($type:ty) => {
        impl axum::response::IntoResponse for $type {
            fn into_response(self) -> axum::http::Response<axum::body::Body> {
                axum::Json(self).into_response()
            }
        }
    };
    () => {};
}

#[cfg(feature = "axum")]
pub(crate) use impl_into_response;
use typed_builder::TypedBuilder;

/// JSON wrapper for all error responses (non-2xx)
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct IcebergErrorResponse {
    pub error: ErrorModel,
}

impl From<IcebergErrorResponse> for iceberg::Error {
    fn from(resp: IcebergErrorResponse) -> iceberg::Error {
        resp.error.into()
    }
}

impl From<ErrorModel> for iceberg::Error {
    fn from(value: ErrorModel) -> Self {
        let mut error = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, value.message)
            .with_context("type", value.r#type)
            .with_context("code", format!("{}", value.code));

        if let Some(stack) = value.stack {
            error = error.with_context("stack", stack.join("\n"));
        }

        error
    }
}

impl From<ErrorModel> for IcebergErrorResponse {
    fn from(value: ErrorModel) -> Self {
        IcebergErrorResponse { error: value }
    }
}

/// JSON error payload returned in a response with further details on the error
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct ErrorModel {
    /// Human-readable error message
    pub message: String,
    /// Internal type definition of the error
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub stack: Option<Vec<String>>,
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for IcebergErrorResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let code = self.error.code;
        let mut response = axum::Json(self).into_response();
        *response.status_mut() = axum::http::StatusCode::from_u16(code)
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_error_response_serialization() {
        let json = serde_json::json!({
        "error": {
            "message": "The server does not support this operation",
            "type": "UnsupportedOperationException",
            "code": 406}
        });

        let resp: IcebergErrorResponse = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(&resp).unwrap(), json);
    }
}
