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
    #[builder(setter(into))]
    pub message: String,
    /// Internal type definition of the error
    #[builder(setter(into))]
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub stack: Option<Vec<String>>,
}

impl ErrorModel {
    pub fn push_to_stack(&mut self, message: impl Into<String>) -> &mut Self {
        if let Some(stack) = &mut self.stack {
            stack.push(message.into());
        } else {
            self.stack = Some(vec![message.into()]);
        }

        self
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for IcebergErrorResponse {
    fn into_response(mut self) -> axum::http::Response<axum::body::Body> {
        // ToDo: Better Log handling. kv-log-macro?
        let error_id = uuid::Uuid::now_v7();

        let console_log = serde_json::json!(
            {
                "error_id": error_id.to_string(),
                "message": self.error.message,
                "type": self.error.r#type,
                "code": self.error.code,
                "stack": self.error.stack
            }
        );
        let code = self.error.code;

        // Exchange stack for error_id. We don't want the stack
        // to be exposed to the client
        self.error.stack = Some(vec![format!("Error ID: {}", error_id)]);

        let mut response = axum::Json(self).into_response();

        log::info!("{}", console_log.to_string());

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
        assert_eq!(serde_json::to_value(resp).unwrap(), json);
    }
}
