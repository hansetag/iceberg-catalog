/// IcebergErrorResponse : JSON wrapper for all error responses (non-2xx)
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct IcebergErrorResponse {
    #[serde(rename = "error")]
    pub error: Box<ErrorModel>,
}

/// ErrorModel : JSON error payload returned in a response with further details on the error
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, validator::Validate)]
pub struct ErrorModel {
    /// Human-readable error message
    #[serde(rename = "message")]
    pub message: String,
    /// Internal type definition of the error
    #[serde(rename = "type")]
    pub r#type: String,
    /// HTTP response code
    #[validate(range(min = 400, max = 600))]
    #[serde(rename = "code")]
    pub code: i32,
    #[serde(rename = "stack", skip_serializing_if = "Option::is_none")]
    pub stack: Option<Vec<String>>,
}

impl ErrorModel {
    /// JSON error payload returned in a response with further details on the error
    pub fn new(
        message: String,
        r#type: String,
        code: i32,
        stack: Option<Vec<String>>,
    ) -> ErrorModel {
        ErrorModel {
            message,
            r#type,
            code,
            stack,
        }
    }
}
