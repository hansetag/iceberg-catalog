use super::impl_into_response;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "grant_type")]
pub enum OAuthTokenRequest {
    #[serde(rename = "urn:ietf:params:oauth:grant-type:token-exchange")]
    OAuthTokenExchangeRequest(OAuthTokenExchangeRequest),
    #[serde(rename = "client_credentials")]
    OAuthClientCredentialsRequest(OAuthClientCredentialsRequest),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthTokenExchangeRequest {
    pub scope: Option<String>,
    pub requested_token_type: Option<OAuthTokenType>,
    pub subject_token: String,
    pub subject_token_type: OAuthTokenType,
    pub actor_token: Option<String>,
    pub actor_token_type: Option<OAuthTokenType>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthClientCredentialsRequest {
    pub scope: Option<String>,
    pub client_id: String,
    pub client_secret: String,
}

/// Token type identifier, from [RFC 8693 Section 3](https://datatracker.ietf.org/doc/html/rfc8693#section-3).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum OAuthTokenType {
    #[serde(rename = "urn:ietf:params:oauth:token-type:access_token")]
    AccessToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:refresh_token")]
    RefreshToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:id_token")]
    IdToken,
    #[serde(rename = "urn:ietf:params:oauth:token-type:saml1")]
    Saml1,
    #[serde(rename = "urn:ietf:params:oauth:token-type:saml2")]
    Saml2,
    #[serde(rename = "urn:ietf:params:oauth:token-type:jwt")]
    Jwt,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthError {
    pub error: OAuthErrorType,
    pub error_description: Option<String>,
    pub error_uri: Option<String>,
}

impl From<OAuthError> for iceberg::Error {
    fn from(value: OAuthError) -> Self {
        let mut error = iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("OAuthError: {}", value.error),
        );

        if let Some(desc) = value.error_description {
            error = error.with_context("description", desc);
        }

        if let Some(uri) = value.error_uri {
            error = error.with_context("uri", uri);
        }

        error
    }
}

#[derive(
    Clone,
    strum_macros::Display,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OAuthErrorType {
    InvalidRequest,
    InvalidClient,
    InvalidGrant,
    UnauthorizedClient,
    UnsupportedGrantType,
    InvalidScope,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthTokenResponse {
    /// The access token, for client credentials or token exchange
    pub access_token: String,
    /// Access token type for client credentials or token exchange.
    /// See [rfc6749](https://datatracker.ietf.org/doc/html/rfc6749#section-7.1)
    pub token_type: OAuthAccessTokenType,
    /// Lifetime of the access token in seconds for client credentials or token exchange
    pub expires_in: Option<u64>,
    pub issued_token_type: Option<OAuthTokenType>,
    /// Refresh token for client credentials or token exchange
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Authorization scope for client credentials or token exchange
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// Access token type for client credentials or token exchange.
/// See [rfc6749](https://datatracker.ietf.org/doc/html/rfc6749#section-7.1)
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum OAuthAccessTokenType {
    #[serde(rename = "bearer")]
    Bearer,
    #[serde(rename = "mac")]
    Mac,
    #[serde(rename = "N_A")]
    NA,
}

#[cfg(feature = "axum")]
impl_into_response! {OAuthTokenResponse}

#[cfg(feature = "axum")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_serialization() {
        let j = serde_json::json!(
            {
                "error": "invalid_request",
                "error_description": "Invalid request",
                "error_uri": "https://example.com/error"
            }
        );
        let e: OAuthError = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(e.error, OAuthErrorType::InvalidRequest);
        assert_eq!(e.error_description, Some("Invalid request".into()));
        assert_eq!(e.error_uri, Some("https://example.com/error".into()));
        assert_eq!(serde_json::to_value(e).unwrap(), j);
    }
}
