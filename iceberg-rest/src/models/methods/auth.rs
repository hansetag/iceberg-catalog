use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "grant_type")]
pub enum OAuthTokenRequest {
    #[serde(rename = "urn:ietf:params:oauth:grant-type:token-exchange")]
    OAuthTokenExchangeRequest(OAuthTokenExchangeRequest),
    #[serde(rename = "client_credentials")]
    OAuthClientCredentialsRequest(OAuthClientCredentialsRequest),
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthTokenExchangeRequest {
    pub scope: Option<String>,
    #[validate(nested)]
    pub requested_token_type: Option<TokenType>,
    #[validate(length(min = 1))]
    pub subject_token: String,
    #[validate(nested)]
    pub subject_token_type: TokenType,
    #[validate(length(min = 1))]
    pub actor_token: Option<String>,
    #[validate(nested)]
    pub actor_token_type: Option<TokenType>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthClientCredentialsRequest {
    pub scope: Option<String>,
    #[validate(length(min = 1))]
    pub client_id: String,
    #[validate(length(min = 1))]
    pub client_secret: String,
}

/// TokenType : Token type identifier, from RFC 8693 Section 3  See https://datatracker.ietf.org/doc/html/rfc8693#section-3
/// Token type identifier, from RFC 8693 Section 3  See https://datatracker.ietf.org/doc/html/rfc8693#section-3
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum TokenType {
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

impl ToString for TokenType {
    fn to_string(&self) -> String {
        match self {
            Self::AccessToken => String::from("urn:ietf:params:oauth:token-type:access_token"),
            Self::RefreshToken => String::from("urn:ietf:params:oauth:token-type:refresh_token"),
            Self::IdToken => String::from("urn:ietf:params:oauth:token-type:id_token"),
            Self::Saml1 => String::from("urn:ietf:params:oauth:token-type:saml1"),
            Self::Saml2 => String::from("urn:ietf:params:oauth:token-type:saml2"),
            Self::Jwt => String::from("urn:ietf:params:oauth:token-type:jwt"),
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthError {
    #[serde(rename = "error")]
    pub error: OAuthErrorType,
    #[serde(rename = "error_description", skip_serializing_if = "Option::is_none")]
    pub error_description: Option<String>,
    #[serde(rename = "error_uri", skip_serializing_if = "Option::is_none")]
    pub error_uri: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum OAuthErrorType {
    #[serde(rename = "invalid_request")]
    InvalidRequest,
    #[serde(rename = "invalid_client")]
    InvalidClient,
    #[serde(rename = "invalid_grant")]
    InvalidGrant,
    #[serde(rename = "unauthorized_client")]
    UnauthorizedClient,
    #[serde(rename = "unsupported_grant_type")]
    UnsupportedGrantType,
    #[serde(rename = "invalid_scope")]
    InvalidScope,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct OAuthTokenResponse {
    /// The access token, for client credentials or token exchange
    #[serde(rename = "access_token")]
    pub access_token: String,
    /// Access token type for client credentials or token exchange  See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1
    #[serde(rename = "token_type")]
    pub token_type: OAuthAccessTokenType,
    /// Lifetime of the access token in seconds for client credentials or token exchange
    #[serde(rename = "expires_in", skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<i32>,
    #[validate(nested)]
    #[serde(rename = "issued_token_type", skip_serializing_if = "Option::is_none")]
    pub issued_token_type: Option<TokenType>,
    /// Refresh token for client credentials or token exchange
    #[serde(rename = "refresh_token", skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Authorization scope for client credentials or token exchange
    #[serde(rename = "scope", skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// Access token type for client credentials or token exchange  See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum OAuthAccessTokenType {
    #[serde(rename = "bearer")]
    Bearer,
    #[serde(rename = "mac")]
    Mac,
    #[serde(rename = "N_A")]
    NA,
}

impl Validate for TokenType {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

impl Validate for OAuthTokenRequest {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            OAuthTokenRequest::OAuthTokenExchangeRequest(x) => x.validate(),
            OAuthTokenRequest::OAuthClientCredentialsRequest(x) => x.validate(),
        }
    }
}

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
