use anyhow::Context;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
use jwks_client_rs::source::WebSource;
use jwks_client_rs::{JsonWebKey, JwksClient};

use crate::request_metadata::RequestMetadata;
use axum::Extension;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum AuthDetails {
    JWT(Claims),
}

impl AuthDetails {
    #[must_use]
    pub fn user_id(&self) -> Option<Uuid> {
        match self {
            // TODO: sub is a string, we don't want to deal with that in our db, can we prescribe
            //       uuid in the jwt?
            Self::JWT(claims) => claims.sub.parse().ok(),
        }
    }
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::JWT(claims) => claims.name.as_deref(),
        }
    }

    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        match self {
            Self::JWT(claims) => claims.preferred_username.as_deref(),
        }
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        match self {
            Self::JWT(claims) => &claims.iss,
        }
    }

    #[must_use]
    pub fn email(&self) -> Option<&str> {
        match self {
            Self::JWT(claims) => claims.other.get("email").and_then(|v| v.as_str()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: Aud,
    pub exp: usize,
    pub iat: usize,
    pub name: Option<String>,
    pub preferred_username: Option<String>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Aud {
    String(String),
    Vec(Vec<String>),
}

pub(crate) async fn auth_middleware_fn(
    State(verifier): State<Verifier>,
    authorization: Option<TypedHeader<Authorization<Bearer>>>,
    Extension(mut metadata): Extension<RequestMetadata>,
    mut request: Request,
    next: Next,
) -> Response {
    tracing::debug!("Auth middleware");
    if let Some(authorization) = authorization {
        match verifier.decode::<Claims>(authorization.token()).await {
            Ok(val) => {
                tracing::debug!("Token verified: {:?}", val);
                metadata.auth_details = Some(AuthDetails::JWT(val));
                request.extensions_mut().insert(metadata);
            }
            Err(err) => {
                tracing::debug!("Failed to verify token: {:?}", err);
                return IcebergErrorResponse::from(err).into_response();
            }
        };
    } else {
        return IcebergErrorResponse::from(
            ErrorModel::builder()
                .message("Missing authorization header")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .build(),
        )
        .into_response();
    }

    next.run(request).await
}

#[derive(Clone)]
pub struct Verifier {
    client: JwksClient<WebSource>,
    issuer: String,
}

impl Verifier {
    const WELL_KNOWN_CONFIG: &'static str = ".well-known/openid-configuration";
    /// Create a new verifier with the given openid configuration url and audience.
    ///
    /// # Errors
    ///
    /// This function can fail if the openid configuration cannot be fetched or parsed.
    /// This function can also fail if the `WebSource` cannot be built from the jwks uri in the
    /// fetched openid configuration
    pub async fn new(mut url: Url) -> anyhow::Result<Self> {
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let config = Arc::new(
            reqwest::get(url.join(Self::WELL_KNOWN_CONFIG)?)
                .await
                .context("Failed to fetch openid configuration")?
                .json::<WellKnownConfig>()
                .await
                .context("Failed to parse openid configuration")?,
        );
        let source = WebSource::builder().build(config.jwks_uri.clone())?;
        let client = JwksClient::builder().build(source);
        Ok(Self {
            client,
            issuer: config.issuer.clone(),
        })
    }

    // this function is mostly lifted out of jwks_client_rs which is incompatible with azure jwks.
    async fn decode<O: DeserializeOwned>(&self, token: &str) -> Result<O, ErrorModel> {
        let header: Header = jsonwebtoken::decode_header(token).map_err(|e| {
            ErrorModel::builder()
                .message("Failed to decode auth token header.")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .source(Some(Box::new(e)))
                .build()
        })?;

        if let Some(kid) = header.kid.as_ref() {
            let key: JsonWebKey = self
                .client
                .get_opt(kid)
                .await
                .map_err(|e| Self::internal_error(e, "Failed to fetch key from jwks endpoint."))?
                .ok_or_else(|| {
                    ErrorModel::builder()
                        .message("Unknown kid")
                        .r#type("UnauthorizedError")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .build()
                })?;

            let validation = self.setup_validation(&header, &key)?;
            let decoding_key = Self::setup_decoding_key(key)?;

            return Ok(jsonwebtoken::decode(token, &decoding_key, &validation)
                .map_err(|e| {
                    tracing::debug!("Failed to decode token: {}", e);
                    ErrorModel::builder()
                        .message("Failed to decode token.")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .r#type("UnauthorizedError")
                        .source(Some(Box::new(e)))
                        .build()
                })?
                .claims);
        }

        Err(ErrorModel::builder()
            .message("Token header does not contain a key id.")
            .code(StatusCode::UNAUTHORIZED.into())
            .r#type("UnauthorizedError")
            .build())
    }

    fn setup_decoding_key(key: JsonWebKey) -> Result<DecodingKey, ErrorModel> {
        let decoding_key = match key {
            JsonWebKey::Rsa(jwk) => DecodingKey::from_rsa_components(jwk.modulus(), jwk.exponent())
                .map_err(|e| {
                    Self::internal_error(
                        e,
                        "Failed to create rsa decoding key from key components.",
                    )
                })?,
            JsonWebKey::Ec(jwk) => {
                DecodingKey::from_ec_components(jwk.x(), jwk.y()).map_err(|e| {
                    Self::internal_error(e, "Failed to create ec decoding key from key components.")
                })?
            }
        };
        Ok(decoding_key)
    }

    fn setup_validation(
        &self,
        header: &Header,
        key: &JsonWebKey,
    ) -> Result<Validation, ErrorModel> {
        let mut validation = if let Some(alg) = key.alg() {
            Validation::new(Algorithm::from_str(alg).map_err(|e| {
                Self::internal_error(
                    e,
                    "Failed to parse algorithm from key obtained from the jwks endpoint.",
                )
            })?)
        } else {
            // We need this fallback since e.g. azure's keys at
            // https://login.microsoftonline.com/common/discovery/keys don't have the alg field
            Validation::new(header.alg)
        };

        // TODO: aud validation in multi-tenant setups
        validation.validate_aud = false;

        validation.set_issuer(&[&self.issuer]);

        Ok(validation)
    }

    fn internal_error(
        e: impl std::error::Error + Sync + Send + 'static,
        message: &str,
    ) -> ErrorModel {
        ErrorModel::builder()
            .message(message)
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .r#type("InternalServerError")
            .source(Some(Box::new(e)))
            .build()
    }
}

impl Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish()
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct WellKnownConfig {
    #[serde(flatten)]
    pub other: serde_json::Value,
    pub jwks_uri: Url,
    pub issuer: String,
    pub userinfo_endpoint: Option<Url>,
    pub token_endpoint: Url,
}

#[cfg(test)]
mod test {
    use crate::service::token_verification::Claims;

    #[test]
    fn test_aud_with_array() {
        let _: Claims = serde_json::from_value(serde_json::json!({
            "sub": "1234567890",
            "iat": 22,
            "aud": ["aud1", "aud2"],
            "iss": "https://example.com",
            "exp": 9022
        }))
        .unwrap();
    }

    #[test]
    fn test_aud_with_string() {
        let _: Claims = serde_json::from_value(serde_json::json!({
            "sub": "1234567890",
            "iat": 22,
            "aud": "aud1",
            "iss": "https://example.com",
            "exp": 9022
        }))
        .unwrap();
    }
}
