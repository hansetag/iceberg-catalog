use anyhow::Context;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use http::header::AUTHORIZATION;
use http::{HeaderMap, StatusCode};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
use jwks_client_rs::source::WebSource;
use jwks_client_rs::JwksClientError::Error;
use jwks_client_rs::{JsonWebKey, JwksClient, JwksClientError};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use url::Url;

#[derive(Clone)]
pub struct Verifier {
    client: JwksClient<WebSource>,
    audience: Vec<String>,
}

impl Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish()
    }
}

#[derive(Deserialize, Debug)]
pub struct WellKnownConfig {
    #[serde(flatten)]
    other: serde_json::Value,
    jwks_uri: Url,
}

impl Verifier {
    const WELL_KNOWN_CONFIG: &'static str = ".well-known/openid-configuration";
    pub async fn new(url: Url, audience: Vec<String>) -> anyhow::Result<Self> {
        let config = dbg!(reqwest::get(url.join(Self::WELL_KNOWN_CONFIG)?)
            .await
            .context("Failed to fetch openid configuration")?
            .json::<WellKnownConfig>()
            .await
            .context("Failed to parse openid configuration")?);
        let source = WebSource::builder().build(config.jwks_uri)?;
        let client = JwksClient::builder().build(source);
        Ok(Self { client, audience })
    }

    // this function is mostly lifted out of jwks_client_rs which is incompatible with azure jwks.
    async fn decode<O: DeserializeOwned>(&self, token: &str) -> Result<O, ErrorModel> {
        let header: Header = jsonwebtoken::decode_header(token).map_err(|e| {
            ErrorModel::builder()
                .message("Failed to decode auth token header.")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

        fn internal_error(e: impl Display, message: &str) -> ErrorModel {
            ErrorModel::builder()
                .message(message)
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .r#type("InternalServerError")
                .stack(Some(vec![e.to_string()]))
                .build()
        }

        if let Some(kid) = header.kid.as_ref() {
            let key: JsonWebKey = self
                .client
                .get_opt(kid)
                .await
                .map_err(|e| internal_error(e, "Failed to fetch key from jwks endpoint."))?
                .ok_or_else(|| {
                    ErrorModel::builder()
                        .message("Unknown kid")
                        .r#type("UnauthorizedError")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .build()
                })?;

            let mut validation = if let Some(alg) = key.alg() {
                Validation::new(Algorithm::from_str(alg).map_err(|e| {
                    internal_error(
                        e,
                        "Failed to parse algorithm from key obtained from the jwks endpoint.",
                    )
                })?)
            } else {
                // We need this fallback since e.g. azure's keys at
                // https://login.microsoftonline.com/common/discovery/keys don't have the alg field
                Validation::new(header.alg)
            };

            if !self.audience.is_empty() {
                validation.set_audience(&self.audience);
            }

            let decoding_key = match key {
                JsonWebKey::Rsa(jwk) => DecodingKey::from_rsa_components(
                    jwk.modulus(),
                    jwk.exponent(),
                )
                .map_err(|e| {
                    internal_error(e, "Failed to create rsa decoding key from key components.")
                })?,
                JsonWebKey::Ec(jwk) => {
                    DecodingKey::from_ec_components(jwk.x(), jwk.y()).map_err(|e| {
                        internal_error(e, "Failed to create ec decoding key from key components.")
                    })?
                }
            };
            // Can this block the current thread? (should I spawn_blocking?)
            Ok(jsonwebtoken::decode(token, &decoding_key, &validation)
                .map_err(|e| {
                    tracing::debug!("Failed to decode token: {}", e);
                    ErrorModel::builder()
                        .message("Failed to decode token.")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .r#type("UnauthorizedError")
                        .stack(Some(vec![e.to_string()]))
                        .build()
                })?
                .claims)
        } else {
            Err(ErrorModel::builder()
                .message("Token header does not contain a key id.")
                .code(StatusCode::UNAUTHORIZED.into())
                .r#type("UnauthorizedError")
                .build())
        }
    }
}

pub(crate) async fn auth_middleware_fn(
    State(verifier): State<Option<Verifier>>,
    authorization: TypedHeader<Authorization<Bearer>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(verifier) = verifier {
        if let Err(err) = dbg!(
            verifier
                .decode::<serde_json::Value>(authorization.token())
                .await
        ) {
            tracing::debug!("Failed to verify token: {:?}", err);
            return IcebergErrorResponse::from(err).into_response();
        }
    }

    next.run(request).await
}
