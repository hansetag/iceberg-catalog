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

use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct JWT<O>(pub O)
where
    O: Clone;

impl<O> Debug for JWT<O>
where
    O: Clone,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JWT").finish()
    }
}

impl<O> Deref for JWT<O>
where
    O: Clone,
{
    type Target = O;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) async fn auth_middleware_fn<O: DeserializeOwned + Clone + Sync + Send + 'static>(
    State(verifier): State<Option<Arc<Verifier>>>,
    authorization: TypedHeader<Authorization<Bearer>>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(verifier) = verifier {
        match verifier.decode::<O>(authorization.token()).await {
            Ok(val) => {
                request.extensions_mut().insert(JWT(val));
            }
            Err(err) => {
                tracing::debug!("Failed to verify token: {:?}", err);
                return IcebergErrorResponse::from(err).into_response();
            }
        }
    }

    next.run(request).await
}

#[derive(Clone)]
pub struct Verifier {
    client: JwksClient<WebSource>,
    audience: Option<String>,
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
    pub async fn new(url: Url, audience: Option<&str>) -> anyhow::Result<Self> {
        let config = reqwest::get(url.join(Self::WELL_KNOWN_CONFIG)?)
            .await
            .context("Failed to fetch openid configuration")?
            .json::<WellKnownConfig>()
            .await
            .context("Failed to parse openid configuration")?;
        let source = WebSource::builder().build(config.jwks_uri)?;
        let client = JwksClient::builder().build(source);
        Ok(Self {
            client,
            audience: audience.map(ToOwned::to_owned),
        })
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

            // Can this block the current thread? (should I spawn_blocking?)
            return Ok(jsonwebtoken::decode(token, &decoding_key, &validation)
                .map_err(|e| {
                    tracing::debug!("Failed to decode token: {}", e);
                    ErrorModel::builder()
                        .message("Failed to decode token.")
                        .code(StatusCode::UNAUTHORIZED.into())
                        .r#type("UnauthorizedError")
                        .stack(Some(vec![e.to_string()]))
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

        if let Some(audience) = self.audience.as_ref() {
            validation.set_audience(&[audience]);
            validation.validate_aud = true;
        } else {
            validation.validate_aud = false;
        }
        Ok(validation)
    }

    fn internal_error(e: impl Display, message: &str) -> ErrorModel {
        ErrorModel::builder()
            .message(message)
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .r#type("InternalServerError")
            .stack(Some(vec![e.to_string()]))
            .build()
    }
}

impl Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish()
    }
}

#[derive(Deserialize, Debug)]
pub struct WellKnownConfig {
    #[serde(flatten)]
    pub other: serde_json::Value,
    pub jwks_uri: Url,
}
