//! Get `OpenFGA` clients

use std::str::FromStr;

use crate::service::authz::{ErrorModel, Result};
use openfga_rs::{
    authentication::{BearerTokenInterceptor, ClientCredentialInterceptor},
    tonic::{
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    },
};
use openfga_rs::{
    authentication::{ClientCredentials, RefreshConfiguration},
    open_fga_service_client::OpenFgaServiceClient,
};

/// Create a new `OpenFGA` client without authentication.
///
/// # Errors
/// - Connection to `OpenFGA` fails
pub async fn new_unauthenticated_client(
    endpoint: url::Url,
) -> Result<OpenFgaServiceClient<Channel>> {
    let client = OpenFgaServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|e| {
            ErrorModel::internal(
                format!("Failed to connect to OpenFGA at {endpoint}"),
                "OpenFGAConnection",
                Some(Box::new(e)),
            )
        })?;

    Ok(client)
}

/// Create a new `OpenFGA` client with bearer token.
///
/// # Errors
/// - Connection to `OpenFGA` fails
pub async fn new_bearer_auth_client(
    endpoint: url::Url,
    token: &str,
) -> Result<OpenFgaServiceClient<InterceptedService<Channel, BearerTokenInterceptor>>> {
    let channel = new_channel(endpoint).await?;
    let interceptor = BearerTokenInterceptor::new(token).map_err(|e| {
        ErrorModel::internal(
            format!("Failed to create BearerTokenInterceptor: {e}"),
            "OpenFGAConnection",
            Some(Box::new(e)),
        )
    })?;
    let client = OpenFgaServiceClient::with_interceptor(channel, interceptor);
    Ok(client)
}

/// Create a new `OpenFGA` client with client credentials.
///
/// # Errors
/// - Client credentials cannot be exchanged for a token
/// - Connection to `OpenFGA` fails
pub async fn new_client_credentials(
    endpoint: url::Url,
    credentials: ClientCredentials,
    refresh_config: RefreshConfiguration,
) -> Result<OpenFgaServiceClient<InterceptedService<Channel, ClientCredentialInterceptor>>> {
    let channel = new_channel(endpoint).await?;
    let interceptor = ClientCredentialInterceptor::new_initialized(credentials, refresh_config)
        .map_err(|e| {
            ErrorModel::internal(
                format!("Failed to create ClientCredentialInterceptor: {e}"),
                "OpenFGAConnection",
                Some(Box::new(e)),
            )
        })?;
    let client: OpenFgaServiceClient<InterceptedService<Channel, ClientCredentialInterceptor>> =
        OpenFgaServiceClient::with_interceptor(channel, interceptor);
    Ok(client)
}

async fn new_channel(endpoint: url::Url) -> Result<Channel> {
    let channel = Endpoint::from_str(endpoint.as_ref())
        .map_err(|e| {
            ErrorModel::internal(
                format!("Invalid OpenFGA endpoint: {endpoint}"),
                "OpenFGAConnection",
                Some(Box::new(e)),
            )
        })?
        .connect()
        .await
        .map_err(|e| {
            ErrorModel::internal(
                format!("Failed to connect to OpenFGA at {endpoint}"),
                "OpenFGAConnection",
                Some(Box::new(e)),
            )
        })?;

    Ok(channel)
}
