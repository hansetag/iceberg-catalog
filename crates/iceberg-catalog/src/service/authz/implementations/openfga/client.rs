//! Get `OpenFGA` clients

use super::{
    ClientHelper as _, ModelVersion, OpenFGAAuthorizer, OpenFGAError, OpenFGAResult, AUTH_CONFIG,
};
use crate::service::authz::implementations::openfga::migration::get_auth_model_id;
use crate::{service::authz::implementations::Authorizers, OpenFGAAuth};
use http::HeaderMap;
use openfga_rs::{
    authentication::{BearerTokenInterceptor, ClientCredentialInterceptor},
    tonic::{
        self,
        codegen::{Body, Bytes, StdError},
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    },
};
use openfga_rs::{
    authentication::{ClientCredentials, RefreshConfiguration},
    open_fga_service_client::OpenFgaServiceClient,
};
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::RwLock;

pub type UnauthenticatedOpenFGAAuthorizer = OpenFGAAuthorizer<Channel>;
pub type BearerOpenFGAAuthorizer =
    OpenFGAAuthorizer<InterceptedService<Channel, BearerTokenInterceptor>>;
pub type ClientCredentialsOpenFGAAuthorizer =
    OpenFGAAuthorizer<InterceptedService<Channel, ClientCredentialInterceptor>>;

pub(crate) enum Clients {
    Unauthenticated(OpenFgaServiceClient<Channel>),
    Bearer(OpenFgaServiceClient<InterceptedService<Channel, BearerTokenInterceptor>>),
    ClientCredentials(
        OpenFgaServiceClient<InterceptedService<Channel, ClientCredentialInterceptor>>,
    ),
}

pub(crate) async fn new_client_from_config() -> OpenFGAResult<Clients> {
    let endpoint = AUTH_CONFIG.endpoint.clone();
    match &AUTH_CONFIG.auth {
        OpenFGAAuth::Anonymous => Ok(Clients::Unauthenticated(
            new_unauthenticated_client(endpoint).await?,
        )),
        OpenFGAAuth::ApiKey(api_key) => Ok(Clients::Bearer(
            new_bearer_auth_client(endpoint, api_key).await?,
        )),
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
        } => Ok(Clients::ClientCredentials(
            new_client_credentials_client(
                endpoint,
                ClientCredentials {
                    client_id: client_id.clone(),
                    client_secret: client_secret.clone(),
                    token_endpoint: token_endpoint.clone(),
                    extra_headers: HeaderMap::default(),
                    extra_oauth_params: HashMap::default(),
                },
                RefreshConfiguration {
                    max_retry: 10,
                    retry_interval: std::time::Duration::from_millis(5),
                },
            )
            .await?,
        )),
    }
}

/// Create a new `OpenFGA` authorizer from the configuration.
///
/// # Errors
/// - Server connection fails
/// - Store (name) not found (from crate Config)
/// - Active Authorization model not found
pub async fn new_authorizer_from_config() -> OpenFGAResult<Authorizers> {
    match new_client_from_config().await? {
        Clients::Unauthenticated(client) => Ok(Authorizers::OpenFGAUnauthorized(
            new_authorizer(client, None).await?,
        )),
        Clients::Bearer(client) => Ok(Authorizers::OpenFGABearer(
            new_authorizer(client, None).await?,
        )),
        Clients::ClientCredentials(client) => Ok(Authorizers::OpenFGAClientCreds(
            new_authorizer(client, None).await?,
        )),
    }
}

/// Create a new `OpenFGA` authorizer with the given client.
/// This must be run after migration.
pub(crate) async fn new_authorizer<T>(
    mut client: OpenFgaServiceClient<T>,
    store_name: Option<String>,
) -> OpenFGAResult<OpenFGAAuthorizer<T>>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    let active_model_version = ModelVersion::active();
    let store_name = store_name.unwrap_or_else(|| AUTH_CONFIG.store_name.clone());

    let store_id = client
        .get_store_by_name(&store_name)
        .await?
        .ok_or(OpenFGAError::StoreNotFound { store: store_name })?
        .id;
    let authorization_model_id =
        get_auth_model_id(&mut client, store_id.clone(), active_model_version).await?;

    Ok(OpenFGAAuthorizer {
        client,
        store_id,
        authorization_model_id,
        health: Arc::new(RwLock::new(vec![])),
    })
}

/// Create a new `OpenFGA` client without authentication.
///
/// Public use intended for testing only.
///
/// # Errors
/// - Connection to `OpenFGA` fails
pub(crate) async fn new_unauthenticated_client(
    endpoint: url::Url,
) -> OpenFGAResult<OpenFgaServiceClient<Channel>> {
    let client = OpenFgaServiceClient::connect(endpoint.to_string()).await?;

    Ok(client)
}

/// Create a new `OpenFGA` client with bearer token.
///
/// # Errors
/// - Connection to `OpenFGA` fails
async fn new_bearer_auth_client(
    endpoint: url::Url,
    token: &str,
) -> OpenFGAResult<OpenFgaServiceClient<InterceptedService<Channel, BearerTokenInterceptor>>> {
    let channel = new_channel(endpoint).await?;
    let interceptor = BearerTokenInterceptor::new(token).map_err(OpenFGAError::bearer_token)?;
    let client = OpenFgaServiceClient::with_interceptor(channel, interceptor);
    Ok(client)
}

/// Create a new `OpenFGA` client with client credentials.
///
/// # Errors
/// - Client credentials cannot be exchanged for a token
/// - Connection to `OpenFGA` fails
async fn new_client_credentials_client(
    endpoint: url::Url,
    credentials: ClientCredentials,
    refresh_config: RefreshConfiguration,
) -> OpenFGAResult<OpenFgaServiceClient<InterceptedService<Channel, ClientCredentialInterceptor>>> {
    let channel = new_channel(endpoint).await?;
    let interceptor = ClientCredentialInterceptor::new_initialized(credentials, refresh_config)?;
    let client: OpenFgaServiceClient<InterceptedService<Channel, ClientCredentialInterceptor>> =
        OpenFgaServiceClient::with_interceptor(channel, interceptor);
    Ok(client)
}

async fn new_channel(endpoint: url::Url) -> OpenFGAResult<Channel> {
    let channel = Endpoint::from_str(endpoint.as_ref())?.connect().await?;

    Ok(channel)
}
