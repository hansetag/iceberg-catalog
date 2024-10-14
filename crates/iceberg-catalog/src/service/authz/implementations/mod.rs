use crate::{
    service::{
        authz::ErrorModel,
        health::{Health, HealthExt},
    },
    AuthZBackend, CONFIG,
};

pub(super) mod allow_all;

pub mod openfga;

/// Get the default authorizer from the configuration
///
/// # Errors
/// Default model is not obtainable, i.e. if the model is not found in openfga
// Return error model here to convert it into anyhow in bin. IcebergErrorResponse does
// not implement StdError
pub async fn get_default_authorizer_from_config() -> Result<Authorizers, ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(allow_all::AllowAllAuthorizer.into()),
        AuthZBackend::OpenFGA => Ok(openfga::new_authorizer_from_config().await?),
    }
}

/// Migrate the default authorizer to a new model version.
///
/// # Errors
/// Migration fails - for details check the documentation of the configured
/// Authorizer implementation
pub async fn migrate_default_authorizer() -> std::result::Result<(), ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(()),
        AuthZBackend::OpenFGA => {
            let client = openfga::new_client_from_config().await?;
            let store_name = None;
            match client {
                openfga::Clients::Unauthenticated(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
                openfga::Clients::Bearer(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
                openfga::Clients::ClientCredentials(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
            }
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, strum_macros::Display, strum_macros::AsRefStr, strum_macros::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum FgaType {
    User,
    Role,
    Server,
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    ModelVersion,
    AuthModelId,
}

#[derive(Debug, Clone)]
pub enum Authorizers {
    AllowAll(allow_all::AllowAllAuthorizer),
    OpenFGAUnauthorized(openfga::UnauthenticatedOpenFGAAuthorizer),
    OpenFGABearer(openfga::BearerOpenFGAAuthorizer),
    OpenFGAClientCreds(openfga::ClientCredentialsOpenFGAAuthorizer),
}

impl From<allow_all::AllowAllAuthorizer> for Authorizers {
    fn from(authorizer: allow_all::AllowAllAuthorizer) -> Self {
        Self::AllowAll(authorizer)
    }
}

impl From<openfga::UnauthenticatedOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::UnauthenticatedOpenFGAAuthorizer) -> Self {
        Self::OpenFGAUnauthorized(authorizer)
    }
}

impl From<openfga::BearerOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::BearerOpenFGAAuthorizer) -> Self {
        Self::OpenFGABearer(authorizer)
    }
}

impl From<openfga::ClientCredentialsOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::ClientCredentialsOpenFGAAuthorizer) -> Self {
        Self::OpenFGAClientCreds(authorizer)
    }
}

#[async_trait::async_trait]
impl HealthExt for Authorizers {
    async fn health(&self) -> Vec<Health> {
        match self {
            Self::AllowAll(authorizer) => authorizer.health().await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.health().await,
            Self::OpenFGABearer(authorizer) => authorizer.health().await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.health().await,
        }
    }

    async fn update_health(&self) {
        match self {
            Self::AllowAll(authorizer) => authorizer.update_health().await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.update_health().await,
            Self::OpenFGABearer(authorizer) => authorizer.update_health().await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.update_health().await,
        }
    }
}
