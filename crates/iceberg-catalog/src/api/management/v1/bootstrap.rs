use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, ServerAction};
use crate::service::{Catalog, Result, SecretStore, StartupValidationData, State, Transaction};
use crate::CONFIG;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct BootstrapRequest {
    /// Set to true if you accept LAKEKEEPER terms of use.
    pub accept_terms_of_use: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ServerInfo {
    /// Version of the server.
    pub version: String,
    /// Whether the catalog has been bootstrapped.
    pub bootstrapped: bool,
    /// ID of the server.
    pub server_id: uuid::Uuid,
}

impl<C: Catalog, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn bootstrap(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: BootstrapRequest,
    ) -> Result<()> {
        if !request.accept_terms_of_use {
            return Err(ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message("You must accept the terms of use to bootstrap the catalog.".to_string())
                .r#type("TermsOfUseNotAccepted".to_string())
                .build()
                .into());
        }

        // ------------------- AUTHZ -------------------
        // We check at two places if we can bootstrap: AuthZ and the catalog.
        // AuthZ just checks if the request metadata could be added as the servers
        // global admin
        let authorizer = state.v1_state.authz;
        authorizer.can_bootstrap(&request_metadata).await?;

        // ------------------- Business Logic -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let success = C::bootstrap(request.accept_terms_of_use, t.transaction()).await?;
        if !success {
            return Err(ErrorModel::unauthorized(
                "Catalog already bootstrapped",
                "CatalogAlreadyBootstrapped",
                None,
            )
            .into());
        }
        authorizer.bootstrap(&request_metadata).await?;
        t.commit().await
    }

    async fn server_info(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ServerInfo> {
        // ------------------- AuthZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .is_allowed_server_action(&request_metadata, &ServerAction::CanReadServerInfo)
            .await?;

        // ------------------- Business Logic -------------------
        let version = env!("CARGO_PKG_VERSION").to_string();
        let server_data = C::get_server_info(state.v1_state.catalog).await?;

        Ok(ServerInfo {
            version,
            bootstrapped: server_data != StartupValidationData::NotBootstrapped,
            server_id: CONFIG.server_id,
        })
    }
}
