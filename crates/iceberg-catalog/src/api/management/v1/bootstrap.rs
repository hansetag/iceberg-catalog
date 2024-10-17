use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::Authorizer;
use crate::service::{
    Actor, AuthDetails, Catalog, Result, SecretStore, StartupValidationData, State, Transaction,
};
use crate::{ProjectIdent, CONFIG};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use super::user::{UserLastUpdatedWith, UserType};

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct BootstrapRequest {
    /// Set to true if you accept LAKEKEEPER terms of use.
    pub accept_terms_of_use: bool,
    /// Name of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the name from the provided token.
    /// The initial user will become the global admin.
    #[serde(default)]
    pub user_name: Option<String>,
    /// Email of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the email from the provided token.
    #[serde(default)]
    pub user_email: Option<String>,
    /// Type of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the type from the provided token.
    #[serde(default)]
    pub user_type: Option<UserType>,
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
    /// Default Project ID. Null if not set
    #[schema(value_type = uuid::Uuid)]
    pub default_project_id: Option<ProjectIdent>,
}

impl<C: Catalog, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn bootstrap(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: BootstrapRequest,
    ) -> Result<()> {
        let BootstrapRequest {
            user_name,
            user_email,
            user_type,
            accept_terms_of_use,
        } = request;

        if !accept_terms_of_use {
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
            return Err(ErrorModel::bad_request(
                "Catalog already bootstrapped",
                "CatalogAlreadyBootstrapped",
                None,
            )
            .into());
        }
        // Create user in the catalog
        let principal = match request_metadata.auth_details.clone() {
            AuthDetails::Unauthenticated => None,
            AuthDetails::Principal(principal) => Some(principal),
        };

        if let Some(principal) = principal {
            let acting_user_id = principal.user_id();
            let (name, user_type, email) =
                if let (Some(name), Some(user_type)) = (user_name.clone(), user_type) {
                    (name, user_type, None)
                } else {
                    let (p_name, p_type) = principal.get_name_and_type()?;
                    (
                        user_name.unwrap_or(p_name.to_string()),
                        user_type.unwrap_or(p_type),
                        principal.email(),
                    )
                };
            C::create_or_update_user(
                acting_user_id,
                &name,
                user_email.as_deref().or(email),
                UserLastUpdatedWith::UpdateEndpoint,
                user_type,
                t.transaction(),
            )
            .await?;
        }

        authorizer.bootstrap(&request_metadata).await?;
        t.commit().await
    }

    async fn server_info(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ServerInfo> {
        let actor = request_metadata.auth_details.actor();
        match actor {
            Actor::Anonymous => {
                return Err(ErrorModel::unauthorized(
                    "Authentication required",
                    "AuthenticationRequired",
                    None,
                )
                .into());
            }
            Actor::Principal(_) | Actor::Role { .. } => (),
        }

        // ------------------- Business Logic -------------------
        let version = env!("CARGO_PKG_VERSION").to_string();
        let server_data = C::get_server_info(state.v1_state.catalog).await?;

        Ok(ServerInfo {
            version,
            bootstrapped: server_data != StartupValidationData::NotBootstrapped,
            server_id: CONFIG.server_id,
            default_project_id: CONFIG.default_project_id,
        })
    }
}
