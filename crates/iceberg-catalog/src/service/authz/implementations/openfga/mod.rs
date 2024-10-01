use std::{collections::HashSet, str::FromStr};

use crate::{
    request_metadata::RequestMetadata,
    service::{
        authz::{
            Authorizer, ErrorModel, ListProjectsResponse, NamespaceAction, ProjectAction, Result,
            ServerAction, TableAction, ViewAction, WarehouseAction,
        },
        token_verification::Actor,
        NamespaceIdentUuid, TableIdentUuid,
    },
    ProjectIdent, WarehouseIdent, CONFIG,
};
use openfga_rs::open_fga_service_client::OpenFgaServiceClient;
use openfga_rs::{
    tonic::{
        self,
        codegen::{Body, Bytes, StdError},
    },
    CheckRequest, CheckRequestTupleKey, ConsistencyPreference, ListObjectsRequest,
};

mod client;
mod models;
mod service_ext;

pub use client::{new_bearer_auth_client, new_client_credentials, new_unauthenticated_client};
pub use models::{CollaborationModelVersion, CollaborationModels};
pub use service_ext::ClientHelper;

lazy_static::lazy_static! {
    static ref AUTH_CONFIG: crate::config::OpenFGAConfig = {

        CONFIG.openfga.clone().expect("OpenFGAConfig not found")
    };
}

#[derive(Clone, Debug)]
pub struct OpenFGAAuthorizer<T>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    client: OpenFgaServiceClient<T>,
    store_id: String,
    authorization_model_id: String,
}

#[async_trait::async_trait]
impl<T> Authorizer for OpenFGAAuthorizer<T>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<axum::body::Bytes, openfga_rs::tonic::Status>,
    >>::Future: Send,
{
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
        let actor = metadata.actor();

        let check_actor_fut = self.check_actor(&actor);
        let list_all_fut = self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga()?,
            relation: ServerAction::CanListAllProjects.to_string(),
            object: format!("server:{}", AUTH_CONFIG.server_id()),
        });

        let (check_actor, list_all) = futures::join!(check_actor_fut, list_all_fut);
        check_actor?;
        let list_all = list_all?;

        if list_all {
            return Ok(ListProjectsResponse::All);
        }

        let projects = self
            .list_objects(
                "project",
                ProjectAction::CanShowInList.to_string(),
                actor.to_openfga()?,
            )
            .await?
            .iter()
            .map(|p| {
                ProjectIdent::from_str(p).map_err(|_e| {
                    ErrorModel::internal(
                        "Failed to parse project id",
                        "ListProjectsIdParseError",
                        None,
                    )
                    .append_detail(format!("Project id: {p}"))
                    .into()
                })
            })
            .collect::<Result<HashSet<ProjectIdent>>>()?;

        Ok(ListProjectsResponse::Projects(projects))
    }

    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &ServerAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("server:{}", AUTH_CONFIG.server_id()),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: &ProjectAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("project:{project_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &WarehouseAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("warehouse:{warehouse_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    /// Return the namespace_id if the action is allowed, otherwise return None.
    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        action: &NamespaceAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("namespace:{namespace_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    /// Return the table_id if the action is allowed, otherwise return None.
    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        table_id: TableIdentUuid,
        action: &TableAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("table:{table_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    /// Return the view_id if the action is allowed, otherwise return None.
    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        view_id: TableIdentUuid,
        action: &ViewAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(&actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("view:{view_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }
}

impl<T: Sync + Send + Clone> OpenFGAAuthorizer<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    async fn check(&self, tuple_key: CheckRequestTupleKey) -> Result<bool> {
        self.client
            .clone()
            .check(CheckRequest {
                tuple_key: Some(tuple_key),
                store_id: self.store_id.clone(),
                authorization_model_id: self.authorization_model_id.clone(),
                contextual_tuples: None,
                trace: false,
                context: None,
                consistency: ConsistencyPreference::MinimizeLatency.into(),
            })
            .await
            .map_err(|e| {
                let msg = e.message().to_string();
                let code = e.code().to_string();
                ErrorModel::internal(
                    "Failed to check authorization",
                    "AuthorizationCheckFailed",
                    Some(Box::new(e)),
                )
                .append_detail(msg)
                .append_detail(format!("Tonic code: {code}"))
                .into()
            })
            .map(|response| response.get_ref().allowed)
    }

    async fn list_objects(
        &self,
        r#type: impl Into<String>,
        relation: impl Into<String>,
        user: impl Into<String>,
    ) -> Result<Vec<String>> {
        let user = user.into();
        self.client
            .clone()
            .list_objects(ListObjectsRequest {
                r#type: r#type.into(),
                relation: relation.into(),
                user: user.clone(),
                store_id: self.store_id.clone(),
                authorization_model_id: self.authorization_model_id.clone(),
                contextual_tuples: None,
                context: None,
                consistency: ConsistencyPreference::MinimizeLatency.into(),
            })
            .await
            .map_err(|e| {
                let msg = e.message().to_string();
                let code = e.code().to_string();
                ErrorModel::internal(
                    "Failed to expand authorization",
                    "AuthorizationExpandFailed",
                    Some(Box::new(e)),
                )
                .append_detail(msg)
                .append_detail(format!("Tonic code: {code}"))
                .into()
            })
            .map(|response| {
                let s: Vec<String> = response.into_inner().objects;
                // cut off the user: prefix
                s.iter().map(|s| s[user.len()..].to_string()).collect()
            })
    }

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_actor(&self, actor: &Actor) -> Result<()> {
        match actor {
            Actor::Principal(_) | Actor::Anonymous => Ok(()),
            Actor::Role {
                principal,
                assumed_role,
            } => {
                let assume_role_allowed = self
                    .check(CheckRequestTupleKey {
                        user: Actor::Principal(principal.to_string()).to_openfga()?,
                        relation: "can_assume".to_string(),
                        object: actor.to_openfga()?,
                    })
                    .await?;

                if assume_role_allowed {
                    Ok(())
                } else {
                    Err(ErrorModel::forbidden(
                        format!(
                            "Principal is not allowed to assume the specified role with id {assumed_role}"
                        ),
                        "RoleAssumptionNotAllowed",
                        None,
                    )
                    .into())
                }
            }
        }
    }
}

trait ToOpenFGA {
    fn to_openfga(&self) -> Result<String>;
}

impl ToOpenFGA for Actor {
    fn to_openfga(&self) -> Result<String> {
        match self {
            Actor::Anonymous => Ok("user:*".to_string()),
            Actor::Principal(principal) => {
                validate_user_chars(principal, "Principal")?;
                Ok(format!("user:{principal}"))
            }
            Actor::Role {
                principal: _,
                assumed_role,
            } => {
                validate_user_chars(assumed_role, "Role")?;
                Ok(format!("role:{assumed_role}#assignee"))
            }
        }
    }
}

fn validate_user_chars(s: &str, entity_name: &str) -> Result<()> {
    let error_typ = capitalize(entity_name);

    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ErrorModel::bad_request(
            format!("Invalid characters in {entity_name} id"),
            error_typ,
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    // All lowercase
    if s.to_lowercase() != s {
        return Err(ErrorModel::bad_request(
            format!("{entity_name} id must be lowercase"),
            error_typ,
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    // Max length 128
    if s.len() > 128 {
        return Err(ErrorModel::bad_request(
            format!("{entity_name} id must be at most 128 characters"),
            error_typ,
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    Ok(())
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}
