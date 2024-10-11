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
use async_stream::__private::AsyncStream;
use async_stream::stream;
use futures::{pin_mut, StreamExt};
use openfga_rs::open_fga_service_client::OpenFgaServiceClient;
use openfga_rs::{
    tonic::{
        self,
        codegen::{Body, Bytes, StdError},
    },
    CheckRequest, CheckRequestTupleKey, ConsistencyPreference, ListObjectsRequest, ReadRequest,
    ReadRequestTupleKey, ReadResponse, TupleKey, TupleKeyWithoutCondition, WriteRequest,
    WriteRequestDeletes, WriteRequestWrites,
};
use std::sync::Arc;
use std::{collections::HashSet, str::FromStr};

mod client;
mod entities;
mod error;
mod health;
mod migration;
mod models;
mod service_ext;

use crate::service::authz::implementations::openfga::service_ext::MAX_TUPLES_PER_WRITE;
use crate::service::authz::implementations::FgaType;
use crate::service::authz::{
    NamespaceParent, NamespaceRelation, ProjectRelation, RoleAction, ServerRelation, TableRelation,
    UserAction, ViewRelation, WarehouseRelation,
};
use crate::service::health::Health;
use crate::service::{AuthDetails, RoleId, UserId, ViewIdentUuid};
pub use client::{
    new_authorizer_from_config, BearerOpenFGAAuthorizer, ClientCredentialsOpenFGAAuthorizer,
    UnauthenticatedOpenFGAAuthorizer,
};
pub(crate) use client::{new_client_from_config, Clients};
use entities::OpenFgaEntity;
pub use error::{OpenFGAError, OpenFGAResult};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub use migration::migrate;
pub use models::{CollaborationModels, ModelVersion, OpenFgaType};
pub use openfga_rs::authentication::ClientCredentials;
pub use service_ext::ClientHelper;
use tokio::sync::RwLock;

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
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    pub(crate) client: OpenFgaServiceClient<T>,
    pub(crate) store_id: String,
    pub(crate) authorization_model_id: String,
    pub(crate) health: Arc<RwLock<Vec<Health>>>,
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
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
        let actor = metadata.actor();

        let check_actor_fut = self.check_actor(actor);
        let list_all_fut = self.check(CheckRequestTupleKey {
            user: metadata.actor().to_openfga()?,
            relation: ServerAction::CanListAllProjects.to_string(),
            object: format!("server:{}", AUTH_CONFIG.server_id),
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

    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool> {
        let actor = metadata.actor();
        // Currently all authenticated principals can search users
        self.check_actor(actor).await?;

        match metadata.auth_details {
            AuthDetails::Unauthenticated => Ok(false),
            AuthDetails::Principal(_) => Ok(true),
        }
    }

    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: &RoleAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: role_id.to_openfga()?,
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: &UserAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        self.check_actor(actor).await?;

        let is_same_user = match actor {
            Actor::Role {
                principal,
                assumed_role: _,
            }
            | Actor::Principal(principal) => principal == user_id,
            Actor::Anonymous => false,
        };

        if is_same_user {
            return match action {
                UserAction::CanRead | UserAction::CanUpdate | UserAction::CanDelete => Ok(true),
            };
        }

        let server_id = format!("server:{}", AUTH_CONFIG.server_id);
        match action {
            UserAction::CanRead => {
                self.check(CheckRequestTupleKey {
                    user: actor.to_openfga()?,
                    relation: ServerAction::CanListUsers.to_string(),
                    object: server_id,
                })
                .await
            }
            UserAction::CanUpdate => {
                self.check(CheckRequestTupleKey {
                    user: actor.to_openfga()?,
                    relation: ServerAction::CanUpdateUsers.to_string(),
                    object: server_id,
                })
                .await
            }
            UserAction::CanDelete => {
                self.check(CheckRequestTupleKey {
                    user: actor.to_openfga()?,
                    relation: ServerAction::CanDeleteUsers.to_string(),
                    object: server_id,
                })
                .await
            }
        }
    }

    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &ServerAction,
    ) -> Result<bool> {
        let actor = metadata.actor();
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("server:{}", AUTH_CONFIG.server_id),
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
        let check_actor_fut = self.check_actor(actor);
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
        let check_actor_fut = self.check_actor(actor);
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
        let check_actor_fut = self.check_actor(actor);
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
        let check_actor_fut = self.check_actor(actor);
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
        let check_actor_fut = self.check_actor(actor);
        let check_fut = self.check(CheckRequestTupleKey {
            user: actor.to_openfga()?,
            relation: action.to_string(),
            object: format!("view:{view_id}"),
        });

        let (check_actor, check) = futures::join!(check_actor_fut, check_fut);
        check_actor?;
        check
    }

    async fn create_project(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&project_id).await?;
        let parent_id = format!("server:{}", AUTH_CONFIG.server_id);
        let this_id = project_id.to_openfga()?;
        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga()?,
                    relation: ProjectRelation::ProjectAdmin.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: ProjectRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id,
                    relation: ServerRelation::Child.to_string(),
                    object: parent_id,
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: ProjectIdent,
    ) -> Result<()> {
        self.delete_all_relations(&project_id).await
    }

    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        parent_project_id: ProjectIdent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&warehouse_id).await?;
        let parent_id = parent_project_id.to_openfga()?;
        let this_id = warehouse_id.to_openfga()?;
        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga()?,
                    relation: WarehouseRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: WarehouseRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: ProjectRelation::Child.to_string(),
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
    ) -> Result<()> {
        self.delete_all_relations(&warehouse_id).await
    }

    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        parent: NamespaceParent,
    ) -> Result<()> {
        let actor = metadata.actor();

        self.require_no_relations(&namespace_id).await?;

        let (parent_id, parent_child_relation) = match parent {
            NamespaceParent::Warehouse(warehouse_id) => (
                warehouse_id.to_openfga()?,
                WarehouseRelation::Child.to_string(),
            ),
            NamespaceParent::Namespace(parent_namespace_id) => (
                parent_namespace_id.to_openfga()?,
                NamespaceRelation::Child.to_string(),
            ),
        };
        let this_id = namespace_id.to_openfga()?;

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga()?,
                    relation: NamespaceRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: NamespaceRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: parent_child_relation,
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<()> {
        self.delete_all_relations(&namespace_id).await
    }

    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        let actor = metadata.actor();
        let parent_id = parent.to_openfga()?;
        let this_id = table_id.to_openfga()?;

        self.require_no_relations(&table_id).await?;

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga()?,
                    relation: TableRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: TableRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_table(
        &self,
        _metadata: &RequestMetadata,
        table_id: TableIdentUuid,
    ) -> Result<()> {
        self.delete_all_relations(&table_id).await
    }

    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        let actor = metadata.actor();
        let parent_id = parent.to_openfga()?;
        let this_id = view_id.to_openfga()?;

        self.require_no_relations(&view_id).await?;

        self.write(
            Some(vec![
                TupleKey {
                    user: actor.to_openfga()?,
                    relation: ViewRelation::Ownership.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: parent_id.clone(),
                    relation: ViewRelation::Parent.to_string(),
                    object: this_id.clone(),
                    condition: None,
                },
                TupleKey {
                    user: this_id.clone(),
                    relation: NamespaceRelation::Child.to_string(),
                    object: parent_id.clone(),
                    condition: None,
                },
            ]),
            None,
        )
        .await
        .map_err(Into::into)
    }

    async fn delete_view(&self, _metadata: &RequestMetadata, view_id: ViewIdentUuid) -> Result<()> {
        self.delete_all_relations(&view_id).await
    }
}

impl<T: Sync + Send + Clone> OpenFGAAuthorizer<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    /// A convenience wrapper around write.
    /// All writes happen in a single transaction.
    /// At most 100 writes can be performed in a single transaction.
    async fn write(
        &self,
        writes: Option<Vec<TupleKey>>,
        deletes: Option<Vec<TupleKeyWithoutCondition>>,
    ) -> OpenFGAResult<()> {
        let writes = writes.and_then(|w| (!w.is_empty()).then_some(w));
        let deletes = deletes.and_then(|d| (!d.is_empty()).then_some(d));

        if writes.is_none() && deletes.is_none() {
            return Ok(());
        }

        let num_writes_and_deletes = i32::try_from(
            writes.as_ref().map_or(0, Vec::len) + deletes.as_ref().map_or(0, Vec::len),
        )
        .unwrap_or(i32::MAX);
        if num_writes_and_deletes > MAX_TUPLES_PER_WRITE {
            return Err(OpenFGAError::TooManyWrites {
                actual: num_writes_and_deletes,
                max: MAX_TUPLES_PER_WRITE,
            });
        }

        let write_request = WriteRequest {
            store_id: self.store_id.clone(),
            writes: writes.map(|writes| WriteRequestWrites { tuple_keys: writes }),
            deletes: deletes.map(|deletes| WriteRequestDeletes {
                tuple_keys: deletes,
            }),
            authorization_model_id: self.authorization_model_id.clone(),
        };
        self.client
            .clone()
            .write(write_request.clone())
            .await
            .map_err(|e| OpenFGAError::WriteFailed {
                write_request,
                source: e,
            })
            .map(|_| ())
    }

    /// A convenience wrapper around read that handles error conversion
    async fn read(
        &self,
        page_size: i32,
        tuple_key: ReadRequestTupleKey,
        continuation_token: Option<String>,
    ) -> OpenFGAResult<ReadResponse> {
        let read_request = ReadRequest {
            store_id: self.store_id.clone(),
            page_size: Some(page_size),
            continuation_token: continuation_token.unwrap_or_default(),
            tuple_key: Some(tuple_key),
            consistency: ConsistencyPreference::MinimizeLatency.into(),
        };
        self.client
            .clone()
            .read(read_request.clone())
            .await
            .map_err(|e| OpenFGAError::ReadFailed {
                read_request,
                source: e,
            })
            .map(tonic::Response::into_inner)
    }

    /// A convenience wrapper around check
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

    /// Returns Ok(()) only if not tuples are associated in any relation with the given object.
    async fn require_no_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let openfga_tpye = object.openfga_type().clone();
        let fga_object = object.to_openfga()?;
        let objects = openfga_tpye.user_of();
        let fga_object_str = fga_object.as_str();

        // --------------------- 1. Object as "object" for any user ---------------------
        let tuples = self
            .read(
                1,
                ReadRequestTupleKey {
                    user: String::new(),
                    relation: String::new(),
                    object: fga_object.clone(),
                },
                None,
            )
            .await?
            .tuples;

        if !tuples.is_empty() {
            return Err(ErrorModel::conflict(
                format!("Object to create {fga_object} already has relations"),
                "ObjectHasRelations",
                None,
            )
            .append_detail(format!("Found: {tuples:?}"))
            .into());
        }

        // --------------------- 2. Object as "user" for related objects ---------------------
        let suffixes = suffixes_for_user(&openfga_tpye);

        let futures = objects
            .iter()
            .map(|i| (i, &suffixes))
            .map(|(o, s)| async move {
                for suffix in s {
                    let user = format!("{fga_object_str}{suffix}");
                    let tuples = self
                        .read(
                            1,
                            ReadRequestTupleKey {
                                user,
                                relation: String::new(),
                                object: format!("{o}:"),
                            },
                            None,
                        )
                        .await?;

                    if !tuples.tuples.is_empty() {
                        return Err(IcebergErrorResponse::from(
                            ErrorModel::conflict(
                                format!(
                                    "Object to create {fga_object_str} is used as user for type {o}",
                                ),
                                "ObjectUsedInRelation",
                                None,
                            )
                                .append_detail(format!("Found: {tuples:?}")),
                        ));
                    }
                }

                Ok(())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    async fn delete_all_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let (own_relations, user_relations) = futures::join!(
            self.delete_own_relations(object),
            self.delete_user_relations(object)
        );
        own_relations?;
        user_relations
    }

    async fn delete_user_relations(&self, user: &impl OpenFgaEntity) -> Result<()> {
        let user_type = user.openfga_type().clone();
        let fga_user = user.to_openfga()?;
        let objects = user_type.user_of();
        let fga_user_str = fga_user.as_str();

        let suffixes = suffixes_for_user(&user_type);

        let futures = objects
            .iter()
            .map(|o| (o, &suffixes))
            .map(|(o, s)| async move {
                let mut continuation_token = None;
                for suffix in s {
                    let user = format!("{fga_user_str}{suffix}");
                    while continuation_token != Some(String::new()) {
                        let response = self
                            .read(
                                MAX_TUPLES_PER_WRITE,
                                ReadRequestTupleKey {
                                    user: user.clone(),
                                    relation: String::new(),
                                    object: format!("{o}:"),
                                },
                                continuation_token.clone(),
                            )
                            .await?;
                        continuation_token = Some(response.continuation_token);
                        let keys = response
                            .tuples
                            .into_iter()
                            .filter_map(|t| t.key)
                            .collect::<Vec<_>>();
                        self.write(
                            None,
                            Some(
                                keys.into_iter()
                                    .map(|t| TupleKeyWithoutCondition {
                                        user: t.user,
                                        relation: t.relation,
                                        object: t.object,
                                    })
                                    .collect(),
                            ),
                        )
                        .await?;
                    }
                }

                Result::<_, IcebergErrorResponse>::Ok(())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    async fn delete_own_relations(&self, object: &impl OpenFgaEntity) -> Result<()> {
        let fga_object = object.to_openfga()?;

        let read_stream: AsyncStream<_, _> = stream! {
            let mut continuation_token = None;
            let mut seen= HashSet::new();
            while continuation_token != Some(String::new()) {
                let response = self.read(
                    MAX_TUPLES_PER_WRITE,
                    ReadRequestTupleKey {
                        user: String::new(),
                        relation: String::new(),
                        object: fga_object.clone(),
                    },
                    continuation_token.clone(),
                ).await?;
                continuation_token = Some(response.continuation_token);
                let keys = response.tuples.into_iter().filter_map(|t| t.key).filter(|k| !seen.contains(&(k.user.clone(), k.relation.clone()))).collect::<Vec<_>>();
                seen.extend(keys.iter().map(|k| (k.user.clone(), k.relation.clone())));
                yield Result::<_, IcebergErrorResponse>::Ok(keys);
            }
        };
        pin_mut!(read_stream);
        let mut read_tuples: Option<Vec<TupleKey>> = None;

        let delete_tuples = |t: Option<Vec<TupleKey>>| async {
            match t {
                Some(tuples) => {
                    self.write(
                        None,
                        Some(
                            tuples
                                .into_iter()
                                .map(|t| TupleKeyWithoutCondition {
                                    user: t.user,
                                    relation: t.relation,
                                    object: t.object,
                                })
                                .collect(),
                        ),
                    )
                    .await
                }
                None => Ok(()),
            }
        };

        loop {
            let next_future = read_stream.next();
            let deletion_future = delete_tuples(read_tuples.clone());

            let (tuples, delete) = futures::join!(next_future, deletion_future);
            delete?;

            if let Some(tuples) = tuples.transpose()? {
                read_tuples = (!tuples.is_empty()).then_some(tuples);
            } else {
                break Ok(());
            }
        }
    }

    /// A convenience wrapper around `client.list_objects`
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
                        user: Actor::Principal(principal.clone()).to_openfga()?,
                        relation: RoleAction::CanAssume.to_string(),
                        object: assumed_role.to_openfga()?,
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

fn suffixes_for_user(user: &FgaType) -> Vec<String> {
    user.usersets()
        .iter()
        .map(|s| format!("#{s}"))
        .chain(vec![String::new()])
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authz::implementations::openfga::client::{
            new_authorizer, new_unauthenticated_client,
        };
        use crate::service::RoleId;
        use http::StatusCode;

        async fn new_authorizer_in_empty_store() -> OpenFGAAuthorizer<tonic::transport::Channel> {
            let mut client = new_unauthenticated_client(AUTH_CONFIG.endpoint.clone())
                .await
                .unwrap();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();

            new_authorizer(client, Some(store_name)).await.unwrap()
        }

        #[tokio::test]
        async fn test_require_no_relations_own_relations() {
            let authorizer = new_authorizer_in_empty_store().await;

            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "user:this_user".to_string(),
                        relation: "project_admin".to_string(),
                        object: project_id.to_openfga().unwrap(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let err = authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectHasRelations");
        }

        #[tokio::test]
        async fn test_require_no_relations_used_in_other_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga().unwrap(),
                        relation: "child".to_string(),
                        object: "server:this_server".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            let err = authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
            assert_eq!(err.error.r#type, "ObjectUsedInRelation");
        }

        #[tokio::test]
        async fn test_delete_own_relations_direct() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "user:my_user".to_string(),
                        relation: "project_admin".to_string(),
                        object: project_id.to_openfga().unwrap(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_usersets() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: "role:my_role#assignee".to_string(),
                        relation: "project_admin".to_string(),
                        object: project_id.to_openfga().unwrap(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            for i in 0..502 {
                authorizer
                    .write(
                        Some(vec![
                            TupleKey {
                                user: format!("user:user{i}"),
                                relation: "project_admin".to_string(),
                                object: project_id.to_openfga().unwrap(),
                                condition: None,
                            },
                            TupleKey {
                                user: format!("warehouse:warehouse_{i}"),
                                relation: "child".to_string(),
                                object: project_id.to_openfga().unwrap(),
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_own_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer.delete_own_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: project_id.to_openfga().unwrap(),
                        relation: "parent".to_string(),
                        object: "warehouse:my_warehouse".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_empty() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_many() {
            let authorizer = new_authorizer_in_empty_store().await;
            let project_id = ProjectIdent::from(uuid::Uuid::now_v7());
            authorizer.require_no_relations(&project_id).await.unwrap();

            for i in 0..502 {
                authorizer
                    .write(
                        Some(vec![
                            TupleKey {
                                user: project_id.to_openfga().unwrap(),
                                relation: "parent".to_string(),
                                object: format!("warehouse:warehouse_{i}"),
                                condition: None,
                            },
                            TupleKey {
                                user: project_id.to_openfga().unwrap(),
                                relation: "child".to_string(),
                                object: format!("server:server_{i}"),
                                condition: None,
                            },
                        ]),
                        None,
                    )
                    .await
                    .unwrap();
            }

            authorizer
                .require_no_relations(&project_id)
                .await
                .unwrap_err();
            authorizer.delete_user_relations(&project_id).await.unwrap();
            authorizer.require_no_relations(&project_id).await.unwrap();
        }

        #[tokio::test]
        async fn test_delete_user_relations_userset() {
            let authorizer = new_authorizer_in_empty_store().await;
            let user = RoleId::new(uuid::Uuid::nil());
            authorizer.require_no_relations(&user).await.unwrap();

            authorizer
                .write(
                    Some(vec![TupleKey {
                        user: format!("{}#assignee", user.to_openfga().unwrap()),
                        relation: "project_admin".to_string(),
                        object: "project:my_project".to_string(),
                        condition: None,
                    }]),
                    None,
                )
                .await
                .unwrap();

            authorizer.require_no_relations(&user).await.unwrap_err();
            authorizer.delete_user_relations(&user).await.unwrap();
            authorizer.require_no_relations(&user).await.unwrap();
        }
    }
}
