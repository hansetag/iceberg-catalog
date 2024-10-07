pub mod authz;
mod catalog;
pub mod contract_verification;
pub mod event_publisher;
pub mod health;
pub mod secrets;
pub mod storage;
mod tabular_idents;
pub mod task_queue;
pub mod token_verification;

pub use catalog::{
    Catalog, CommitTableResponse, CreateNamespaceRequest, CreateNamespaceResponse,
    CreateTableRequest, CreateTableResponse, DeletionDetails, DropFlags, GetNamespaceResponse,
    GetProjectResponse, GetStorageConfigResponse, GetTableMetadataResponse, GetWarehouseResponse,
    ListFlags, ListNamespacesQuery, ListNamespacesResponse, LoadTableResponse, NamespaceIdent,
    Result, TableCommit, TableCreation, TableIdent, Transaction, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse, ViewMetadataWithLocation,
};
use std::ops::Deref;
pub(crate) use tabular_idents::TabularIdentBorrowed;
pub use tabular_idents::{TabularIdentOwned, TabularIdentUuid};

use self::authz::Authorizer;
use crate::api::iceberg::v1::Prefix;
use crate::api::ThreadSafe as ServiceState;
pub use crate::api::{ErrorModel, IcebergErrorResponse};
use crate::service::contract_verification::ContractVerifiers;
use crate::service::event_publisher::CloudEventsPublisher;
use crate::service::task_queue::TaskQueues;
use http::StatusCode;
pub use secrets::{SecretIdent, SecretStore};
use std::str::FromStr;

// ---------------- State ----------------
#[derive(Clone, Debug)]
pub struct State<A: Authorizer + Clone, C: Catalog, S: SecretStore> {
    pub authz: A,
    pub catalog: C::State,
    pub secrets: S,
    pub publisher: CloudEventsPublisher,
    pub contract_verifiers: ContractVerifiers,
    pub queues: TaskQueues,
}

impl<A: Authorizer + Clone, C: Catalog, S: SecretStore> ServiceState for State<A, C, S> {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct ViewIdentUuid(uuid::Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct NamespaceIdentUuid(uuid::Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct TableIdentUuid(uuid::Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
pub struct WarehouseIdent(pub(crate) uuid::Uuid);

/// Status of a warehouse
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum_macros::Display,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "warehouse_status", rename_all = "kebab-case")
)]
pub enum WarehouseStatus {
    /// The warehouse is active and can be used
    Active,
    /// The warehouse is inactive and cannot be used.
    Inactive,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy,
)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct ProjectIdent(uuid::Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleIdent(String);

impl FromStr for RoleIdent {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_entity_id(s, "Role")?;
        Ok(RoleIdent(s.to_string()))
    }
}

impl std::fmt::Display for RoleIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for RoleIdent {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RoleIdent> for String {
    fn from(ident: RoleIdent) -> Self {
        ident.0
    }
}

impl Deref for ViewIdentUuid {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for NamespaceIdentUuid {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for ViewIdentUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ViewIdentUuid {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ViewIdentUuid(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided view id is not a valid UUID".to_string())
                .r#type("ViewIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl Deref for NamespaceIdentUuid {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NamespaceIdentUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for NamespaceIdentUuid {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NamespaceIdentUuid(uuid::Uuid::from_str(s).map_err(
            |e| {
                ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("Provided namespace id is not a valid UUID".to_string())
                    .r#type("NamespaceIDIsNotUUID".to_string())
                    .source(Some(Box::new(e)))
                    .build()
            },
        )?))
    }
}

impl From<uuid::Uuid> for NamespaceIdentUuid {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<&uuid::Uuid> for NamespaceIdentUuid {
    fn from(uuid: &uuid::Uuid) -> Self {
        Self(*uuid)
    }
}

impl Default for TableIdentUuid {
    fn default() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for TableIdentUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for TableIdentUuid {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<uuid::Uuid> for TableIdentUuid {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl FromStr for TableIdentUuid {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TableIdentUuid(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided table id is not a valid UUID".to_string())
                .r#type("TableIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl From<TableIdentUuid> for uuid::Uuid {
    fn from(ident: TableIdentUuid) -> Self {
        ident.0
    }
}

// ---------------- Identifier ----------------

impl Deref for ProjectIdent {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ProjectIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ProjectIdent {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ProjectIdent(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided project id is not a valid UUID".to_string())
                .r#type("ProjectIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl WarehouseIdent {
    #[must_use]
    pub fn to_uuid(&self) -> uuid::Uuid {
        **self
    }

    #[must_use]
    pub fn as_uuid(&self) -> &uuid::Uuid {
        self
    }
}

impl Deref for WarehouseIdent {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<uuid::Uuid> for WarehouseIdent {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for WarehouseIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WarehouseIdent {
    type Err = IcebergErrorResponse;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(WarehouseIdent(uuid::Uuid::from_str(s).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Provided warehouse id is not a valid UUID".to_string())
                .r#type("WarehouseIDIsNotUUID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?))
    }
}

impl From<uuid::Uuid> for ProjectIdent {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl TryFrom<Prefix> for WarehouseIdent {
    type Error = IcebergErrorResponse;

    fn try_from(value: Prefix) -> Result<Self, Self::Error> {
        let prefix = uuid::Uuid::parse_str(value.as_str()).map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Provided prefix is not a warehouse id. Expected UUID, got: {}",
                    value.as_str()
                ))
                .r#type("PrefixIsNotWarehouseID".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?;
        Ok(WarehouseIdent(prefix))
    }
}

pub(crate) fn validate_entity_id(s: &str, entity_name: &str) -> Result<()> {
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ErrorModel::bad_request(
            format!("Invalid characters in {entity_name} id"),
            "InvalidEntityId",
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    // All lowercase
    if s.to_lowercase() != s {
        return Err(ErrorModel::bad_request(
            format!("{entity_name} id must be lowercase"),
            "InvalidEntityId",
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    // Max length 128
    if s.len() > 128 {
        return Err(ErrorModel::bad_request(
            format!("{entity_name} id must be at most 128 characters"),
            "InvalidEntityId",
            None,
        )
        .append_detail(format!("{entity_name}: {s}"))
        .into());
    }

    Ok(())
}
