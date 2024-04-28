pub mod auth;
mod catalog;
pub mod config;
pub mod secrets;
pub mod storage;

pub use catalog::Catalog;

use http::StatusCode;
use iceberg_rest_service::v1::Prefix;
use iceberg_rest_service::State as ServiceState;
use iceberg_rest_service::{ErrorModel, IcebergErrorResponse};
use std::str::FromStr;

pub use secrets::{SecretIdent, SecretsState};

// ---------------- State ----------------
#[allow(clippy::module_name_repetitions)]
pub trait AuthState: Clone + Send + Sync + 'static {}
#[allow(clippy::module_name_repetitions)]
pub trait CatalogState: Clone + Send + Sync + 'static {}

#[derive(Clone, Debug)]
pub struct State<A: AuthState, D: CatalogState, S: SecretsState> {
    pub auth_state: A,
    pub catalog_state: D,
    pub secrets_state: S,
}

impl<A: AuthState, D: CatalogState, S: SecretsState> ServiceState for State<A, D, S> {}

// ---------------- Identifier ----------------
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
// Is UUID here too strict?
pub struct ProjectIdent(uuid::Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
pub struct WarehouseIdent(uuid::Uuid);

impl WarehouseIdent {
    #[must_use]
    pub fn into_uuid(&self) -> uuid::Uuid {
        self.0
    }

    #[must_use]
    pub fn as_uuid(&self) -> &uuid::Uuid {
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

impl ProjectIdent {
    #[must_use]
    #[inline]
    pub fn into_uuid(&self) -> uuid::Uuid {
        self.0
    }

    #[must_use]
    #[inline]
    pub fn as_uuid(&self) -> &uuid::Uuid {
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
                .stack(Some(vec![e.to_string()]))
                .build()
        })?))
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
                .stack(Some(vec![e.to_string()]))
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
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;
        Ok(WarehouseIdent(prefix))
    }
}
