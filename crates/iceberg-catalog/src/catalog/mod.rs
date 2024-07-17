mod config;

pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
mod tables;
mod views;

pub use config::Server as ConfigServer;
pub use namespace::{MAX_NAMESPACE_DEPTH, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::api::{iceberg::v1::Prefix, ErrorModel, Result};
use crate::service::{InvalidIdentifierError, WarehouseStatus};
use crate::{
    service::{auth::AuthZHandler, secrets::SecretStore, Catalog},
    WarehouseIdent,
};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CatalogServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

#[derive(thiserror::Error, Debug)]
pub enum PrefixError {
    #[error(transparent)]
    NoPrefixProvided(#[from] NoPrefixProvided),
    #[error(transparent)]
    PrefixIsNotWarehouseID(#[from] InvalidIdentifierError),
}

impl From<PrefixError> for IcebergErrorResponse {
    fn from(slf: PrefixError) -> Self {
        match slf {
            PrefixError::PrefixIsNotWarehouseID(e) => ErrorModel::from(e).into(),
            PrefixError::NoPrefixProvided(e) => ErrorModel::from(e).into(),
        }
    }
}

impl From<PrefixError> for ErrorModel {
    fn from(slf: PrefixError) -> Self {
        match slf {
            PrefixError::PrefixIsNotWarehouseID(e) => e.into(),
            PrefixError::NoPrefixProvided(e) => e.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[error("No prefix specified. The warehouse-id must be provided as prefix in the URL.")]
pub struct NoPrefixProvided;

impl From<NoPrefixProvided> for ErrorModel {
    fn from(slf: NoPrefixProvided) -> Self {
        ErrorModel::bad_request(slf.to_string(), "NoPrefixProvided", Some(Box::new(slf)))
    }
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent, PrefixError> {
    Ok(prefix.ok_or(NoPrefixProvided)?.try_into()?)
}

fn require_no_location_specified(location: &Option<String>) -> Result<()> {
    if location.is_some() {
        return Err(ErrorModel::bad_request(
            "Specifying a Table `location` is not supported. Location is managed by the Catalog.",
            "LocationNotSupported",
            None,
        )
        .into());
    }
    Ok(())
}

fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
    if status != WarehouseStatus::Active {
        return Err(
            ErrorModel::not_found("Warehouse is not active.", "WarehouseNotActive", None).into(),
        );
    }
    Ok(())
}
