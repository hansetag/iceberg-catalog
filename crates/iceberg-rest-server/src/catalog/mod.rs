mod config;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
mod tables;

pub use config::Server as ConfigServer;
pub use namespace::{MAX_NAMESPACE_DEPTH, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::{
    service::{auth::AuthZHandler, secrets::SecretStore, Catalog},
    WarehouseIdent,
};
use iceberg_rest_service::{v1::Prefix, ErrorModel, Result};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CatalogServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}
