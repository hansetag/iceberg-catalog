//! Contains Configuration of the service Module
use clap::Parser;
use std::collections::HashSet;
use url::Url;

use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;

use crate::WarehouseIdent;

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Parser)]
#[allow(clippy::module_name_repetitions)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    #[clap(
        env = "ICEBERG_REST__BASE_URI",
        default_value = "https://localhost:8080/catalog/"
    )]
    pub base_uri: url::Url,
    /// The default Project ID to use. We recommend setting this
    /// only for singe-project deployments. A single project
    /// can still contain multiple warehouses.
    #[clap(env = "ICEBERG_REST__DEFAULT_PROJECT_ID", default_value = None)]
    pub default_project_id: Option<uuid::Uuid>,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    #[clap(
        env = "ICEBERG_REST__PREFIX_TEMPLATE",
        default_value = "{warehouse_id}"
    )]
    prefix_template: String,
    /// Reserved namespaces that cannot be created by users.
    /// This is used to prevent users to create certain
    /// (sub)-namespaces. By default, `system` and `examples` are
    /// reserved. More namespaces can be added here.
    #[clap(
        env = "ICEBERG_REST__RESERVED_NAMESPACES",
        default_value = "system,examples"
    )]
    pub reserved_namespaces: ReservedNamespaces,
    // ------------- POSTGRES IMPLEMENTATION -------------
    #[clap(
        env = "ICEBERG_REST__PG_ENCRYPTION_KEY",
        default_value = "<This is unsafe, please set a proper key>"
    )]
    pub(crate) pg_encryption_key: String,
    #[clap(
        env = "ICEBERG_REST__PG_DATABASE_URL_READ",
        default_value = "postgres://postgres:password@localhost:5432/iceberg"
    )]
    pub(crate) pg_database_url_read: String,
    #[clap(
        env = "ICEBERG_REST__PG_DATABASE_URL_WRITE",
        default_value = "postgres://postgres:password@localhost:5432/iceberg"
    )]
    pub(crate) pg_database_url_write: String,
    #[clap(env = "ICEBERG_REST__PG_READ_POOL_CONNECTIONS", default_value = "10")]
    pub pg_read_pool_connections: u32,
    #[clap(env = "ICEBERG_REST__PG_WRITE_POOL_CONNECTIONS", default_value = "5")]
    pub pg_write_pool_connections: u32,

    // ------------- NATS CLOUDEVENTS -------------
    #[clap(env = "ICEBERG_REST__NATS_ADDRESS")]
    pub nats_address: Option<Url>,
    #[clap(env = "ICEBERG_REST__NATS_TOPIC")]
    pub nats_topic: Option<String>,

    // ------------- AUTHORIZATION -------------
    #[clap(env = "ICEBERG_REST__OAUTH_PROVIDER_URI")]
    pub openid_provider_uri: Option<Url>,
    #[clap(env = "ICEBERG_REST__OAUTH_AUDIENCE")]
    pub audience: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
pub struct ReservedNamespaces(HashSet<String>);
impl Deref for ReservedNamespaces {
    type Target = HashSet<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for ReservedNamespaces {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ReservedNamespaces(
            s.split(',').map(str::to_string).collect(),
        ))
    }
}

impl DynAppConfig {
    pub fn s3_signer_uri_for_warehouse(&self, warehouse_id: &WarehouseIdent) -> url::Url {
        self.base_uri
            .join(&format!("v1/{warehouse_id}"))
            .expect("Valid URL")
    }

    pub fn warehouse_prefix(&self, warehouse_id: &WarehouseIdent) -> String {
        self.prefix_template
            .replace("{warehouse_id}", warehouse_id.to_string().as_str())
    }
}

lazy_static::lazy_static! {
    #[derive(Debug)]
    /// Configuration of the service module.
    pub static ref CONFIG: DynAppConfig = {
        // Hack to use clap to parse from env while we still support subcommands in main.
        DynAppConfig::parse_from([""])
    };
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_default() {
        let _ = &CONFIG.base_uri;
    }

    #[test]
    fn reserved_namespaces_should_contains_default_values() {
        assert!(CONFIG.reserved_namespaces.contains("system"));
        assert!(CONFIG.reserved_namespaces.contains("examples"));
    }
}
