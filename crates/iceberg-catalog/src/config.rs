//! Contains Configuration of the service Module
use std::collections::HashSet;
use url::Url;

use figment::value::{Dict, Map};
use figment::{Error, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use veil::Redact;

use crate::WarehouseIdent;

#[derive(Clone, Deserialize, Serialize, PartialEq, Redact)]
#[allow(clippy::module_name_repetitions)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    pub base_uri: url::Url,
    /// The default Project ID to use. We recommend setting this
    /// only for singe-project deployments. A single project
    /// can still contain multiple warehouses.
    pub default_project_id: Option<uuid::Uuid>,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    prefix_template: String,
    /// Reserved namespaces that cannot be created by users.
    /// This is used to prevent users to create certain
    /// (sub)-namespaces. By default, `system` and `examples` are
    /// reserved. More namespaces can be added here.
    pub reserved_namespaces: HashSet<String>,
    // ------------- POSTGRES IMPLEMENTATION -------------
    pub(crate) pg_encryption_key: String,
    pub(crate) pg_database_url_read: String,
    pub(crate) pg_database_url_write: String,
    pub pg_read_pool_connections: u32,
    pub pg_write_pool_connections: u32,

    // ------------- NATS CLOUDEVENTS -------------
    pub nats_address: Option<Url>,
    pub nats_topic: Option<String>,
    pub nats_creds_file: Option<PathBuf>,
    pub nats_user: Option<String>,
    #[redact]
    pub nats_password: Option<String>,
    #[redact]
    pub nats_token: Option<String>,

    // ------------- AUTHORIZATION -------------
    pub openid_provider_uri: Option<Url>,
}

impl Default for DynAppConfig {
    fn default() -> Self {
        Self {
            base_uri: "https://localhost:8080/catalog/"
                .parse()
                .expect("Valid URL"),
            default_project_id: None,
            prefix_template: "{warehouse_id}".to_string(),
            reserved_namespaces: HashSet::from(["system".to_string(), "examples".to_string()]),
            pg_encryption_key: "<This is unsafe, please set a proper key>".to_string(),
            pg_database_url_read: "postgres://postgres:password@localhost:5432/iceberg".to_string(),
            pg_database_url_write: "postgres://postgres:password@localhost:5432/iceberg"
                .to_string(),
            pg_read_pool_connections: 10,
            pg_write_pool_connections: 5,
            nats_address: None,
            nats_topic: None,
            nats_creds_file: None,
            nats_user: None,
            nats_password: None,
            nats_token: None,
            openid_provider_uri: None,
        }
    }
}

impl Provider for DynAppConfig {
    fn metadata(&self) -> Metadata {
        Metadata::named("iceberg-catalog Config")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        figment::providers::Serialized::defaults(DynAppConfig::default()).data()
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
        figment::Figment::from(DynAppConfig::default()).merge(figment::providers::Env::prefixed("ICEBERG_REST__"))
            .extract::<DynAppConfig>()
            .expect("Valid Configuration")
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
