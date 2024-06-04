//! Contains Configuration of the SAGA Module
use config::Config;
use std::collections::HashSet;
use url::Url;

use crate::WarehouseIdent;

const DEFAULT_RESERVED_NAMESPACES: [&str; 2] = ["system", "examples"];

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    pub base_uri: url::Url,
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
    pub nats_topic: String,
}

impl DynAppConfig {
    // pub fn s3_signer_uri_for_table(
    //     &self,
    //     warehouse_id: &WarehouseIdent,
    //     namespace_id: &NamespaceIdentUuid,
    //     table_id: &TableIdentUuid,
    // ) -> url::Url {
    //     self.base_uri
    //         .join(&format!(
    //             "v1/{warehouse_id}/namespace/{namespace_id}/table/{table_id}"
    //         ))
    //         .expect("Valid URL")
    // }

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

fn build_config() -> DynAppConfig {
    let mut config: DynAppConfig = Config::builder()
        // ToDo: base_uri requires trailing slash. Make it work without it.
        .set_default("base_uri", "https://localhost:8080/catalog/")
        .expect("Valid base_url")
        .set_default("prefix_template", "{warehouse_id}")
        .expect("Valid prefix_template")
        .set_default("reserved_namespaces", Vec::<String>::default())
        .expect("Valid reserved_namespaces")
        .set_default(
            "pg_encryption_key",
            "<This is unsafe, please set a proper key>",
        )
        .expect("Valid pg_encryption_key")
        .set_default(
            "pg_database_url_read",
            "postgres://postgres:password@localhost:5432/iceberg",
        )
        .expect("Valid pg_database_url")
        .set_default(
            "pg_database_url_write",
            "postgres://postgres:password@localhost:5432/iceberg",
        )
        .expect("Valid pg_database_url")
        .set_default("pg_read_pool_connections", 10)
        .expect("Valid pg_read_pool_connections")
        .set_default("pg_write_pool_connections", 5)
        .expect("Valid pg_write_pool_connections")
        .add_source(
            config::Environment::with_prefix("ICEBERG_REST")
                .try_parsing(true)
                .separator("__")
                .with_list_parse_key("RESERVED_NAMESPACES")
                .list_separator(","),
        )
        .build()
        .expect("Cannot build 'DynAppConfig'.")
        .try_deserialize()
        .expect("Cannot deserialize 'DynAppConfig'.");

    config.reserved_namespaces = config
        .reserved_namespaces
        .into_iter()
        .map(|namespace| namespace.to_lowercase())
        .chain(
            DEFAULT_RESERVED_NAMESPACES
                .into_iter()
                .map(ToOwned::to_owned)
                .collect::<Vec<String>>(),
        )
        .collect::<HashSet<String>>();

    config
}

lazy_static::lazy_static! {
    #[derive(Debug)]
    /// Configuration of the SAGA Module
    pub static ref CONFIG: DynAppConfig = {
        build_config()
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
