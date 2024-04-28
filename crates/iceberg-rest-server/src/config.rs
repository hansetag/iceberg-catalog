//! Contains Configuration of the SAGA Module
use config::Config;

use crate::WarehouseIdent;

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    pub base_uri: url::Url,
    /// S3 Signer URL. If not specified, the base_uri is used
    s3_signer_uri: Option<url::Url>,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain {warehouse_id} placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// warehouse_id, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: "{warehouse_id}"
    prefix_template: String,

    // ------------- POSTGRES IMPLEMENTATION -------------
    pub(crate) pg_encryption_key: String,
    pub(crate) pg_database_url_read: String,
    pub(crate) pg_database_url_write: String,
    pub pg_read_pool_connections: u32,
    pub pg_write_pool_connections: u32,
}

impl DynAppConfig {
    pub fn s3_signer_uri(&self) -> &url::Url {
        self.s3_signer_uri.as_ref().unwrap_or(&self.base_uri)
    }

    pub fn warehouse_prefix(&self, warehouse_id: &WarehouseIdent) -> String {
        self.prefix_template
            .replace("{warehouse_id}", warehouse_id.to_string().as_str())
    }
}

lazy_static::lazy_static! {
    #[derive(Debug)]
    /// Configurtion of the SAGA Module
    pub static ref CONFIG: DynAppConfig = {
        let config: DynAppConfig = Config::builder()
        .set_default("base_uri", "https://localhost:8080/catalog").expect("Valid base_url")
        .set_default("s3_signer_uri", None::<String>).expect("Valid s3_signer_uri")
        .set_default("prefix_template", "{warehouse_id}").expect("Valid prefix_template")
        .set_default("pg_encryption_key", "<This is unsafe, please set a proper key>").expect("Valid pg_encryption_key")
        .set_default("pg_database_url_read", "postgres://postgres:password@localhost:5432/iceberg").expect("Valid pg_database_url")
        .set_default("pg_database_url_write", "postgres://postgres:password@localhost:5432/iceberg").expect("Valid pg_database_url")
        .set_default("pg_read_pool_connections", 10).expect("Valid pg_read_pool_connections")
        .set_default("pg_write_pool_connections", 5).expect("Valid pg_write_pool_connections")
        .add_source(
            config::Environment::with_prefix("ICEBERG_REST")
            .try_parsing(true)
            .separator("__"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();
    config
    };
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_default() {
        let _ = &CONFIG.base_uri;
        let _ = &CONFIG.s3_signer_uri();
    }
}
