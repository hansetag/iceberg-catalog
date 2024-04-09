/// CatalogConfig : Server-provided configuration for the catalog.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Properties that should be used to override client configuration; applied after defaults and client configuration.
    #[serde(rename = "overrides")]
    pub overrides: std::collections::HashMap<String, String>,
    /// Properties that should be used as default configuration; applied before client configuration.
    #[serde(rename = "defaults")]
    pub defaults: std::collections::HashMap<String, String>,
}

impl CatalogConfig {
    /// Server-provided configuration for the catalog.
    pub fn new(
        overrides: std::collections::HashMap<String, String>,
        defaults: std::collections::HashMap<String, String>,
    ) -> CatalogConfig {
        CatalogConfig {
            overrides,
            defaults,
        }
    }
}
