use super::*;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SqlViewRepresentation {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "sql")]
    pub sql: String,
    #[serde(rename = "dialect")]
    pub dialect: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ViewRepresentation {
    SqlViewRepresentation(SqlViewRepresentation),
}

impl validator::Validate for ViewRepresentation {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::SqlViewRepresentation(t) => t.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct ViewHistoryEntry {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct ViewVersion {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Schema ID to set as current, or -1 to set last added schema
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "summary")]
    pub summary: std::collections::HashMap<String, String>,
    #[validate(nested)]
    #[serde(rename = "representations")]
    pub representations: Vec<ViewRepresentation>,
    #[serde(rename = "default-catalog", skip_serializing_if = "Option::is_none")]
    pub default_catalog: Option<String>,
    /// Reference to one or more levels of a namespace
    #[validate(nested)]
    #[serde(rename = "default-namespace")]
    pub default_namespace: Namespace,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct ViewMetadata {
    #[serde(rename = "view-uuid")]
    pub view_uuid: uuid::Uuid,
    #[serde(rename = "format-version")]
    #[validate(range(min = 1, max = 2))]
    pub format_version: i32,
    #[serde(rename = "location")]
    pub location: String,
    #[serde(rename = "current-version-id")]
    pub current_version_id: i32,
    #[validate(nested)]
    #[serde(rename = "versions")]
    pub versions: Vec<ViewVersion>,
    #[validate(nested)]
    #[serde(rename = "version-log")]
    pub version_log: Vec<ViewHistoryEntry>,
    #[validate(nested)]
    #[serde(rename = "schemas")]
    pub schemas: Vec<Schema>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}
