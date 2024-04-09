use crate::models;
use validator::Validate;

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateViewRequest {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[validate(nested)]
    #[serde(rename = "schema")]
    pub schema: models::Schema,
    #[validate(nested)]
    #[serde(rename = "view-version")]
    pub view_version: models::ViewVersion,
    #[serde(rename = "properties")]
    pub properties: std::collections::HashMap<String, String>,
}

/// LoadViewResult : Result used when a view is successfully loaded.   The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata is returned in the `metadata-location` field. Clients can check whether metadata has changed by comparing metadata locations after the view has been created.  The `config` map returns view-specific configuration for the view's resources.  The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadViewResult {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[validate(nested)]
    #[serde(rename = "metadata")]
    pub metadata: models::ViewMetadata,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
}
