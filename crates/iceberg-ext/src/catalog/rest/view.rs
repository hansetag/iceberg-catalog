use crate::catalog::rest::*;
use crate::catalog::*;
use crate::spec::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateViewRequest {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(rename = "schema")]
    pub schema: Schema,
    #[serde(rename = "view-version")]
    pub view_version: ViewVersion,
    #[serde(rename = "properties")]
    pub properties: std::collections::HashMap<String, String>,
}

/// LoadViewResult : Result used when a view is successfully loaded.   The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata is returned in the `metadata-location` field. Clients can check whether metadata has changed by comparing metadata locations after the view has been created.  The `config` map returns view-specific configuration for the view's resources.  The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadViewResult {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(rename = "metadata")]
    pub metadata: ViewMetadata,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitViewRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    #[serde(rename = "requirements", skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<ViewRequirement>>,
    pub updates: Vec<ViewUpdate>,
}

impl_into_response!(LoadViewResult);
