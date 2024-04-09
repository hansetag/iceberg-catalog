use crate::models;
use validator::Validate;

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitTableRequest {
    #[validate(nested)]
    #[serde(rename = "identifier", skip_serializing_if = "Option::is_none")]
    pub identifier: Option<models::TableIdentifier>,
    #[validate(nested)]
    #[serde(rename = "requirements")]
    pub requirements: Vec<models::TableRequirement>,
    #[validate(nested)]
    #[serde(rename = "updates")]
    pub updates: Vec<models::TableUpdate>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[validate(nested)]
    #[serde(rename = "metadata")]
    pub metadata: models::TableMetadata,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitTransactionRequest {
    #[validate(nested)]
    #[serde(rename = "table-changes")]
    pub table_changes: Vec<models::CommitTableRequest>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitViewRequest {
    #[validate(nested)]
    #[serde(rename = "identifier", skip_serializing_if = "Option::is_none")]
    pub identifier: Option<models::TableIdentifier>,
    #[validate(nested)]
    #[serde(rename = "requirements", skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<models::ViewRequirement>>,
    #[validate(nested)]
    #[serde(rename = "updates")]
    pub updates: Vec<models::ViewUpdate>,
}
