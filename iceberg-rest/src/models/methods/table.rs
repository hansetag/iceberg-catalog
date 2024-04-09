use crate::models;
use crate::validations::*;
use validator::Validate;

/// LoadTableResult : Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    #[serde(rename = "metadata-location", skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[validate(nested)]
    #[serde(rename = "metadata")]
    pub metadata: models::TableMetadata,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateTableRequest {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[validate(nested)]
    #[serde(rename = "schema")]
    pub schema: models::Schema,
    #[validate(nested)]
    #[serde(rename = "partition-spec", skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<models::PartitionSpec>,
    #[validate(nested)]
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    pub write_order: Option<models::SortOrder>,
    #[serde(rename = "stage-create", skip_serializing_if = "Option::is_none")]
    pub stage_create: Option<bool>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegisterTableRequest {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct RenameTableRequest {
    #[validate(nested)]
    #[serde(rename = "source")]
    pub source: models::TableIdentifier,
    #[validate(nested)]
    #[serde(rename = "destination")]
    pub destination: models::TableIdentifier,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListTablesResponse {
    /// An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables). Clients may initiate the first paginated request by sending an empty query parameter `pageToken` to the server. Servers that support pagination should identify the `pageToken` parameter and return a `next-page-token` in the response if there are more results available.  After the initial request, the value of `next-page-token` from each response must be used as the `pageToken` parameter value for the next request. The server must return `null` value for the `next-page-token` in the last response. Servers that support pagination must return all results in a single response with the value of `next-page-token` set to `null` if the query parameter `pageToken` is not set in the request. Servers that do not support pagination should ignore the `pageToken` parameter and return all results in a single response. The `next-page-token` must be omitted from the response. Clients must interpret either `null` or missing response value of `next-page-token` as the end of the listing results.
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    #[validate(custom(function = "validate_unique_vec"))]
    #[serde(rename = "identifiers", skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<Vec<models::TableIdentifier>>,
}

