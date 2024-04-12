use crate::catalog::*;
use crate::spec::*;

use super::impl_into_response;

/// LoadTableResult : Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    #[serde(rename = "metadata")]
    pub metadata: TableMetadata,
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterTableRequest {
    pub name: String,
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RenameTableRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListTablesResponse {
    /// An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables). Clients may initiate the first paginated request by sending an empty query parameter `pageToken` to the server. Servers that support pagination should identify the `pageToken` parameter and return a `next-page-token` in the response if there are more results available.  After the initial request, the value of `next-page-token` from each response must be used as the `pageToken` parameter value for the next request. The server must return `null` value for the `next-page-token` in the last response. Servers that support pagination must return all results in a single response with the value of `next-page-token` set to `null` if the query parameter `pageToken` is not set in the request. Servers that do not support pagination should ignore the `pageToken` parameter and return all results in a single response. The `next-page-token` must be omitted from the response. Clients must interpret either `null` or missing response value of `next-page-token` as the end of the listing results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub identifiers: Vec<TableIdent>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableResponse {
    pub metadata_location: String,
    pub metadata: TableMetadata,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

impl_into_response!(LoadTableResult);
impl_into_response!(ListTablesResponse);
impl_into_response!(CommitTableResponse);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_request_minimal() {
        let j = serde_json::json!(
        {
            "name": "tbl_name",
            "schema": {
                "schema-id": 1,
                "type" : "struct",
                "fields" : [ {
                  "id" : 1,
                  "name" : "event_count",
                  "required" : false,
                  "type" : "int",
                  "doc" : "Count of events"
                }, {
                  "id" : 2,
                  "name" : "event_date",
                  "required" : false,
                  "type" : "date"
                } ]
              }
        });

        let r: CreateTableRequest = serde_json::from_value(j).unwrap();
        assert_eq!(r.name, "tbl_name");
        assert_eq!(r.location, None);
        assert_eq!(r.schema.schema_id(), 1);
    }
}
