use crate::catalog::{TableIdent, TableRequirement, TableUpdate};
use crate::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};

use super::impl_into_response;

/// Result used when a table is successfully loaded.
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
    /// An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables).
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
