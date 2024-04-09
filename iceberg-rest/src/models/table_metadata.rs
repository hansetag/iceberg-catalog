use crate::models::*;
use validator::Validate;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct TableMetadata {
    #[validate(range(min = 1, max = 2))]
    #[serde(rename = "format-version")]
    pub format_version: i32,
    #[serde(rename = "table-uuid")]
    pub table_uuid: uuid::Uuid,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(rename = "last-updated-ms", skip_serializing_if = "Option::is_none")]
    pub last_updated_ms: Option<i64>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
    #[validate(nested)]
    #[serde(rename = "schemas", skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<Schema>>,
    #[serde(rename = "current-schema-id", skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<i32>,
    #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
    pub last_column_id: Option<i32>,
    #[validate(nested)]
    #[serde(rename = "partition-specs", skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<PartitionSpec>>,
    #[serde(rename = "default-spec-id", skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<i32>,
    #[serde(rename = "last-partition-id", skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<i32>,
    #[validate(nested)]
    #[serde(rename = "sort-orders", skip_serializing_if = "Option::is_none")]
    pub sort_orders: Option<Vec<SortOrder>>,
    #[serde(
        rename = "default-sort-order-id",
        skip_serializing_if = "Option::is_none"
    )]
    pub default_sort_order_id: Option<i32>,
    #[validate(nested)]
    #[serde(rename = "snapshots", skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    #[validate(nested)]
    #[serde(rename = "refs", skip_serializing_if = "Option::is_none")]
    pub refs: Option<std::collections::HashMap<String, SnapshotReference>>,
    #[serde(
        rename = "current-snapshot-id",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_snapshot_id: Option<i64>,
    #[serde(
        rename = "last-sequence-number",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_sequence_number: Option<i64>,
    #[validate(nested)]
    #[serde(rename = "snapshot-log", skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogInner>>,
    #[validate(nested)]
    #[serde(rename = "metadata-log", skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLogInner>>,
    #[validate(nested)]
    #[serde(rename = "statistics-files", skip_serializing_if = "Option::is_none")]
    pub statistics_files: Option<Vec<StatisticsFile>>,
    #[validate(nested)]
    #[serde(
        rename = "partition-statistics-files",
        skip_serializing_if = "Option::is_none"
    )]
    pub partition_statistics_files: Option<Vec<PartitionStatisticsFile>>,
}

impl TableMetadata {
    pub fn new(format_version: i32, table_uuid: uuid::Uuid) -> TableMetadata {
        TableMetadata {
            format_version,
            table_uuid,
            location: None,
            last_updated_ms: None,
            properties: None,
            schemas: None,
            current_schema_id: None,
            last_column_id: None,
            partition_specs: None,
            default_spec_id: None,
            last_partition_id: None,
            sort_orders: None,
            default_sort_order_id: None,
            snapshots: None,
            refs: None,
            current_snapshot_id: None,
            last_sequence_number: None,
            snapshot_log: None,
            metadata_log: None,
            statistics_files: None,
            partition_statistics_files: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct MetadataLogInner {
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}
