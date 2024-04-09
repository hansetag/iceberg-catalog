use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct StatisticsFile {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
    #[serde(rename = "file-footer-size-in-bytes")]
    pub file_footer_size_in_bytes: i64,
    #[validate(nested)]
    #[serde(rename = "blob-metadata")]
    pub blob_metadata: Vec<BlobMetadata>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct BlobMetadata {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "fields")]
    pub fields: Vec<i32>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct PartitionStatisticsFile {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
}
