use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Location of the snapshot's manifest list file
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    #[validate(nested)]
    #[serde(rename = "summary")]
    pub summary: SnapshotSummary,
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SnapshotSummary {
    #[validate(nested)]
    #[serde(rename = "operation")]
    pub operation: SummaryOperation,
}

impl SnapshotSummary {
    pub fn new(operation: SummaryOperation) -> SnapshotSummary {
        SnapshotSummary { operation }
    }
}
///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SummaryOperation {
    #[serde(rename = "append")]
    Append,
    #[serde(rename = "replace")]
    Replace,
    #[serde(rename = "overwrite")]
    Overwrite,
    #[serde(rename = "delete")]
    Delete,
}

impl Validate for SummaryOperation {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SnapshotReference {
    #[validate(nested)]
    #[serde(rename = "type")]
    pub r#type: SnapshotType,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,
    #[serde(
        rename = "max-snapshot-age-ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_snapshot_age_ms: Option<i64>,
    #[serde(
        rename = "min-snapshots-to-keep",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_snapshots_to_keep: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SnapshotType {
    #[serde(rename = "tag")]
    Tag,
    #[serde(rename = "branch")]
    Branch,
}

impl Validate for SnapshotType {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SnapshotLogInner {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}
