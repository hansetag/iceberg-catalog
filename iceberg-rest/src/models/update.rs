use crate::models;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ViewUpdate {
    AssignUUID(AssignUuidUpdate),
    UpgradeFormatVersion(UpgradeFormatVersionUpdate),
    AddSchema(AddSchemaUpdate),
    SetLocation(SetLocationUpdate),
    SetProperties(SetPropertiesUpdate),
    RemoveProperties(RemovePropertiesUpdate),
    AddViewVersion(AddViewVersionUpdate),
    SetCurrentViewVersion(SetCurrentViewVersionUpdate),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AssignUUID(AssignUuidUpdate),
    UpgradeFormatVersion(UpgradeFormatVersionUpdate),
    AddSchema(AddSchemaUpdate),
    SetCurrentSchema(SetCurrentSchemaUpdate),
    AddPartitionSpec(AddPartitionSpecUpdate),
    SetDefaultSpec(SetDefaultSpecUpdate),
    AddSortOrder(AddSortOrderUpdate),
    SetDefaultSortOrder(SetDefaultSortOrderUpdate),
    AddSnapshot(AddSnapshotUpdate),
    SetSnapshotRef(SetSnapshotRefUpdate),
    RemoveSnapshots(RemoveSnapshotsUpdate),
    RemoveSnapshotRef(RemoveSnapshotRefUpdate),
    SetLocation(SetLocationUpdate),
    SetProperties(SetPropertiesUpdate),
    RemoveProperties(RemovePropertiesUpdate),
    SetStatistics(SetStatisticsUpdate),
    RemoveStatistics(RemoveStatisticsUpdate),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum BaseUpdate {
    AddSchema(AddSchemaUpdate),
    AddSnapshot(AddSnapshotUpdate),
    AddSortOrder(AddSortOrderUpdate),
    AddPartitionSpec(AddPartitionSpecUpdate),
    AddViewVersion(AddViewVersionUpdate),
    AssignUuid(AssignUuidUpdate),
    RemovePartitionStatistics(RemovePartitionStatisticsUpdate),
    RemoveProperties(RemovePropertiesUpdate),
    RemoveSnapshotRef(RemoveSnapshotRefUpdate),
    RemoveSnapshots(RemoveSnapshotsUpdate),
    RemoveStatistics(RemoveStatisticsUpdate),
    SetCurrentSchema(SetCurrentSchemaUpdate),
    SetCurrentViewVersion(SetCurrentViewVersionUpdate),
    SetDefaultSortOrder(SetDefaultSortOrderUpdate),
    SetDefaultSpec(SetDefaultSpecUpdate),
    SetLocation(SetLocationUpdate),
    SetPartitionStatistics(SetPartitionStatisticsUpdate),
    SetProperties(SetPropertiesUpdate),
    SetSnapshotRef(SetSnapshotRefUpdate),
    SetStatistics(SetStatisticsUpdate),
    UpgradeFormatVersion(UpgradeFormatVersionUpdate),
}

impl Validate for BaseUpdate {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            BaseUpdate::AddSchema(u) => u.validate(),
            BaseUpdate::AddSnapshot(u) => u.validate(),
            BaseUpdate::AddSortOrder(u) => u.validate(),
            BaseUpdate::AddPartitionSpec(u) => u.validate(),
            BaseUpdate::AddViewVersion(u) => u.validate(),
            BaseUpdate::AssignUuid(u) => u.validate(),
            BaseUpdate::RemovePartitionStatistics(u) => u.validate(),
            BaseUpdate::RemoveProperties(u) => u.validate(),
            BaseUpdate::RemoveSnapshotRef(u) => u.validate(),
            BaseUpdate::RemoveSnapshots(u) => u.validate(),
            BaseUpdate::RemoveStatistics(u) => u.validate(),
            BaseUpdate::SetCurrentSchema(u) => u.validate(),
            BaseUpdate::SetCurrentViewVersion(u) => u.validate(),
            BaseUpdate::SetDefaultSortOrder(u) => u.validate(),
            BaseUpdate::SetDefaultSpec(u) => u.validate(),
            BaseUpdate::SetLocation(u) => u.validate(),
            BaseUpdate::SetPartitionStatistics(u) => u.validate(),
            BaseUpdate::SetProperties(u) => u.validate(),
            BaseUpdate::SetSnapshotRef(u) => u.validate(),
            BaseUpdate::SetStatistics(u) => u.validate(),
            BaseUpdate::UpgradeFormatVersion(u) => u.validate(),
        }
    }
}

impl Validate for ViewUpdate {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ViewUpdate::AssignUUID(u) => u.validate(),
            ViewUpdate::UpgradeFormatVersion(u) => u.validate(),
            ViewUpdate::AddSchema(u) => u.validate(),
            ViewUpdate::SetLocation(u) => u.validate(),
            ViewUpdate::SetProperties(u) => u.validate(),
            ViewUpdate::RemoveProperties(u) => u.validate(),
            ViewUpdate::AddViewVersion(u) => u.validate(),
            ViewUpdate::SetCurrentViewVersion(u) => u.validate(),
        }
    }
}

impl Validate for TableUpdate {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            TableUpdate::AssignUUID(u) => u.validate(),
            TableUpdate::UpgradeFormatVersion(u) => u.validate(),
            TableUpdate::AddSchema(u) => u.validate(),
            TableUpdate::SetCurrentSchema(u) => u.validate(),
            TableUpdate::AddPartitionSpec(u) => u.validate(),
            TableUpdate::SetDefaultSpec(u) => u.validate(),
            TableUpdate::AddSortOrder(u) => u.validate(),
            TableUpdate::SetDefaultSortOrder(u) => u.validate(),
            TableUpdate::AddSnapshot(u) => u.validate(),
            TableUpdate::SetSnapshotRef(u) => u.validate(),
            TableUpdate::RemoveSnapshots(u) => u.validate(),
            TableUpdate::RemoveSnapshotRef(u) => u.validate(),
            TableUpdate::SetLocation(u) => u.validate(),
            TableUpdate::SetProperties(u) => u.validate(),
            TableUpdate::RemoveProperties(u) => u.validate(),
            TableUpdate::SetStatistics(u) => u.validate(),
            TableUpdate::RemoveStatistics(u) => u.validate(),
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpgradeFormatVersionUpdate {
    #[validate(range(min = 1, max = 2))]
    #[serde(rename = "format-version")]
    pub format_version: i32,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetStatisticsUpdate {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[validate(nested)]
    #[serde(rename = "statistics")]
    pub statistics: models::StatisticsFile,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetSnapshotRefUpdate {
    #[serde(rename = "ref-name")]
    pub ref_name: String,
    #[validate(nested)]
    #[serde(flatten)]
    pub snapshot_ref: models::SnapshotReference,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetPropertiesUpdate {
    #[serde(rename = "updates")]
    pub updates: std::collections::HashMap<String, String>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetPartitionStatisticsUpdate {
    #[validate(nested)]
    #[serde(rename = "partition-statistics")]
    pub partition_statistics: models::PartitionStatisticsFile,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetLocationUpdate {
    #[serde(rename = "location")]
    pub location: String,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetDefaultSpecUpdate {
    /// Partition spec ID to set as the default, or -1 to set last added spec
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetDefaultSortOrderUpdate {
    /// Sort order ID to set as the default, or -1 to set last added sort order
    #[serde(rename = "sort-order-id")]
    pub sort_order_id: i32,
}

/// AssignUuidUpdate : Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AssignUuidUpdate {
    #[serde(rename = "uuid")]
    pub uuid: uuid::Uuid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AddSchemaUpdate {
    #[validate(nested)]
    #[serde(rename = "schema")]
    pub schema: models::Schema,
    /// The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.
    #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
    pub last_column_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AddSnapshotUpdate {
    #[validate(nested)]
    #[serde(rename = "snapshot")]
    pub snapshot: models::Snapshot,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AddPartitionSpecUpdate {
    #[validate(nested)]
    #[serde(rename = "spec")]
    pub spec: Box<models::PartitionSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AddSortOrderUpdate {
    #[validate(nested)]
    #[serde(rename = "sort-order")]
    pub sort_order: models::SortOrder,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AddViewVersionUpdate {
    #[validate(nested)]
    #[serde(rename = "view-version")]
    pub view_version: models::ViewVersion,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct RemovePartitionStatisticsUpdate {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct RemovePropertiesUpdate {
    #[serde(rename = "removals")]
    pub removals: Vec<String>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoveSnapshotRefUpdate {
    #[serde(rename = "ref-name")]
    pub ref_name: String,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoveSnapshotsUpdate {
    #[serde(rename = "snapshot-ids")]
    pub snapshot_ids: Vec<i64>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoveStatisticsUpdate {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetCurrentSchemaUpdate {
    /// Schema ID to set as current, or -1 to set last added schema
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetCurrentViewVersionUpdate {
    /// The view version id to set as current, or -1 to set last added view version id
    #[serde(rename = "view-version-id")]
    pub view_version_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_uuid_update() {
        let json = serde_json::json!({
            "action": "assign-uuid",
            "uuid": "550e8400-e29b-41d4-a716-446655440000"
        });

        let update: BaseUpdate = serde_json::from_value(json.clone()).unwrap();
        match update.clone() {
            BaseUpdate::AssignUuid(u) => {
                assert_eq!(
                    u.uuid,
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
                );
            }
            _ => panic!("Expected BaseUpdate::AssignUuidUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }

    #[test]
    fn test_upgrade_format_version() {
        let json = serde_json::json!({
            "action": "upgrade-format-version",
            "format-version": 2
        });

        let update: BaseUpdate = serde_json::from_value(json.clone()).unwrap();
        match update.clone() {
            BaseUpdate::UpgradeFormatVersion(u) => {
                assert_eq!(u.format_version, 2);
            }
            _ => panic!("Expected BaseUpdate::UpgradeFormatVersionUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }

    #[test]
    fn test_snapshot_ref_update() {
        let json = serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "type": "tag",
            "snapshot-id": 1,
        });

        let update: BaseUpdate = serde_json::from_value(json.clone()).unwrap();
        match update.clone() {
            BaseUpdate::SetSnapshotRef(u) => {
                assert_eq!(u.ref_name, "main");
                assert_eq!(u.snapshot_ref.snapshot_id, 1);
            }
            _ => panic!("Expected BaseUpdate::SetSnapshotRefUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }
}
