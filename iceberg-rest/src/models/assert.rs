use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate(AssertCreate),
    AssertTableUuid(AssertTableUuid),
    AssertRefSnapshotId(AssertRefSnapshotId),
    AssertLastAssignedFieldId(AssertLastAssignedFieldId),
    AssertCurrentSchemaId(AssertCurrentSchemaId),
    AssertLastAssignedPartitionId(AssertLastAssignedPartitionId),
    AssertDefaultSpecId(AssertDefaultSpecId),
    AssertDefaultSortOrderId(AssertDefaultSortOrderId),
}

impl Validate for TableRequirement {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            TableRequirement::AssertCreate(x) => x.validate(),
            TableRequirement::AssertTableUuid(x) => x.validate(),
            TableRequirement::AssertRefSnapshotId(x) => x.validate(),
            TableRequirement::AssertLastAssignedFieldId(x) => x.validate(),
            TableRequirement::AssertCurrentSchemaId(x) => x.validate(),
            TableRequirement::AssertLastAssignedPartitionId(x) => x.validate(),
            TableRequirement::AssertDefaultSpecId(x) => x.validate(),
            TableRequirement::AssertDefaultSortOrderId(x) => x.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ViewRequirement {
    AssertViewUuid(AssertViewUuid),
}

impl Validate for ViewRequirement {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ViewRequirement::AssertViewUuid(x) => x.validate(),
        }
    }
}

/// AssertViewUuid : The view UUID must match the requirement's `uuid`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct AssertViewUuid {
    #[serde(rename = "uuid")]
    pub uuid: uuid::Uuid,
}

/// AssertCreate : The table must not already exist; used for create transactions
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertCreate {}

/// AssertTableUuid : The table UUID must match the requirement's `uuid`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertTableUuid {
    #[serde(rename = "uuid")]
    pub uuid: uuid::Uuid,
}

/// AssertRefSnapshotId : The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertRefSnapshotId {
    #[serde(rename = "ref")]
    pub r#ref: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

/// AssertLastAssignedFieldId : The table's last assigned column id must match the requirement's `last-assigned-field-id`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertLastAssignedFieldId {
    #[serde(rename = "last-assigned-field-id")]
    pub last_assigned_field_id: i32,
}

/// AssertCurrentSchemaId : The table's current schema id must match the requirement's `current-schema-id`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertCurrentSchemaId {
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
}

/// AssertLastAssignedPartitionId : The table's last assigned partition id must match the requirement's `last-assigned-partition-id`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertLastAssignedPartitionId {
    #[serde(rename = "last-assigned-partition-id")]
    pub last_assigned_partition_id: i32,
}

/// AssertDefaultSpecId : The table's default spec id must match the requirement's `default-spec-id`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertDefaultSpecId {
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
}

/// AssertDefaultSortOrderId : The table's default sort order id must match the requirement's `default-sort-order-id`
#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssertDefaultSortOrderId {
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
}
