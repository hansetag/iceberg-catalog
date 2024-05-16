use http::StatusCode;

use crate::catalog::{TableIdent, TableRequirement, TableUpdate};
use crate::spec::{Schema, SortOrder, TableMetadata, TableMetadataBuilder, UnboundPartitionSpec};

use super::{impl_into_response, ErrorModel, IcebergErrorResponse};

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
    /// An opaque token that allows clients to make use of pagination for list
    /// APIs (e.g. `ListTables`).
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
    pub config: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    pub table_changes: Vec<CommitTableRequest>,
}

impl_into_response!(LoadTableResult);
impl_into_response!(ListTablesResponse);
impl_into_response!(CommitTableResponse);

#[allow(clippy::module_name_repetitions)]
pub trait TableRequirementExt {
    /// Assert if requirements match the given metadata.
    ///
    /// # Errors
    /// Fails if the requirements are not met.
    fn assert(&self, metadata: &TableMetadata, is_staged: bool)
        -> Result<(), IcebergErrorResponse>;
}

impl TableRequirementExt for TableRequirement {
    #[allow(clippy::too_many_lines)]
    fn assert(&self, metadata: &TableMetadata, exists: bool) -> Result<(), IcebergErrorResponse> {
        match self {
            TableRequirement::NotExist => {
                if exists {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::CONFLICT.as_u16())
                        .message("assert-create Table Requirement violated".to_string())
                        .r#type("TableRequirementNotExist".to_string())
                        .build()
                        .into());
                }
            }
            TableRequirement::UuidMatch { uuid } => {
                if &metadata.uuid() != uuid {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::CONFLICT.as_u16())
                        .message("assert-table-uuid Table Requirement violated".to_string())
                        .r#type("TableRequirementUuidMatch".to_string())
                        .stack(Some(vec![format!(
                            "Expected: {uuid}, Found: {}",
                            metadata.uuid()
                        )]))
                        .build()
                        .into());
                }
            }
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: _,
            } => {
                unimplemented!("TableRequirement::CurrentSchemaIdMatch")
            }
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: _,
            } => {
                unimplemented!("TableRequirement::DefaultSortOrderIdMatch")
            }
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref = metadata.refs.get(r#ref).ok_or(
                        ErrorModel::builder()
                            .code(StatusCode::NOT_FOUND.as_u16())
                            .message("Snapshot reference not found".to_string())
                            .r#type("TableRequirementRefSnapshotIdMatch".to_string())
                            .build(),
                    )?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(ErrorModel::builder()
                            .code(StatusCode::CONFLICT.as_u16())
                            .message(
                                "assert-ref-snapshot-id Branch points to different snapshot."
                                    .to_string(),
                            )
                            .r#type("TableRequirementRefSnapshotIdMatch".to_string())
                            .stack(Some(vec![format!(
                                "Expected: {snapshot_id}, Found: {}",
                                snapshot_ref.snapshot_id
                            )]))
                            .build()
                            .into());
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::CONFLICT.as_u16())
                        .message("assert-ref-snapshot-id Branch points a snapshot.".to_string())
                        .r#type("TableRequirementRefSnapshotExists".to_string())
                        .stack(Some(vec![format!(
                            "Expected: None, Found: {}",
                            metadata.refs.get(r#ref).unwrap().snapshot_id
                        )]))
                        .build()
                        .into());
                }
            }
            TableRequirement::DefaultSpecIdMatch { default_spec_id: _ } => {
                unimplemented!("TableRequirement::DefaultSpecIdMatch")
            }
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: _,
            } => {
                unimplemented!("TableRequirement::LastAssignedPartitionIdMatch")
            }
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id,
            } => {
                // ToDo: Check why requirement uses i64 for last_assigned_field_id
                // and metadata i32
                let last_column_id: i64 = metadata.last_column_id.into();
                if &last_column_id != last_assigned_field_id {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::CONFLICT.as_u16())
                        .message(
                            "assert-last-assigned-field-id Table Requirement violated".to_string(),
                        )
                        .r#type("TableRequirementLastAssignedFieldIdMatch".to_string())
                        .stack(Some(vec![format!(
                            "Expected: {last_assigned_field_id}, Found: {}",
                            metadata.last_column_id
                        )]))
                        .build()
                        .into());
                }
            }
        };
        Ok(())
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait TableUpdateExt {
    /// Apply the update to the given metadata builder.
    ///
    /// # Errors
    /// Fails if the update cannot be applied.
    /// For more details, check the docs of the `TableMetadataBuilder`
    fn apply(
        self,
        builder: &mut TableMetadataBuilder,
    ) -> Result<&mut TableMetadataBuilder, ErrorModel>;
}

impl TableUpdateExt for TableUpdate {
    fn apply(
        self,
        builder: &mut TableMetadataBuilder,
    ) -> Result<&mut TableMetadataBuilder, ErrorModel> {
        match self {
            TableUpdate::AssignUuid { uuid } => {
                builder.assign_uuid(uuid)?;
            }
            TableUpdate::UpgradeFormatVersion { format_version } => {
                builder.upgrade_format_version(format_version)?;
            }
            TableUpdate::RemoveProperties { removals } => {
                builder.remove_properties(&removals)?;
            }
            TableUpdate::SetProperties { updates } => {
                builder.set_properties(updates)?;
            }
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            } => {
                builder.add_schema(schema, last_column_id)?;
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                builder.set_current_schema(schema_id)?;
            }
            TableUpdate::SetDefaultSpec { spec_id } => {
                builder.set_default_partition_spec(spec_id)?;
            }
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                // ToDo: Check why TableUpdate uses i64 for sort_order_id
                // and metadata i32
                builder.set_default_sort_order(sort_order_id.into())?;
            }
            TableUpdate::AddSpec { spec } => {
                builder.add_partition_spec(spec)?;
            }
            TableUpdate::AddSortOrder { sort_order } => {
                builder.add_sort_order(sort_order)?;
            }
            TableUpdate::SetLocation { location } => {
                builder.set_location(location)?;
            }
            TableUpdate::AddSnapshot { snapshot } => {
                builder.add_snapshot(snapshot)?;
            }
            TableUpdate::RemoveSnapshots { snapshot_ids: _ } => {
                // ToDo: Implement
                unimplemented!("TableUpdate::RemoveSnapshots")
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                builder.set_snapshot_ref(ref_name, reference)?;
            }
            TableUpdate::RemoveSnapshotRef { ref_name: _ } => {
                // ToDo: Implement
                unimplemented!("TableUpdate::RemoveSnapshotRef")
            }
        }
        Ok(builder)
    }
}

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
