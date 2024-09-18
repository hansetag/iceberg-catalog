use std::str::FromStr as _;

use iceberg::{spec::TableMetadata, TableRequirement, TableUpdate};
use iceberg_ext::{
    catalog::rest::{TableRequirementExt as _, TableUpdateExt},
    configs::Location,
    spec::TableMetadataAggregate,
};

use crate::service_modules::{ErrorModel, Result};

/// Apply the commits to table metadata.
pub(super) fn apply_commit(
    metadata: TableMetadata,
    metadata_location: &Option<Location>,
    requirements: &[TableRequirement],
    updates: Vec<TableUpdate>,
) -> Result<TableMetadata> {
    // Check requirements
    requirements
        .iter()
        .map(|r| r.assert(&metadata, metadata_location.is_some()))
        .collect::<Result<Vec<_>>>()?;

    // Store data of current metadata to prevent disallowed changes
    let previous_location = Location::from_str(&metadata.location).map_err(|e| {
        ErrorModel::internal(
            format!("Invalid table location in DB: {e}"),
            "InvalidTableLocation",
            Some(Box::new(e)),
        )
    })?;
    let previous_uuid = metadata.uuid();
    let mut builder = TableMetadataAggregate::new_from_metadata(metadata);

    // Update!
    for update in updates {
        match &update {
            TableUpdate::AssignUuid { uuid } => {
                if uuid != &previous_uuid {
                    return Err(ErrorModel::bad_request(
                        "Cannot assign a new UUID",
                        "AssignUuidNotAllowed",
                        None,
                    )
                    .into());
                }
            }
            TableUpdate::SetLocation { location } => {
                if location != &previous_location.to_string() {
                    return Err(ErrorModel::bad_request(
                        "Cannot change table location",
                        "SetLocationNotAllowed",
                        None,
                    )
                    .into());
                }
            }
            _ => {
                TableUpdateExt::apply(update, &mut builder)?;
            }
        }
    }

    builder.build().map_err(Into::into)
}
