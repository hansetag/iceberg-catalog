use std::cmp::max;
use std::collections::HashSet;
use std::{collections::HashMap, vec};

use http::StatusCode;
use iceberg::spec::{
    FormatVersion, PartitionField, PartitionSpec, PartitionSpecRef, Schema, SchemaRef, Snapshot,
    SnapshotLog, SnapshotReference, SortOrder, SortOrderRef, TableMetadata, UnboundPartitionSpec,
    DEFAULT_SORT_ORDER_ID, DEFAULT_SPEC_ID, MAIN_BRANCH,
};
use iceberg::TableUpdate;
use uuid::Uuid;

use crate::catalog::rest::ErrorModel;
use crate::spec::partition_binder::PartitionSpecBinder;

// ToDo: Migrate to schema impl
pub(crate) trait SchemaExt {
    fn is_same_schema(&self, other: &SchemaRef) -> bool;
}

impl SchemaExt for Schema {
    fn is_same_schema(&self, other: &SchemaRef) -> bool {
        self.as_struct().eq(other.as_struct())
            && self.identifier_field_ids().eq(other.identifier_field_ids())
    }
}

// ToDo: Migrate to PartitionSpec impl
// ToDo: Move to last_assigned_field_id impl in PartitionSpec
/// Returns true if this spec is equivalent to the other, with partition field ids ignored. That
/// is, if both specs have the same number of fields, field order, field name, source columns, and
/// transforms.
trait PartitionSpecExt {
    fn compatible_with(&self, other: &PartitionSpec) -> bool;
    fn last_assigned_field_id(&self) -> i32;
}

impl PartitionSpecExt for PartitionSpec {
    fn compatible_with(&self, other: &PartitionSpec) -> bool {
        if self.eq(other) {
            return true;
        }

        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (this_field, other_field) in self.fields.iter().zip(&other.fields) {
            if this_field.source_id != other_field.source_id
                || this_field.transform.to_string() != other_field.transform.to_string()
                || this_field.name != other_field.name
            {
                return false;
            }
        }

        true
    }

    fn last_assigned_field_id(&self) -> i32 {
        self.fields
            .iter()
            .map(|field| field.field_id)
            .max()
            .unwrap_or(PartitionSpecBinder::UNPARTITIONED_LAST_ASSIGNED_ID)
    }
}

type Result<T> = std::result::Result<T, ErrorModel>;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TableMetadataAggregate {
    metadata: TableMetadata,
    changes: Vec<TableUpdate>,
    last_added_schema_id: Option<i32>,
    last_added_spec_id: Option<i32>,
    last_added_order_id: Option<i64>,
}

impl TableMetadataAggregate {
    const PARTITION_DATA_ID_START: i32 = 1000;
    const LAST_ADDED_I32: i32 = -1;
    const LAST_ADDED_I64: i64 = -1;
    const RESERVED_PROPERTIES: [&'static str; 9] = [
        "format-version",
        "uuid",
        "snapshot-count",
        "current-snapshot-summary",
        "current-snapshot-id",
        "current-snapshot-timestamp-ms",
        "current-schema",
        "default-partition-spec",
        "default-sort-order",
    ];

    #[must_use]
    /// Creates a new table metadata builder.
    pub fn new(location: String, schema: Schema) -> Self {
        // ToDo: Assign fresh IDs?
        // https://github.com/apache/iceberg/blob/6a594546b06df9fb75dd7e9713a8dc173e67c870/core/src/main/java/org/apache/iceberg/TableMetadata.java#L119
        let schema_id = schema.schema_id();
        Self {
            metadata: TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: Uuid::now_v7(),
                location: location.clone(),
                last_sequence_number: 0,
                last_updated_ms: chrono::Utc::now().timestamp_millis(),
                last_column_id: schema.highest_field_id(),
                current_schema_id: schema_id,
                schemas: HashMap::from_iter(vec![(schema.schema_id(), schema.clone().into())]),
                partition_specs: HashMap::new(),
                default_spec_id: DEFAULT_SPEC_ID - 1,
                last_partition_id: Self::PARTITION_DATA_ID_START - 1,
                properties: HashMap::new(),
                current_snapshot_id: None,
                snapshots: HashMap::new(),
                snapshot_log: vec![],
                sort_orders: HashMap::new(),
                metadata_log: vec![],
                default_sort_order_id: DEFAULT_SORT_ORDER_ID - 1,
                refs: HashMap::default(),
            },
            changes: vec![
                TableUpdate::AddSchema {
                    schema,
                    last_column_id: None,
                },
                TableUpdate::SetLocation { location },
            ],
            last_added_schema_id: Some(schema_id),
            last_added_spec_id: None,
            last_added_order_id: None,
        }
    }

    /// Creates a new table metadata builder from the given table metadata.
    #[must_use]
    pub fn new_from_metadata(origin: TableMetadata) -> Self {
        Self {
            metadata: origin,
            changes: Vec::default(),
            last_added_schema_id: None,
            last_added_spec_id: None,
            last_added_order_id: None,
        }
    }

    /// Changes uuid of table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn assign_uuid(&mut self, uuid: Uuid) -> Result<&mut Self> {
        if self.metadata.table_uuid != uuid {
            self.metadata.table_uuid = uuid;
            self.changes.push(TableUpdate::AssignUuid { uuid });
        }

        Ok(self)
    }

    /// Upgrade `FormatVersion`. Downgrades are not allowed.
    ///
    /// # Errors
    /// - Cannot downgrade `format_version` from V2 to V1.
    pub fn upgrade_format_version(&mut self, format_version: FormatVersion) -> Result<&mut Self> {
        let new_version = match format_version {
            FormatVersion::V1 => match self.metadata.format_version {
                FormatVersion::V1 => FormatVersion::V1,
                FormatVersion::V2 => {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::CONFLICT.into())
                        .message("Cannot downgrade FormatVersion from V2 to V1")
                        .r#type("FormatVersionNoDowngrade")
                        .build())
                }
            },
            FormatVersion::V2 => FormatVersion::V2,
        };

        self.metadata.format_version = new_version;
        self.changes.push(TableUpdate::UpgradeFormatVersion {
            format_version: new_version,
        });

        Ok(self)
    }

    //// Remove a property from the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn remove_properties(&mut self, properties: &[String]) -> Result<&mut Self> {
        for property in properties {
            self.metadata.properties.remove(property);
        }

        self.changes.push(TableUpdate::RemoveProperties {
            removals: properties.to_vec(),
        });

        Ok(self)
    }

    /// Set properties
    ///
    /// # Errors
    /// None yet.
    pub fn set_properties(&mut self, properties: HashMap<String, String>) -> Result<&mut Self> {
        let reserved_props = Self::RESERVED_PROPERTIES
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<HashSet<String>>();
        if properties
            .iter()
            .any(|(prop_name, _)| reserved_props.contains(prop_name))
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Table properties should not contain reserved properties")
                .r#type("FailedToSetProperties")
                .build());
        }

        self.metadata.properties.extend(properties.clone());
        self.changes.push(TableUpdate::SetProperties {
            updates: properties,
        });

        Ok(self)
    }

    /// Set the location of the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn set_location(&mut self, location: String) -> Result<&mut Self> {
        if self.metadata.location != location {
            self.changes.push(TableUpdate::SetLocation {
                location: location.clone(),
            });
            self.metadata.location = location;
        }

        Ok(self)
    }

    /// Add a schema to the table metadata.
    ///
    /// If `new_last_column_id` is not provided, the highest field id in the schema is used.
    ///
    /// # Errors
    /// - Last column id is lower than the current last column id.
    /// - Schema ID already exists.
    pub fn add_schema(
        &mut self,
        schema: Schema,
        new_last_column_id: Option<i32>,
    ) -> Result<&mut Self> {
        let new_last_column_id = new_last_column_id.unwrap_or(schema.highest_field_id());
        if new_last_column_id < self.metadata.last_column_id {
            return Err(ErrorModel::builder()
                .message(format!(
                    "Invalid last column id {}, must be >= {}",
                    new_last_column_id, self.metadata.last_column_id
                ))
                .r#type("LastColumnIdTooLow")
                .code(StatusCode::CONFLICT.into())
                .build());
        }

        let new_schema_id = self.reuse_or_create_new_schema_id(&schema);
        let schema_found = self.metadata.schemas.contains_key(&new_schema_id);

        if schema_found && new_last_column_id == self.metadata.last_column_id {
            self.last_added_schema_id = Some(new_schema_id);

            return Ok(self);
        }

        let schema = if new_schema_id == schema.schema_id() {
            schema
        } else {
            Schema::into_builder(schema)
                .with_schema_id(new_schema_id)
                .build()
                .map_err(|e| {
                    ErrorModel::builder()
                        .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                        .message("Failed to assign new schema id")
                        .r#type("FailedToAssignSchemaId")
                        .stack(Some(vec![e.to_string()]))
                        .build()
                })?
        };

        self.metadata.last_column_id = new_last_column_id;

        if !schema_found {
            self.metadata
                .schemas
                .insert(schema.schema_id(), schema.clone().into());
        }
        self.changes.push(TableUpdate::AddSchema {
            schema,
            last_column_id: Some(new_last_column_id),
        });

        self.last_added_schema_id = Some(new_schema_id);

        Ok(self)
    }

    fn reuse_or_create_new_schema_id(&self, new_schema: &Schema) -> i32 {
        self.metadata
            .schemas
            .iter()
            .find_map(|(id, schema)| new_schema.is_same_schema(schema).then_some(*id))
            .unwrap_or_else(|| self.get_highest_schema_id() + 1)
    }

    fn get_highest_schema_id(&self) -> i32 {
        *self
            .metadata
            .schemas
            .keys()
            .max()
            .unwrap_or(&self.metadata.current_schema_id)
    }

    /// Set the current schema id.
    /// If this `schema_id` does not exist yet, it must be added before
    /// `build` is called. If "-1" is specified, the highest added `schema_id` added during this
    /// build is used.
    ///
    /// # Errors
    /// - Current schema already set.
    pub fn set_current_schema(&mut self, schema_id: i32) -> Result<&mut Self> {
        if schema_id == Self::LAST_ADDED_I32 {
            return if let Some(id) = self.last_added_schema_id {
                self.set_current_schema(id)
            } else {
                Err(ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message("Cannot set last added schema: no schema has been added.")
                    .r#type("CurrentSchemaNotAdded")
                    .build())
            };
        }

        if schema_id == self.metadata.current_schema_id {
            return Ok(self);
        }

        let Some(schema) = self.metadata.schemas.get(&schema_id) else {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(format!(
                    "Cannot set current schema to schema with unknown Id: '{schema_id}'"
                ))
                .r#type("CurrentSchemaNotFound")
                .build());
        };

        // rebuild all the partition specs and sort orders for the new current schema
        self.metadata.partition_specs = self
            .metadata
            .partition_specs
            .values()
            .map(|spec| {
                PartitionSpecBinder::new(
                    schema.clone(),
                    spec.spec_id,
                    // No new fields are assigned anyway, so we can pass None here.
                    None,
                )
                .update_spec_schema((**spec).clone())
            })
            .collect::<Result<Vec<PartitionSpec>>>()?
            .into_iter()
            .map(|spec| (spec.spec_id, spec.into()))
            .collect::<HashMap<i32, PartitionSpecRef>>();

        self.metadata.sort_orders = self
            .metadata
            .sort_orders
            .values()
            .cloned()
            .map(|sort_order| {
                SortOrder::builder()
                    .with_order_id(sort_order.order_id)
                    .with_fields(sort_order.fields.clone())
                    .build_unbound()
                    .map_err(|e| {
                        ErrorModel::builder()
                            .message(e.message())
                            .code(StatusCode::CONFLICT.into())
                            .r#type(e.kind().into_static())
                            .stack(Some(vec![e.to_string()]))
                            .build()
                    })
            })
            .collect::<Result<Vec<SortOrder>>>()?
            .into_iter()
            .map(|sort_order| (sort_order.order_id, sort_order.into()))
            .collect::<HashMap<i64, SortOrderRef>>();

        self.metadata.current_schema_id = schema_id;

        if self.last_added_schema_id == Some(schema_id) {
            self.changes.push(TableUpdate::SetCurrentSchema {
                schema_id: Self::LAST_ADDED_I32,
            });
        } else {
            self.changes
                .push(TableUpdate::SetCurrentSchema { schema_id });
        }

        Ok(self)
    }

    /// Add a partition spec to the table metadata.
    ///
    /// # Errors
    /// None yet. Fails during build if `spec` cannot be bound.
    pub fn add_partition_spec(&mut self, unbound_spec: UnboundPartitionSpec) -> Result<&mut Self> {
        if self.metadata.current_schema_id == Self::LAST_ADDED_I32 {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Cannot add partition spec before current schema has been set.")
                .r#type("AddPartitionSpecBeforeSchema")
                .build());
        }
        let mut spec = PartitionSpecBinder::new(
            self.get_current_schema()?.clone(),
            unbound_spec.spec_id.unwrap_or_default(),
            Some(self.metadata.last_partition_id),
        )
        .bind_spec(unbound_spec.clone())?;

        // No spec_id specified, we need to reuse or create a new one.
        let new_spec_id = self.reuse_or_create_new_spec_id(&spec);

        if self.metadata.partition_specs.contains_key(&new_spec_id) {
            self.last_added_spec_id = Some(new_spec_id);

            return Ok(self);
        }

        if self.metadata.format_version <= FormatVersion::V1
            && !Self::has_sequential_ids(&spec.fields)
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Spec does not use sequential IDs that are required in v1.")
                .r#type("FailedToBuildPartitionSpec")
                .build());
        }

        // We already checked compatibility in the binder.
        spec.spec_id = new_spec_id;
        let mut unbound_spec = unbound_spec;
        unbound_spec.spec_id = Some(new_spec_id);

        self.last_added_spec_id = Some(spec.spec_id);
        self.metadata.last_partition_id = max(
            self.metadata.last_partition_id,
            spec.last_assigned_field_id(),
        );

        self.metadata
            .partition_specs
            .insert(spec.spec_id, spec.clone().into());
        self.changes
            .push(TableUpdate::AddSpec { spec: unbound_spec });

        Ok(self)
    }

    /// If the spec already exists, use the same ID. Otherwise, use 1 more than the highest ID.
    fn reuse_or_create_new_spec_id(&self, new_spec: &PartitionSpec) -> i32 {
        self.metadata
            .partition_specs
            .iter()
            .find_map(|(id, old_spec)| new_spec.compatible_with(old_spec).then_some(*id))
            .unwrap_or_else(|| self.highest_spec_id() + 1)
    }

    fn highest_spec_id(&self) -> i32 {
        *self
            .metadata
            .partition_specs
            .keys()
            .max()
            .unwrap_or(&(DEFAULT_SPEC_ID - 1))
    }

    /// Set the default partition spec.
    ///
    /// # Errors
    /// None yet. Fails during build if the default spec does not exist.
    pub fn set_default_partition_spec(&mut self, spec_id: i32) -> Result<&mut Self> {
        if spec_id == Self::LAST_ADDED_I32 {
            return if let Some(id) = self.last_added_spec_id {
                self.set_default_partition_spec(id)
            } else {
                Err(ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message("Cannot set last added spec: no spec has been added.")
                    .r#type("FailedToSetDefaultPartitionSpec")
                    .build())
            };
        }

        if self.metadata.default_spec_id == spec_id {
            return Ok(self);
        }

        self.metadata.default_spec_id = spec_id;

        if self.last_added_spec_id == Some(spec_id) {
            self.changes.push(TableUpdate::SetDefaultSpec {
                spec_id: Self::LAST_ADDED_I32,
            });
        } else {
            self.changes.push(TableUpdate::SetDefaultSpec { spec_id });
        }

        Ok(self)
    }

    /// Add a sort order to the table metadata.
    ///
    /// # Errors
    /// - Sort Order ID to add already exists.
    pub fn add_sort_order(&mut self, sort_order: SortOrder) -> Result<&mut Self> {
        let new_order_id = self.reuse_or_create_new_sort_id(&sort_order);
        if self.metadata.sort_orders.contains_key(&new_order_id) {
            self.last_added_order_id = Some(new_order_id);

            return Ok(self);
        }

        if self.metadata.current_schema_id == Self::LAST_ADDED_I32 {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Cannot add sort order before current schema has been set.")
                .r#type("AddSortOrderBeforeSchema")
                .build());
        }

        let schema = self.get_current_schema()?.clone().as_ref().clone();
        let mut sort_order = SortOrder::builder()
            .with_order_id(sort_order.order_id)
            .with_fields(sort_order.fields)
            .build(schema)
            .map_err(|e| {
                ErrorModel::builder()
                    .message("Failed to bind 'SortOrder'")
                    .code(StatusCode::CONFLICT.into())
                    .r#type("FailedToBindSortOrder")
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?;

        sort_order.order_id = self.reuse_or_create_new_sort_id(&sort_order);

        self.last_added_order_id = Some(sort_order.order_id);
        self.metadata
            .sort_orders
            .insert(sort_order.order_id, sort_order.clone().into());
        self.changes.push(TableUpdate::AddSortOrder { sort_order });

        Ok(self)
    }

    fn reuse_or_create_new_sort_id(&self, new_sort_order: &SortOrder) -> i64 {
        if new_sort_order.is_unsorted() {
            return SortOrder::unsorted_order().order_id;
        }

        self.metadata
            .sort_orders
            .iter()
            .find_map(|(id, sort_order)| {
                sort_order.fields.eq(&new_sort_order.fields).then_some(*id)
            })
            .unwrap_or_else(|| self.highest_sort_id() + 1)
    }

    fn highest_sort_id(&self) -> i64 {
        *self
            .metadata
            .sort_orders
            .keys()
            .max()
            .unwrap_or(&DEFAULT_SORT_ORDER_ID)
    }

    /// Set the default sort order.
    ///
    /// # Errors
    /// - Default Sort Order already set.
    pub fn set_default_sort_order(&mut self, order_id: i64) -> Result<&mut Self> {
        if order_id == i64::from(Self::LAST_ADDED_I32) {
            return if let Some(id) = self.last_added_order_id {
                self.set_default_sort_order(id)
            } else {
                Err(ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message("Cannot set last added sort order: no sort order has been added.")
                    .r#type("FailedToSetDefaultSortOrderSpec")
                    .build())
            };
        }

        if self.metadata.default_sort_order_id == order_id {
            return Ok(self);
        }

        self.metadata.default_sort_order_id = order_id;

        if self.last_added_order_id == Some(order_id) {
            self.changes.push(TableUpdate::SetDefaultSortOrder {
                sort_order_id: Self::LAST_ADDED_I64,
            });
        } else {
            self.changes.push(TableUpdate::SetDefaultSortOrder {
                sort_order_id: order_id,
            });
        };

        Ok(self)
    }

    /// Add a snapshot to the table metadata.
    ///
    /// # Errors
    /// - Only one snapshot update is allowed per commit.
    pub fn add_snapshot(&mut self, snapshot: Snapshot) -> Result<&mut Self> {
        if self.metadata.schemas.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a schema is added")
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforeSchema")
                .build());
        }

        if self.metadata.partition_specs.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a partition spec is added")
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforePartitionSpec")
                .build());
        }

        if self.metadata.sort_orders.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a sort order is added")
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforeSortOrder")
                .build());
        }

        if self
            .metadata
            .snapshots
            .contains_key(&snapshot.snapshot_id())
        {
            return Err(ErrorModel::builder()
                .message(format!(
                    "Snapshot already exists for: '{}'",
                    snapshot.snapshot_id()
                ))
                .code(StatusCode::CONFLICT.into())
                .r#type("SnapshotAlreadyExists")
                .build());
        }

        if self.metadata.format_version != FormatVersion::V1
            && snapshot.sequence_number() <= self.metadata.last_sequence_number
            && snapshot.parent_snapshot_id().is_some()
        {
            return Err(ErrorModel::builder()
                .message(format!(
                    "Cannot add snapshot with sequence number {} older than last sequence number {}",
                    snapshot.sequence_number(),
                    self.metadata.last_sequence_number
                ))
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotOlderThanLast")
                .build()
            );
        }

        self.changes.push(TableUpdate::AddSnapshot {
            snapshot: snapshot.clone(),
        });

        self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
        self.metadata.last_sequence_number = snapshot.sequence_number();
        self.metadata
            .snapshots
            .insert(snapshot.snapshot_id(), snapshot.into());

        Ok(self)
    }

    //// Remove snapshots by its ids from the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn remove_snapshots(&mut self, snapshot_ids: &[i64]) -> Result<&mut Self> {
        for snapshot_id in snapshot_ids {
            self.metadata.snapshots.remove(snapshot_id);
        }

        self.changes.push(TableUpdate::RemoveSnapshots {
            snapshot_ids: snapshot_ids.to_vec(),
        });

        for (snapshot_name, snapshot_ref) in self.metadata.refs.clone() {
            if self
                .metadata
                .snapshots
                .contains_key(&snapshot_ref.snapshot_id)
            {
                self.remove_snapshot_by_ref(&snapshot_name)?;
            }
        }

        Ok(self)
    }

    /// Set a reference to a snapshot.
    ///
    /// # Errors
    /// Reference already set in this commit.
    pub fn set_snapshot_ref(
        &mut self,
        ref_name: String,
        reference: SnapshotReference,
    ) -> Result<&mut Self> {
        if self
            .metadata
            .refs
            .get(&ref_name)
            .is_some_and(|snap_ref| snap_ref.eq(&reference))
        {
            return Ok(self);
        }

        let Some(snapshot) = self.metadata.snapshots.get(&reference.snapshot_id) else {
            return Err(ErrorModel::builder()
                .message(format!(
                    "Cannot set '{ref_name}' to unknown snapshot: '{}'",
                    reference.snapshot_id
                ))
                .code(StatusCode::CONFLICT.into())
                .r#type("SetReferenceToUnknownSnapshot")
                .build());
        };

        // Update last_updated_ms to the exact timestamp of the snapshot if it was added in this commit
        let is_added_snapshot = self.changes.iter().any(|update| {
            matches!(update, TableUpdate::AddSnapshot { snapshot: snap } if snap.snapshot_id() == snapshot.snapshot_id())
        });
        if is_added_snapshot {
            self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
        }

        if ref_name == MAIN_BRANCH {
            self.metadata.current_snapshot_id = Some(snapshot.snapshot_id());
            self.metadata.last_updated_ms = if self.metadata.last_updated_ms == 0 {
                chrono::Utc::now().timestamp_millis()
            } else {
                self.metadata.last_updated_ms
            };

            self.metadata.snapshot_log.push(SnapshotLog {
                snapshot_id: snapshot.snapshot_id(),
                timestamp_ms: self.metadata.last_updated_ms,
            });
        }

        self.changes.push(TableUpdate::SetSnapshotRef {
            ref_name: ref_name.clone(),
            reference: reference.clone(),
        });
        self.metadata.refs.insert(ref_name, reference);

        Ok(self)
    }

    //// Removes snapshot by its name from the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn remove_snapshot_by_ref(&mut self, snapshot_ref: &str) -> Result<&mut Self> {
        if snapshot_ref == MAIN_BRANCH {
            self.metadata.current_snapshot_id = Some(i64::from(Self::LAST_ADDED_I32));
            self.metadata.snapshot_log.clear();
        }

        if self.metadata.refs.remove(snapshot_ref).is_some() {
            self.changes.push(TableUpdate::RemoveSnapshotRef {
                ref_name: snapshot_ref.to_owned(),
            });
        }

        Ok(self)
    }

    /// Build the table metadata.
    ///
    /// # Errors
    /// - No schema has been added.
    /// - Default sort order is set to -1 but no sort order has been added.
    /// - Default partition spec is set to -1 but no partition spec has been added.
    /// - Ref is set to an unknown snapshot.
    pub fn build(mut self) -> Result<TableMetadata> {
        if self.metadata.current_schema_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without current_schema_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("CurrentSchemaIdMissing")
                .build());
        }

        if self.metadata.last_column_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without last_column_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("LastColumnIdMissing")
                .build());
        }

        // It hasn't been changed at all
        if self.metadata.default_spec_id == DEFAULT_SPEC_ID - 1 {
            self.metadata.default_spec_id = DEFAULT_SPEC_ID;
            self.metadata
                .partition_specs
                .entry(DEFAULT_SPEC_ID)
                .or_insert_with(|| {
                    PartitionSpec {
                        spec_id: DEFAULT_SPEC_ID,
                        fields: vec![],
                    }
                    .into()
                });
        }

        if self.metadata.default_sort_order_id == (DEFAULT_SORT_ORDER_ID - 1) {
            let unsorted = SortOrder::unsorted_order();
            self.metadata.default_sort_order_id = DEFAULT_SORT_ORDER_ID;
            self.metadata
                .sort_orders
                .entry(DEFAULT_SORT_ORDER_ID)
                .or_insert_with(|| unsorted.into());
        }

        Ok(self.metadata)
    }

    fn get_current_schema(&self) -> Result<&SchemaRef> {
        let err = Self::throw_err(
            format!(
                "Failed to get current schema: '{}'",
                self.metadata.current_schema_id
            ),
            "FailedToGetCurrentSchema",
        );

        self.metadata
            .schemas
            .get(&self.metadata.current_schema_id)
            .ok_or_else(err)
    }

    #[allow(clippy::cast_sign_loss)]
    fn has_sequential_ids(fields: &[PartitionField]) -> bool {
        for (index, field) in fields.iter().enumerate() {
            if (field.field_id as usize).ne(&(Self::PARTITION_DATA_ID_START as usize + index)) {
                return false;
            }
        }

        true
    }

    #[inline]
    fn throw_err<A: Into<String>, B: Into<String>>(
        msg: A,
        r#type: B,
    ) -> impl FnOnce() -> ErrorModel {
        || -> ErrorModel {
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(msg)
                .r#type(r#type)
                .build()
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use iceberg::spec::Type::{self, Primitive};
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, SortDirection, SortField, Transform,
        UnboundPartitionField,
    };

    lazy_static::lazy_static! {
        static ref SCHEMA: Schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();

        static ref PARTITION_SPEC: PartitionSpec = PartitionSpec::builder()
        .with_spec_id(1)
        .with_partition_field(PartitionField {
            name: "id".to_string(),
            transform: Transform::Identity,
            source_id: 1,
            field_id: 1000,
        })
        .build()
        .unwrap();

        static ref TABLE_METADATA: TableMetadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://b/wh/data.db/table".to_string(),
            last_updated_ms: chrono::Utc::now().timestamp_millis(),
            last_column_id: 1,
            schemas: HashMap::from_iter(vec![(1, Arc::new(SCHEMA.clone()))]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(1, PARTITION_SPEC.clone().into())]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::from_iter(vec![(
                "commit.retry.num-retries".to_string(),
                "1".to_string(),
            )]),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
        };
    }

    fn get_allowed_props() -> HashMap<String, String> {
        (1..=9)
            .map(|index| (format!("key{index}"), format!("value{index}")))
            .collect()
    }

    #[test]
    fn default_order_id_is_unsorted() {
        let unsorted = SortOrder::unsorted_order();
        assert_eq!(unsorted.order_id, DEFAULT_SORT_ORDER_ID);
    }

    #[test]
    fn get_full_aggregate() {
        let new_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();

        let new_spec = UnboundPartitionSpec::builder()
            .with_spec_id(2)
            .with_fields(vec![UnboundPartitionField {
                name: "name".to_string(),
                transform: Transform::Identity,
                source_id: 2,
                partition_id: None,
            }])
            .build()
            .unwrap();

        let new_sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_fields(vec![SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }])
            .build_unbound()
            .unwrap();

        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());

        let new_uuid = Uuid::now_v7();
        aggregate
            .assign_uuid(new_uuid)
            .expect("Cannot assign uuid.")
            .add_schema(new_schema, None)
            .expect("Cannot set schema.")
            .set_current_schema(-1)
            .expect("Cannot set current schema.")
            .upgrade_format_version(FormatVersion::V2)
            .expect("Cannot set format version.")
            .add_partition_spec(new_spec)
            .expect("Cannot set partition spec.")
            .set_default_partition_spec(-1)
            .expect("Cannot set default partition spec.")
            .add_sort_order(new_sort_order)
            .expect("Cannot set sort order.")
            .set_default_sort_order(-1)
            .expect("Cannot set default sort order.")
            .set_location("location".to_owned())
            .expect("Cannot set location")
            .set_properties(get_allowed_props())
            .expect("Cannot set properties");

        let metadata = aggregate.build().expect("Cannot build metadata.");
        assert_eq!(metadata.format_version, FormatVersion::V2);
        assert_eq!(metadata.table_uuid, new_uuid);
        assert_eq!(metadata.location, "location");
        assert_eq!(metadata.schemas.len(), 2);
        assert_eq!(metadata.partition_specs.len(), 2);
        assert_eq!(metadata.sort_orders.len(), 1);
    }

    #[test]
    fn downgrade_version() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        assert!(aggregate.upgrade_format_version(FormatVersion::V1).is_err());
    }

    #[test]
    fn same_version() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        assert!(aggregate.upgrade_format_version(FormatVersion::V2).is_ok());
    }

    #[test]
    fn add_schema() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        let new_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        assert!(aggregate.add_schema(new_schema, None).is_ok());
        let metadata = aggregate.build().expect("Cannot build metadata.");
        assert_eq!(metadata.schemas.len(), 2);
        assert!(metadata.schemas.contains_key(&2));
        assert_eq!(metadata.current_schema_id, 1);
    }

    #[test]
    fn readd_schema_and_set_current_schema() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        let new_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        assert!(aggregate.add_schema(new_schema.clone(), None).is_ok());
        let metadata = aggregate.build().expect("Cannot build metadata.");
        let mut aggregate = TableMetadataAggregate::new_from_metadata(metadata);
        assert!(aggregate.add_schema(new_schema.clone(), None).is_ok());
        assert!(aggregate.set_current_schema(-1).is_ok());
        let metadata = aggregate.build().expect("Cannot build metadata.");
        assert_eq!(metadata.current_schema_id, 2);
        assert_eq!(metadata.current_schema(), &Arc::new(new_schema));
    }

    #[test]
    fn set_default_schema() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        let new_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        assert!(aggregate.add_schema(new_schema.clone(), None).is_ok());
        assert!(aggregate.set_current_schema(-1).is_ok());
        let metadata = aggregate.build().expect("Cannot build metadata.");
        assert_eq!(metadata.current_schema_id, 2);
        assert_eq!(metadata.current_schema(), &Arc::new(new_schema));
    }

    #[test]
    fn set_properties() {
        let forbidden_props = TableMetadataAggregate::RESERVED_PROPERTIES
            .iter()
            .enumerate()
            .map(|(index, prop)| ((*prop).to_string(), format!("value{index}")))
            .collect();
        let allowed_props = get_allowed_props();

        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());

        assert!(aggregate.set_properties(forbidden_props).is_err());
        assert!(aggregate.set_properties(allowed_props).is_ok());
    }

    #[test]
    fn cannot_add_schema_with_column_id_too_low() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());

        let schema_fields_1 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::String)).into(),
        ];

        let schema_fields_2 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
        ];

        let schema_1 = Schema::builder()
            .with_schema_id(2)
            .with_fields(schema_fields_1)
            .build()
            .expect("Cannot create schema mock");
        let schema_2 = Schema::builder()
            .with_schema_id(3)
            .with_fields(schema_fields_2)
            .build()
            .expect("Cannot create schema mock");

        aggregate
            .add_schema(schema_1, None)
            .expect("Cannot add new schema.");
        assert!(dbg!(aggregate.add_schema(schema_2, None)).is_err());
    }

    #[test]
    fn reuse_schema_id_if_schemas_is_eq() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());

        aggregate
            .add_schema(SCHEMA.clone(), None)
            .expect("Cannot add new schema.");
        assert!(aggregate.metadata.schemas.len().eq(&1));
    }

    #[test]
    fn schema_ids_should_increase_by_1() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());

        let fields_1 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "email", Primitive(PrimitiveType::String)).into(),
        ];
        let schema_1 = Schema::builder()
            .with_schema_id(0)
            .with_fields(fields_1)
            .build()
            .expect("Cannot create schema mock");

        let fields_2 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "email", Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "name", Primitive(PrimitiveType::String)).into(),
        ];

        let schema_2 = Schema::builder()
            .with_schema_id(1)
            .with_fields(fields_2)
            .build()
            .expect("Cannot create schema mock");

        aggregate
            .add_schema(schema_1, None)
            .unwrap()
            .set_current_schema(-1)
            .unwrap();
        let schema_id = aggregate.metadata.current_schema_id;
        aggregate.add_schema(schema_2, None).unwrap();

        assert!(aggregate.metadata.schemas.contains_key(&(schema_id + 1)));
        assert!(aggregate.metadata.last_column_id.eq(&4));
    }

    #[test]
    fn partition_spec_with_transform() {
        let mut aggregate = TableMetadataAggregate::new_from_metadata(TABLE_METADATA.clone());
        let new_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "ints",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(2)
            .with_fields(vec![UnboundPartitionField::builder()
                .source_id(2)
                .name("ints_bucket".to_string())
                .transform(iceberg::spec::Transform::Bucket(16))
                .build()])
            .build()
            .unwrap();
        aggregate
            .add_schema(new_schema.clone(), None)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .add_partition_spec(partition_spec)
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap();
        let metadata = aggregate.build().unwrap();
        assert_eq!(
            metadata.partition_specs[&2].fields[0].transform,
            iceberg::spec::Transform::Bucket(16)
        );
        assert_eq!(metadata.partition_specs[&2].fields[0].name, "ints_bucket");
        assert_eq!(metadata.partition_specs[&2].fields[0].source_id, 2);
        assert_eq!(metadata.default_spec_id, 2);
    }

    #[test]
    fn default_sort_order_and_partitioning() {
        // When building a new metadata, we test that the default partition spec and sort order are set to 0.
        // Setting the default partition spec and sort order increases compatibility with other languages.
        // I.e. java would request a "create table" statement without a "write-order": null, however fails deserialization
        // if the returned metadata does not contain one explicitly.
        let aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let metadata = aggregate.build().unwrap();
        assert_eq!(metadata.default_spec_id, 0);
        assert!(metadata
            .default_partition_spec()
            .unwrap()
            .fields
            .len()
            .eq(&0));
        assert_eq!(metadata.default_sort_order_id, 0);
        assert!(metadata.default_sort_order().unwrap().fields.len().eq(&0),);
    }

    #[test]
    fn first_partition_gets_id_0_and_field_999() {
        let mut aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let partition_spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![],
        };
        assert!(aggregate.add_partition_spec(partition_spec.clone()).is_ok());
        let metadata = aggregate.build().unwrap();

        assert_eq!(metadata.partition_specs[&0].fields.len(), 0);
        assert_eq!(metadata.default_spec_id, 0);
        assert_eq!(metadata.last_partition_id, 999);
    }

    #[test]
    fn second_partition_spec_gets_id_1_and_field_1000() {
        let aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let metadata = aggregate.build().unwrap();

        let mut aggregate = TableMetadataAggregate::new_from_metadata(metadata);
        let partition_spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![UnboundPartitionField {
                name: "id".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                partition_id: None,
            }],
        };
        assert!(aggregate.add_partition_spec(partition_spec.clone()).is_ok());
        assert!(aggregate.set_default_partition_spec(-1).is_ok());
        let metadata = aggregate.build().unwrap();

        assert_eq!(metadata.partition_specs[&1].fields.len(), 1);
        assert_eq!(metadata.default_spec_id, 1);
        assert_eq!(metadata.last_partition_id, 1000);
    }

    #[test]
    fn partition_last_field_id_respected() {
        let mut aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let partition_spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![UnboundPartitionField {
                name: "id".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                partition_id: None,
            }],
        };
        aggregate
            .add_partition_spec(partition_spec.clone())
            .unwrap();
        let metadata = aggregate.build().unwrap();

        let mut aggregate = TableMetadataAggregate::new_from_metadata(metadata);
        let new_schema = Schema::builder()
            .with_schema_id(-1)
            .with_fields(vec![Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let new_partition_spec = UnboundPartitionSpec {
            spec_id: None,
            fields: vec![UnboundPartitionField {
                name: "name".to_string(),
                transform: Transform::Identity,
                source_id: 2,
                partition_id: None,
            }],
        };

        aggregate
            .add_schema(new_schema, None)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .add_partition_spec(new_partition_spec)
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap();

        let metadata = aggregate.build().unwrap();
        assert_eq!(metadata.last_partition_id, 1001);
        assert_eq!(metadata.partition_specs[&1].fields[0].field_id, 1001);
        assert_eq!(metadata.default_spec_id, 1);
        assert_eq!(
            metadata.default_partition_spec().unwrap().fields[0].name,
            "name"
        );
    }

    #[test]
    fn add_unsort_order() {
        let mut aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let sort_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build_unbound()
            .unwrap();
        assert!(aggregate.add_sort_order(sort_order).is_ok());
        let metadata = aggregate.build().unwrap();

        assert_eq!(metadata.sort_orders[&0].fields.len(), 0);
        assert_eq!(metadata.default_sort_order_id, 0);
    }

    #[test]
    fn add_sort_order() {
        let mut aggregate = TableMetadataAggregate::new("foo".to_string(), SCHEMA.clone());
        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_fields(vec![SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }])
            .build_unbound()
            .unwrap();
        aggregate
            .add_sort_order(sort_order)
            .unwrap()
            .set_default_sort_order(-1)
            .unwrap();
        let metadata = aggregate.build().unwrap();

        assert_eq!(metadata.sort_orders[&1].fields.len(), 1);
        assert_eq!(metadata.default_sort_order_id, 1);
    }
}
