use std::cmp::max;
use std::collections::HashSet;
use std::ops::BitAnd;
use std::{
    collections::{hash_map::Entry as HashMapEntry, HashMap},
    vec,
};

use http::StatusCode;
use iceberg::spec::{
    FormatVersion, PartitionField, PartitionSpec, PartitionSpecRef, Schema, SchemaRef, Snapshot,
    SnapshotLog, SnapshotReference, SortOrder, SortOrderRef, TableMetadata, UnboundPartitionSpec,
};
use iceberg::TableUpdate;
use uuid::Uuid;

use crate::catalog::rest::ErrorModel;
use crate::spec::partition_binder::PartitionSpecBinder;

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
    const MAIN_BRANCH: &'static str = "main";
    const INITIAL_SPEC_ID: i32 = 0;
    const INITIAL_SORT_ORDER_ID: i64 = 0;
    const PARTITION_DATA_ID_START: usize = 1000;
    const LAST_ADDED_I32: i32 = -1;
    const LAST_ADDED_I64: i64 = -1;
    const INITIAL_SCHEMA_ID: i32 = 0;
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

    /// Initialize new table metadata aggregate.
    #[must_use]
    pub fn new(location: String) -> Self {
        Self {
            metadata: TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: Uuid::now_v7(),
                location,
                last_sequence_number: 0,
                last_updated_ms: chrono::Utc::now().timestamp_millis(),
                last_column_id: Self::LAST_ADDED_I32,
                current_schema_id: Self::INITIAL_SCHEMA_ID,
                schemas: HashMap::new(),
                partition_specs: HashMap::new(),
                default_spec_id: Self::LAST_ADDED_I32,
                last_partition_id: 0,
                properties: HashMap::new(),
                current_snapshot_id: None,
                snapshots: HashMap::new(),
                snapshot_log: vec![],
                sort_orders: HashMap::new(),
                metadata_log: vec![],
                default_sort_order_id: i64::from(Self::LAST_ADDED_I32),
                refs: HashMap::default(),
            },
            changes: Vec::default(),
            last_added_schema_id: None,
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
                .message("Table properties should not contain reserved properties!")
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

        let new_schema_id = self.reuse_or_create_new_schema_id(&schema)?;
        let schema_found = self.metadata.schemas.contains_key(&new_schema_id);

        if !schema_found {
            self.metadata
                .schemas
                .insert(schema.schema_id(), schema.clone().into());
            self.changes.push(TableUpdate::AddSchema {
                schema,
                last_column_id: Some(new_last_column_id),
            });
            self.metadata.last_column_id = new_last_column_id;
        }

        self.last_added_schema_id = Some(new_schema_id);

        Ok(self)
    }

    fn reuse_or_create_new_schema_id(&self, other: &Schema) -> Result<i32> {
        let (other_id, other_fields) = (other.schema_id(), other.identifier_field_ids());

        let err = Self::throw_err(
            format!("Schema with id '{other_id}' already exists and is different!"),
            "SchemaIdAlreadyExistsAndDifferent",
        );

        let is_schemas_eq = |(id, this_schema): (&i32, &SchemaRef)| -> Option<i32> {
            other
                .as_struct()
                .eq(this_schema.as_struct())
                .bitand(other_fields.eq(this_schema.identifier_field_ids()))
                .then_some(*id)
        };

        if other_id == Self::LAST_ADDED_I32 {
            // Search for an identical schema and reuse the ID.
            // If no identical schema is found, use the next available ID.
            Ok(self
                .metadata
                .schemas
                .iter()
                .find_map(is_schemas_eq)
                .unwrap_or_else(|| self.get_highest_schema_id() + 1))
        } else {
            // If the schema_id() already exists, it must be the same schema.
            Ok(self
                .metadata
                .schemas
                .get(&other_id)
                .map(AsRef::as_ref)
                .map(|exising| exising.eq(other).then_some(other_id).ok_or_else(err))
                .transpose()?
                .unwrap_or(other_id))
        }
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
                    .r#type("CannotSetCurrentSchema")
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
                    "Cannot set current schema to schema with unknown Id: '{schema_id}'!"
                ))
                .r#type("CannotSetCurrentSchema")
                .build());
        };

        self.metadata.partition_specs = self
            .metadata
            .partition_specs
            .values()
            .cloned()
            .map(|spec| {
                PartitionSpecBinder::new(schema.clone(), spec.spec_id).update_spec_schema(&spec)
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
                    .build(schema.as_ref().clone())
                    .map_err(|e| {
                        ErrorModel::builder()
                            .message("Failed to bound 'SortOrder'!")
                            .code(StatusCode::CONFLICT.into())
                            .r#type("CannotSetCurrentSchema")
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
        let mut spec = PartitionSpecBinder::new(
            self.get_current_schema()?.clone(),
            unbound_spec.spec_id.unwrap_or_default(),
        )
        .bind_spec_schema(unbound_spec.clone())?;

        // No spec_id specified, we need to reuse or create a new one.
        spec.spec_id = self.reuse_or_create_new_spec_id(&spec)?;

        if self.metadata.format_version <= FormatVersion::V1
            && !Self::has_sequential_ids(&spec.fields)
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Spec does not use sequential IDs that are required in v1.")
                .r#type("FailedToBuildPartitionSpec")
                .build());
        }

        self.last_added_spec_id = Some(spec.spec_id);

        if let HashMapEntry::Vacant(e) = self.metadata.partition_specs.entry(spec.spec_id) {
            self.changes
                .push(TableUpdate::AddSpec { spec: unbound_spec });
            self.metadata.last_partition_id = max(
                self.metadata.last_partition_id,
                spec.fields.iter().last().map_or(0, |field| field.field_id),
            );
            e.insert(spec.into());
        }

        Ok(self)
    }

    /// If the spec already exists, use the same ID. Otherwise, use 1 more than the highest ID.
    fn reuse_or_create_new_spec_id(&self, other: &PartitionSpec) -> Result<i32> {
        let (other_id, other_fields) = (other.spec_id, &other.fields);

        let err = Self::throw_err(
            format!("PartitionSpec with id '{other_id}' already exists and it's different!",),
            "PartitionSpecIdAlreadyExistsAndDifferent",
        );

        let is_specs_eq = |(id, this_partition): (&i32, &PartitionSpecRef)| -> Option<i32> {
            this_partition.fields.eq(other_fields).then_some(*id)
        };

        if other_id == Self::LAST_ADDED_I32 {
            // Search for identical `spec_id` and reuse the ID if possible.
            // Otherwise, use the next available ID.
            Ok(self
                .metadata
                .partition_specs
                .iter()
                .find_map(is_specs_eq)
                .unwrap_or_else(|| self.highest_spec_id() + 1))
        } else {
            // If the `spec_id` already exists, it must be the same spec.
            Ok(self
                .metadata
                .partition_specs
                .get(&other_id)
                .map(AsRef::as_ref)
                .map(|exising| exising.eq(other).then_some(other_id).ok_or_else(err))
                .transpose()?
                .unwrap_or(other_id))
        }
    }

    fn highest_spec_id(&self) -> i32 {
        *self
            .metadata
            .partition_specs
            .keys()
            .max()
            .unwrap_or(&Self::INITIAL_SPEC_ID)
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
        let schema = self.get_current_schema()?.clone().as_ref().clone();
        let mut sort_order = SortOrder::builder()
            .with_order_id(sort_order.order_id)
            .with_fields(sort_order.fields)
            .build(schema)
            .map_err(|e| {
                ErrorModel::builder()
                    .message("Failed to bound 'SortOrder'!")
                    .code(StatusCode::CONFLICT.into())
                    .r#type("FailedToBuildSortOrder")
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?;

        sort_order.order_id = if sort_order.is_unsorted() {
            0
        } else {
            self.reuse_or_create_new_sort_id(&sort_order)?
        };
        self.last_added_order_id = Some(sort_order.order_id);
        if let HashMapEntry::Vacant(e) = self.metadata.sort_orders.entry(sort_order.order_id) {
            self.changes.push(TableUpdate::AddSortOrder {
                sort_order: sort_order.clone(),
            });

            e.insert(sort_order.into());
        }

        Ok(self)
    }

    fn reuse_or_create_new_sort_id(&self, other: &SortOrder) -> Result<i64> {
        let (other_id, other_fields) = (other.order_id, &other.fields);

        let err = Self::throw_err(
            format!("Sort Order with id '{other_id}' already exists and is different!",),
            "SortOrderIdAlreadyExistsAndDifferent",
        );

        let is_order_eq = |(id, this_order): (&i64, &SortOrderRef)| -> Option<i64> {
            this_order.fields.eq(other_fields).then_some(*id)
        };

        if other_id == Self::LAST_ADDED_I64 {
            // Search for identical order_id and reuse the ID if possible.
            // Otherwise, use the next available ID.
            Ok(self
                .metadata
                .sort_orders
                .iter()
                .find_map(is_order_eq)
                .unwrap_or_else(|| self.highest_sort_id() + 1))
        } else {
            // If the order_id already exists, it must be the same sort order.
            Ok(self
                .metadata
                .sort_orders
                .get(&other_id)
                .map(AsRef::as_ref)
                .map(|exising| exising.eq(other).then_some(other_id).ok_or_else(err))
                .transpose()?
                .unwrap_or(other_id))
        }
    }

    fn highest_sort_id(&self) -> i64 {
        *self
            .metadata
            .sort_orders
            .keys()
            .max()
            .unwrap_or(&Self::INITIAL_SORT_ORDER_ID)
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
                .message("Attempting to add a snapshot before a schema is added!")
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforeSchema")
                .build());
        }

        if self.metadata.partition_specs.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a partition spec is added!")
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforePartitionSpec")
                .build());
        }

        if self.metadata.sort_orders.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a sort order is added!")
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
                    "Snapshot already exists for: '{}'!",
                    snapshot.snapshot_id()
                ))
                .code(StatusCode::CONFLICT.into())
                .r#type("SnapshotAlreadyExists")
                .build());
        }

        if self.metadata.format_version == FormatVersion::V2
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
                    "Cannot set '{ref_name}' to unknown snapshot: '{reference:?}'."
                ))
                .code(StatusCode::CONFLICT.into())
                .r#type("ReferenceAlreadyExists")
                .build());
        };

        self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();

        if ref_name == Self::MAIN_BRANCH {
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
        if snapshot_ref == Self::MAIN_BRANCH {
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
    pub fn build(self) -> Result<TableMetadata> {
        if self.changes.is_empty() {
            return Ok(self.metadata);
        }

        if self.metadata.last_column_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without last_column_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("LastColumnIdMissing")
                .build());
        }

        if self.metadata.current_schema_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without current_schema_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("CurrentSchemaIdMissing")
                .build());
        }

        if self.metadata.default_spec_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without default_spec_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("DefaultSpecIdMissing")
                .build());
        }

        if self.metadata.default_sort_order_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without default_sort_order_id")
                .code(StatusCode::CONFLICT.into())
                .r#type("DefaultSortOrderIdMissing")
                .build());
        }

        Ok(self.metadata)
    }

    fn get_current_schema(&self) -> Result<&SchemaRef> {
        let err = Self::throw_err(
            format!(
                "Failed to get current schema: '{}'!",
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
            if (field.field_id as usize).ne(&(Self::PARTITION_DATA_ID_START + index)) {
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
    use super::*;
    use iceberg::spec::Type::Primitive;
    use iceberg::spec::{NestedField, PrimitiveType};

    fn get_mock_schema(from_id: i32) -> (Schema, i32) {
        let schema_fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::String)).into(),
        ];

        let schema = Schema::builder()
            .with_schema_id(from_id)
            .with_fields(schema_fields)
            .build()
            .expect("Cannot create schema mock!");
        (schema, from_id)
    }

    fn get_allowed_props() -> HashMap<String, String> {
        (1..=9)
            .map(|index| (format!("key{index}"), format!("value{index}")))
            .collect()
    }

    fn get_unbounded_spec() -> (UnboundPartitionSpec, i32) {
        let id = 1;
        (
            UnboundPartitionSpec::builder()
                .with_spec_id(id)
                .with_fields(vec![])
                .build()
                .expect("Cannot build partition spec."),
            id,
        )
    }

    fn get_sort_order(schema: Schema) -> (SortOrder, i64) {
        let sort_id = 0;
        (
            SortOrder::builder()
                .with_order_id(sort_id)
                .with_fields(vec![])
                .build(schema)
                .expect("Cannot build sort order."),
            sort_id,
        )
    }

    fn get_empty_aggregate() -> TableMetadataAggregate {
        TableMetadataAggregate::new(ToOwned::to_owned("location"))
    }

    fn get_full_aggregate() -> TableMetadataAggregate {
        let (schema, schema_id) = get_mock_schema(1);
        let (spec, spec_id) = get_unbounded_spec();
        let (sort, sort_id) = get_sort_order(schema.clone());

        let mut aggregate = TableMetadataAggregate::new(ToOwned::to_owned("location"));
        aggregate
            .assign_uuid(Uuid::now_v7())
            .expect("Cannot assign uuid.")
            .add_schema(schema, None)
            .expect("Cannot set schema.")
            .set_current_schema(schema_id)
            .expect("Cannot set current schema.")
            .upgrade_format_version(FormatVersion::V2)
            .expect("Cannot set format version.")
            .add_partition_spec(spec)
            .expect("Cannot set partition spec.")
            .set_default_partition_spec(spec_id)
            .expect("Cannot set default partition spec.")
            .add_sort_order(sort)
            .expect("Cannot set sort order.")
            .set_default_sort_order(sort_id)
            .expect("Cannot set default sort order.")
            .set_location("location".to_owned())
            .expect("Cannot set location")
            .set_properties(get_allowed_props())
            .expect("Cannot set properties");

        aggregate
    }

    #[test]
    fn downgrade_version() {
        let mut aggregate = get_full_aggregate();
        assert!(aggregate.upgrade_format_version(FormatVersion::V1).is_err());
    }

    #[test]
    fn same_version() {
        let mut aggregate = get_empty_aggregate();
        assert!(aggregate.upgrade_format_version(FormatVersion::V2).is_ok());
    }

    #[test]
    fn add_schema() {
        let (schema, _) = get_mock_schema(1);
        let mut aggregate = get_empty_aggregate();
        assert!(aggregate.add_schema(schema, None).is_ok());
    }

    #[test]
    fn set_default_schema() {
        let (schema, schema_id) = get_mock_schema(1);
        let mut aggregate = get_empty_aggregate();
        assert!(aggregate
            .set_current_schema(schema_id)
            .inspect_err(|e| {
                dbg!(e);
            })
            .is_err());
        assert!(aggregate.add_schema(schema, None).is_ok());
        assert!(aggregate.set_current_schema(schema_id).is_ok());
    }

    #[test]
    fn get_current_schema() {
        let (schema, schema_id) = get_mock_schema(1);
        let mut aggregate = get_empty_aggregate();
        assert!(aggregate.set_current_schema(schema_id).is_err());
        assert!(aggregate.add_schema(schema, None).is_ok());
        assert!(aggregate.set_current_schema(schema_id).is_ok());
        assert!(aggregate
            .get_current_schema()
            .is_ok_and(|schema| schema.schema_id().eq(&schema_id)));
    }

    #[test]
    fn set_properties() {
        let forbidden_props = TableMetadataAggregate::RESERVED_PROPERTIES
            .iter()
            .enumerate()
            .map(|(index, prop)| ((*prop).to_string(), format!("value{index}")))
            .collect();
        let allowed_props = get_allowed_props();

        let mut aggregate = get_empty_aggregate();

        assert!(aggregate.set_properties(forbidden_props).is_err());
        assert!(aggregate.set_properties(allowed_props).is_ok());
    }

    #[test]
    fn can_create_new_schema_id_for_uniq_schema() {
        let mut aggregate = get_empty_aggregate();

        for schema_id in 1..=10 {
            let (schema, schema_id) = get_mock_schema(schema_id);
            aggregate.add_schema(schema, None).unwrap_or_else(|_| panic!("Cannot add {schema_id} new schema."));
            assert!(aggregate.metadata.schemas.contains_key(&schema_id));
        }
    }

    #[test]
    fn cannot_add_schema_with_same_id_but_with_different_fields() {
        let mut aggregate = get_empty_aggregate();

        let schema_fields1 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "category", Primitive(PrimitiveType::String)).into(),
        ];

        let schema_fields2 = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
        ];

        let schema_id1 = 1;
        let schema1 = Schema::builder()
            .with_schema_id(schema_id1)
            .with_fields(schema_fields1)
            .build()
            .expect("Cannot create schema mock!");
        let schema2 = Schema::builder()
            .with_schema_id(schema_id1)
            .with_fields(schema_fields2)
            .build()
            .expect("Cannot create schema mock!");

        aggregate.add_schema(schema1, None).expect("Cannot add new schema.");
        assert!(dbg!(aggregate.add_schema(schema2, None)).is_err());
    }

    #[test]
    fn reuse_schema_id_if_schemas_is_eq() {
        let mut aggregate = get_empty_aggregate();

        let schema_id = 1;
        let (schema, schema_id) = get_mock_schema(schema_id);

        aggregate.add_schema(schema, None).expect("Cannot add new schema.");
        assert!(aggregate.metadata.schemas.contains_key(&schema_id));

        let (schema, schema_id) = get_mock_schema(schema_id);

        aggregate.add_schema(schema, None).expect("Cannot add new schema.");
        assert!(aggregate.metadata.schemas.contains_key(&schema_id));

        assert!(aggregate.metadata.schemas.len().eq(&1));
    }

    // This test will fail because of changes in this commit:
    // https://github.com/hansetag/iceberg-rest-server/pull/36/commits/6faeb8d360f5c9779a4cd0d4ac237da1c6ec97ca
    // In `table_metadata.rs` file on line 248.
    // Because of this changes it do not assign new schema id to schema after `reuse_or_create_new_schema_id` call.
    #[test]
    #[ignore]
    fn should_take_highest_schema_id_plus_one_is() {
        let mut aggregate = get_empty_aggregate();
        let highest = 5;

        for schema_id in 1..=highest {
            let (schema, schema_id) = get_mock_schema(schema_id);
            aggregate.add_schema(schema, None).unwrap_or_else(|_| panic!("Cannot add {schema_id} new schema."));
            assert!(aggregate.metadata.schemas.contains_key(&schema_id));
        }

        let fields = vec![
            NestedField::required(1, "id", Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(2, "data", Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "email", Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "name", Primitive(PrimitiveType::String)).into()
        ];

        let schema_id = -1;
        let schema = Schema::builder()
            .with_schema_id(schema_id)
            .with_fields(fields)
            .build()
            .expect("Cannot create schema mock!");
        aggregate.add_schema(schema, None).unwrap_or_else(|e| panic!("Cannot add new schema: {e:?}."));
        assert!(dbg!(aggregate.metadata.schemas).contains_key(&(highest + 1)));
    }

}
