use std::{collections::HashMap, vec};

use http::StatusCode;
use iceberg::spec::{
    FormatVersion, PartitionField, PartitionSpec, PartitionSpecRef, Schema, SchemaRef, Snapshot,
    SnapshotLog, SnapshotReference, SortOrder, SortOrderRef, TableMetadata, UnboundPartitionSpec,
};
use uuid::Uuid;

use crate::catalog::rest::ErrorModel;
use crate::spec::partition_binder::PartitionSpecBinder;

type Result<T> = std::result::Result<T, ErrorModel>;

static MAIN_BRANCH: &str = "main";
static DEFAULT_SPEC_ID: i32 = 0;
static DEFAULT_SORT_ORDER_ID: i64 = 0;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TableMetadataAggregate {
    metadata: TableMetadata,
    current_schema_id: i32,
    added_schema_ids: Vec<i32>,
    partition_specs: Vec<UnboundPartitionSpec>,
    default_spec_id: Option<i32>,
    sort_orders: Vec<SortOrder>,
    default_sort_order_id: Option<i64>,
    snapshot: Option<Snapshot>,
    refs: HashMap<String, SnapshotReference>,
    new: bool,
}

impl TableMetadataAggregate {
    const INITIAL_SPEC_ID: i32 = 0;
    const INITIAL_SORT_ORDER_ID: i64 = 0;
    const PARTITION_DATA_ID_START: usize = 1000;

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
                last_column_id: -1,
                current_schema_id: -1,
                schemas: HashMap::new(),
                partition_specs: HashMap::new(),
                default_spec_id: -1,
                last_partition_id: 0,
                properties: HashMap::new(),
                current_snapshot_id: None,
                snapshots: HashMap::new(),
                snapshot_log: vec![],
                sort_orders: HashMap::new(),
                metadata_log: vec![],
                default_sort_order_id: -1,
                refs: HashMap::default(),
            },
            added_schema_ids: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: None,
            sort_orders: vec![],
            default_sort_order_id: None,
            snapshot: None,
            refs: HashMap::new(),
            new: true,
        }
    }

    /// Creates a new table metadata builder from the given table metadata.
    #[must_use]
    pub fn new_from_metadata(origin: TableMetadata) -> Self {
        Self {
            metadata: origin,
            added_schema_ids: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: None,
            sort_orders: vec![],
            default_sort_order_id: None,
            snapshot: None,
            refs: HashMap::new(),
            new: false,
        }
    }

    /// Changes uuid of table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn assign_uuid(&mut self, uuid: Uuid) -> Result<&mut Self> {
        if self.metadata.table_uuid != uuid {
            self.metadata.table_uuid = uuid;
        }

        Ok(self)
    }

    /// Upgrade `FormatVersion`. Downgrades are not allowed.
    ///
    /// # Errors
    /// - Cannot downgrade `format_version` from V2 to V1.
    pub fn upgrade_format_version(&mut self, format_version: FormatVersion) -> Result<&mut Self> {
        self.metadata.format_version = match format_version {
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
        Ok(self)
    }

    /// Set properties
    ///
    /// # Errors
    /// None yet.
    pub fn set_properties(&mut self, properties: HashMap<String, String>) -> Result<&mut Self> {
        self.metadata.properties.extend(properties);
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

        Ok(self)
    }

    //// Removes snapshot by its name from the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn remove_snapshot_by_ref(&mut self, snapshot_ref: &str) -> Result<&mut Self> {
        self.metadata.refs.remove(snapshot_ref);

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
        let new_last_column_id = new_last_column_id.unwrap_or_default();
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

        let (new_schema_id, schema_found) = {
            let id = self.reuse_or_create_new_schema_id(&schema);
            let is_new_id = id == schema.schema_id();
            (id, is_new_id)
        };

        if schema_found && new_last_column_id == self.metadata.last_column_id {
            self.metadata.last_column_id = new_last_column_id;
            Ok(self)
        } else {
            let schema: SchemaRef = Schema::into_builder(schema)
                .with_schema_id(new_schema_id)
                .build()
                .map_err(|e| {
                    ErrorModel::builder()
                        .code(StatusCode::CONFLICT.into())
                        .message("Failed to build new schema!")
                        .r#type("FailedToBuildNewSchema")
                        .stack(Some(vec![e.to_string()]))
                        .build()
                })?
                .into();

            self.metadata.current_schema_id = schema.schema_id();
            self.current_schema_id = schema.schema_id();
            self.metadata.schemas.insert(schema.schema_id(), schema);
            self.metadata.last_column_id = new_last_column_id;

            Ok(self)
        }
    }

    fn reuse_or_create_new_schema_id(&self, schema: &Schema) -> i32 {
        self.metadata
            .schemas
            .get(&schema.schema_id())
            .is_some_and(|exising_schema| exising_schema.as_ref().eq(schema))
            .then_some(schema.schema_id())
            .unwrap_or_else(|| self.highest_spec_id() + 1)
    }

    fn get_highest_schema_id(&self) -> i32 {
        *self
            .metadata
            .schemas
            .keys()
            .max()
            .unwrap_or(&self.current_schema_id)
    }

    /// Set the current schema id.
    /// If this `schema_id` does not exist yet, it must be added before
    /// `build` is called. If "-1" is specified, the highest added `schema_id` added during this
    /// build is used.
    ///
    /// # Errors
    /// - Current schema already set.
    pub fn set_current_schema(&mut self, schema_id: i32) -> Result<&mut Self> {
        if schema_id == -1 {
            return self.set_current_schema(self.metadata.current_schema_id);
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
            .into_iter()
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
            .into_iter()
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
        self.current_schema_id = schema_id;

        Ok(self)
    }

    // fn finalize_current_schema(&mut self) -> Result<()> {
    //     // Finalize `current_schema_id`
    //     if let Some(current_schema_id) = self.current_schema_id {
    //         if current_schema_id < 0 {
    //             match self.added_schema_ids.iter().max() {
    //                         None => {
    //                             return Err(ErrorModel::builder()
    //                                 .message("Cannot set current schema to last added schema when no schema has been added".to_string())
    //                                 .code(StatusCode::CONFLICT.into())
    //                                 .r#type("SetCurrentSchemaWithoutAddedSchema".to_string())
    //                                 .build())
    //                         }
    //                         Some(max_added_schema_id) => max_added_schema_id.clone_into(&mut self.metadata.current_schema_id)
    //                     };
    //         } else {
    //             if !self.metadata.schemas.contains_key(&current_schema_id) {
    //                 return Err(ErrorModel::builder()
    //                     .message(format!(
    //                         "Schema ID to set {current_schema_id} does not exist."
    //                     ))
    //                     .code(StatusCode::CONFLICT.into())
    //                     .r#type("SetCurrentSchemaDoesNotExist".to_string())
    //                     .build());
    //             };
    //             self.metadata.current_schema_id = current_schema_id;
    //         };
    //     };
    //
    //     Ok(())
    // }

    /// Add a partition spec to the table metadata.
    ///
    /// # Errors
    /// None yet. Fails during build if `spec` cannot be bound.
    pub fn add_partition_spec(&mut self, spec: UnboundPartitionSpec) -> Result<&mut Self> {
        let mut spec = PartitionSpecBinder::new(
            self.get_current_schema()?.clone(),
            spec.spec_id.unwrap_or_default(),
        )
        .bind_spec_schema(spec)?;
        spec.spec_id = self.reuse_or_create_new_spec_id(&spec);

        if self.metadata.format_version <= FormatVersion::V1
            && !Self::has_sequential_ids(&spec.fields)
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Spec does not use sequential IDs that are required in v1.")
                .r#type("FailedToBuildPartitionSpec")
                .build());
        }

        self.metadata.last_partition_id = spec.spec_id;
        self.metadata
            .partition_specs
            .insert(spec.spec_id, spec.into());

        Ok(self)
    }

    fn get_current_schema(&self) -> Result<&SchemaRef> {
        self.metadata
            .schemas
            .get(&self.current_schema_id)
            .ok_or_else(|| {
                ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message(format!(
                        "Failed to get current schema: '{}'!",
                        self.current_schema_id
                    ))
                    .r#type("FailedToGetCurrentSchema")
                    .build()
            })
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

    /// Set the default partition spec.
    ///
    /// # Errors
    /// None yet. Fails during build if the default spec does not exist.
    pub fn set_default_partition_spec(&mut self, spec_id: i32) -> Result<&mut Self> {
        if spec_id == -1 {
            return self.set_default_partition_spec(self.metadata.last_partition_id);
        }

        if self.metadata.default_spec_id == spec_id {
            return Ok(self);
        }

        self.metadata.default_spec_id = spec_id;

        Ok(self)
    }

    /// Returns added partition spec IDs
    ///
    /// To ask:
    /// Why `UnboundPartitionSpec.spec_id` is option ?
    /// What is `default_spec_id` ?
    /// Is this `last_partition_id` is equal to `lastAddedSpecId` in Java version ?
    /// Why we don't update last `last_partition_id` when calling `add_partition_spec` ?
    /// Can I replace `highest_spec_id` with `self.last_partition_id` ?
    /// Why this thing is still unimplemented ?
    /// Maybe it make sense to initialize `added_partition_specs_ids` via `.with_capacity` function ?
    /// How to bind schema to `partition_spec` ?
    // fn finalize_partition_spec(&mut self) -> Result<Vec<i32>> {
    //     todo!();
    // }

    /// If the spec already exists, use the same ID. Otherwise, use 1 more than the highest ID.
    fn reuse_or_create_new_spec_id(&self, other: &PartitionSpec) -> i32 {
        self.metadata
            .partition_specs
            .get(&other.spec_id)
            .is_some_and(|founded_spec| founded_spec.fields.eq(&other.fields))
            .then_some(other.spec_id)
            .unwrap_or_else(|| self.highest_spec_id() + 1)
    }

    fn highest_spec_id(&self) -> i32 {
        *self
            .metadata
            .partition_specs
            .keys()
            .max()
            .unwrap_or(&Self::INITIAL_SPEC_ID)
    }

    // fn finalize_default_partition_spec(&mut self, added_partition_specs: &[i32]) -> Result<()> {
    //     if let Some(default_spec_id) = self.default_spec_id {
    //         if default_spec_id < 0 {
    //             match added_partition_specs.iter().max() {
    //                         None => {
    //                             return Err(ErrorModel::builder()
    //                                 .message("Cannot set current partition spec to last added spec when no spec has been added".to_string())
    //                                 .code(StatusCode::CONFLICT.into())
    //                                 .r#type("SetDefaultSpecWithoutAddedSpec".to_string())
    //                                 .build())
    //                         }
    //                         Some(max_added_spec) => max_added_spec.clone_into(&mut self.metadata.default_spec_id)
    //                     };
    //         } else {
    //             if !self.metadata.partition_specs.contains_key(&default_spec_id) {
    //                 return Err(ErrorModel::builder()
    //                     .message(format!(
    //                         "Partition spec id to set {default_spec_id} does not exist.",
    //                     ))
    //                     .code(StatusCode::CONFLICT.into())
    //                     .r#type("SetDefaultSpecNotExist".to_string())
    //                     .build());
    //             };
    //             self.metadata.default_spec_id = default_spec_id;
    //         };
    //     };
    //
    //     Ok(())
    // }

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
                    .r#type("FailedToBuildSortOrdering")
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?;

        sort_order.order_id = self.reuse_or_create_new_sort_id(&sort_order);

        self.metadata
            .sort_orders
            .insert(sort_order.order_id, sort_order.into());

        Ok(self)
    }

    fn reuse_or_create_new_sort_id(&self, other: &SortOrder) -> i64 {
        if other.is_unsorted() {
            return 0;
        }

        self.metadata
            .sort_orders
            .get(&other.order_id)
            .is_some_and(|founded_spec| founded_spec.fields.eq(&other.fields))
            .then_some(other.order_id)
            .unwrap_or_else(|| self.highest_sort_id() + 1)
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
        if order_id == -1 {
            return self.set_default_sort_order(order_id);
        }

        if self.metadata.default_sort_order_id == order_id {
            return Ok(self);
        }

        self.metadata.default_sort_order_id = order_id;

        Ok(self)
    }

    fn finalize_sort_order(&mut self) -> Result<Vec<i64>> {
        // ToDo: Why is sort-order not unbound?
        let mut added_sort_orders = vec![];

        for sort_order in &self.sort_orders {
            // ToDo: Validate that all fields exist in the schema.
            if sort_order.order_id < 0 {
                return Err(ErrorModel::builder()
                    .message("Sort Order ID to add is negative.".to_string())
                    .code(StatusCode::BAD_REQUEST.into())
                    .r#type("NegativeSortOrderId".to_string())
                    .build());
            };

            added_sort_orders.push(sort_order.order_id);
            self.metadata
                .sort_orders
                .insert(sort_order.order_id, sort_order.clone().into());
        }

        Ok(added_sort_orders)
    }

    // fn finalize_default_sort_order(&mut self, added_sort_orders: &[i64]) -> Result<()> {
    //     if let Some(default_sort_order_id) = self.default_sort_order_id {
    //         if default_sort_order_id < 0 {
    //             match added_sort_orders.iter().max() {
    //                         None => {
    //                             return Err(ErrorModel::builder()
    //                                 .message("Cannot set current sort order to last added sort order when no sort order has been added".to_string())
    //                                 .code(StatusCode::CONFLICT.into())
    //                                 .r#type("SetDefaultSortOrderWithoutAddedSortOrder".to_string())
    //                                 .build())
    //                         }
    //                         Some(max_added_sort_order) => max_added_sort_order.clone_into(&mut self.metadata.default_sort_order_id)
    //                     };
    //         } else {
    //             if !self
    //                 .metadata
    //                 .sort_orders
    //                 .contains_key(&default_sort_order_id)
    //             {
    //                 return Err(ErrorModel::builder()
    //                     .message(format!(
    //                         "Sort order id to set {default_sort_order_id} does not exist."
    //                     ))
    //                     .code(StatusCode::CONFLICT.into())
    //                     .r#type("SetDefaultSortOrderNotExist".to_string())
    //                     .build());
    //             };
    //             self.metadata.default_sort_order_id = default_sort_order_id;
    //         };
    //     };
    //
    //     Ok(())
    // }

    /// Set the location of the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn set_location(&mut self, location: String) -> Result<&mut Self> {
        if self.metadata.location != location {
            self.metadata.location = location;
        }

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

        self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
        self.metadata.last_sequence_number = snapshot.sequence_number();
        self.metadata
            .snapshots
            .insert(snapshot.snapshot_id(), snapshot.into());

        Ok(self)
    }

    // fn finalize_snapshot(&mut self) -> Result<Option<i64>> {
    //         if self.metadata.schemas.is_empty() {
    //             return Err(ErrorModel::builder()
    //                 .message("Attempting to add a snapshot before a schema is added".to_string())
    //                 .code(StatusCode::CONFLICT.into())
    //                 .r#type("AddSnapshotBeforeSchema".to_string())
    //                 .build());
    //         } else if self.metadata.partition_specs.is_empty() {
    //             return Err(ErrorModel::builder()
    //                 .message(
    //                     "Attempting to add a snapshot before a partition spec is added".to_string(),
    //                 )
    //                 .code(StatusCode::CONFLICT.into())
    //                 .r#type("AddSnapshotBeforePartitionSpec".to_string())
    //                 .build());
    //         } else if self.metadata.sort_orders.is_empty() {
    //             return Err(ErrorModel::builder()
    //                 .message("Attempting to add a snapshot before a sort order is added".to_string())
    //                 .code(StatusCode::CONFLICT.into())
    //                 .r#type("AddSnapshotBeforeSortOrder".to_string())
    //                 .build());
    //         }
    //
    //         let snapshot_id = self
    //             .snapshot
    //             .as_ref()
    //             .map(iceberg::spec::Snapshot::snapshot_id);
    //
    //         if let Some(snapshot) = &self.snapshot {
    //             if self
    //                 .metadata
    //                 .snapshot_by_id(snapshot.snapshot_id())
    //                 .is_some()
    //             {
    //                 return Err(ErrorModel::builder()
    //                     .message(format!(
    //                         "Snapshot with id {} already exists.",
    //                         snapshot.snapshot_id()
    //                     ))
    //                     .code(StatusCode::CONFLICT.into())
    //                     .r#type("SnapshotIDAlreadyExists".to_string())
    //                     .build());
    //             };
    //
    //             if self.metadata.format_version == FormatVersion::V2
    //                 && snapshot.sequence_number() <= self.metadata.last_sequence_number
    //                 && snapshot.parent_snapshot_id().is_some()
    //             {
    //                 return Err(ErrorModel::builder()
    //                     .message(format!(
    //                         "Cannot add snapshot with sequence number {} older than last sequence number {}",
    //                         snapshot.sequence_number(),
    //                         self.metadata.last_sequence_number
    //                     ))
    //                     .code(StatusCode::CONFLICT.into())
    //                     .r#type("AddSnapshotOlderThanLast".to_string())
    //                     .build()
    //                     );
    //             };
    //
    //             self.metadata
    //                 .snapshots
    //                 .insert(snapshot.snapshot_id(), snapshot.to_owned().into());
    //
    //             self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
    //             self.metadata.last_sequence_number = snapshot.sequence_number();
    //         }
    //
    //         Ok(snapshot_id)
    // }

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

        if ref_name == MAIN_BRANCH {
            self.metadata.current_snapshot_id = Some(snapshot.snapshot_id());
            self.metadata.last_updated_ms = self
                .metadata
                .last_updated_ms
                .eq(&0)
                .then_some(chrono::Utc::now().timestamp_millis())
                .unwrap_or(self.metadata.last_updated_ms);

            self.metadata.snapshot_log.push(SnapshotLog {
                snapshot_id: snapshot.snapshot_id(),
                timestamp_ms: self.metadata.last_updated_ms,
            });
        }

        self.metadata.refs.insert(ref_name, reference);

        Ok(self)
    }

    // fn finalize_refs(&mut self) -> Result<&mut Self> {
    //     for (ref_name, reference) in &self.refs {
    //         // ToDo: Respect retention policy
    //         let SnapshotReference {
    //             snapshot_id,
    //             retention: _,
    //         } = &reference;
    //
    //         let existing_ref = self.metadata.refs.get(ref_name);
    //
    //         // Maybe there is nothing to update
    //         if existing_ref.is_some() && existing_ref == Some(reference) {
    //             return Ok(self);
    //         }
    //
    //         // Check if snapshot exists
    //         let snapshot = self.metadata.snapshot_by_id(*snapshot_id);
    //         if snapshot.is_none() {
    //             return Err(ErrorModel::builder()
    //                 .message(format!(
    //                     "Cannot set {ref_name} to unknown snapshot {snapshot_id}"
    //                 ))
    //                 .code(StatusCode::CONFLICT.into())
    //                 .r#type("SetRefToUnknownSnapshot".to_string())
    //                 .build());
    //         }
    //
    //         if let Some(snapshot) = snapshot {
    //             self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
    //         }
    //
    //         if ref_name == MAIN_BRANCH {
    //             self.metadata.current_snapshot_id = Some(*snapshot_id);
    //             if self.metadata.last_updated_ms == 0 {
    //                 self.metadata.last_updated_ms = chrono::Utc::now().timestamp_millis();
    //             }
    //
    //             self.metadata.snapshot_log.push(SnapshotLog {
    //                 snapshot_id: *snapshot_id,
    //                 timestamp_ms: self.metadata.last_updated_ms,
    //             });
    //         }
    //
    //         self.metadata
    //             .refs
    //             .insert(ref_name.to_owned(), reference.to_owned());
    //     }
    //
    //     Ok(self)
    // }

    /// Build the table metadata.
    ///
    /// # Errors
    /// - No schema has been added.
    /// - Default sort order is set to -1 but no sort order has been added.
    /// - Default partition spec is set to -1 but no partition spec has been added.
    /// - Ref is set to an unknown snapshot.
    pub fn build(mut self) -> Result<TableMetadata> {
        if self.new && self.sort_orders.is_empty() && self.default_sort_order_id.is_none() {
            self.sort_orders.push(SortOrder {
                order_id: DEFAULT_SORT_ORDER_ID,
                fields: vec![],
            });
            self.default_sort_order_id = Some(DEFAULT_SORT_ORDER_ID);
        }

        if self.new && self.partition_specs.is_empty() && self.default_spec_id.is_none() {
            self.partition_specs.push(UnboundPartitionSpec {
                spec_id: Some(DEFAULT_SPEC_ID),
                fields: vec![],
            });
            self.default_spec_id = Some(DEFAULT_SPEC_ID);
        }

        if self.new && self.metadata.schemas.is_empty() {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without a schema")
                .code(StatusCode::CONFLICT.into())
                .r#type("CreateTableWithoutSchema")
                .build());
        }

        self.finalize_current_schema()?;
        let added_specs = self.finalize_partition_spec()?;
        self.finalize_default_partition_spec(&added_specs)?;
        let added_sort_orders = self.finalize_sort_order()?;
        self.finalize_default_sort_order(&added_sort_orders)?;
        let _snapshot_id = self.finalize_snapshot()?;
        self.finalize_refs()?;

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
}
