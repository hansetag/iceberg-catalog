use http::StatusCode;
use iceberg::spec::{
    FormatVersion, PartitionSpec, Schema, Snapshot, SnapshotLog, SnapshotReference, SortOrder,
    TableMetadata, UnboundPartitionSpec,
};
use std::{collections::HashMap, vec};
use uuid::Uuid;

use crate::catalog::rest::ErrorModel;

type Result<T> = std::result::Result<T, ErrorModel>;

static MAIN_BRANCH: &str = "main";
static DEFAULT_SPEC_ID: i32 = 0;
static DEFAULT_SORT_ORDER_ID: i64 = 0;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TableMetadataBuilder {
    metadata: TableMetadata,
    current_schema_id: Option<i32>,
    added_schema_ids: Vec<i32>,
    partition_specs: Vec<UnboundPartitionSpec>,
    default_spec_id: Option<i32>,
    sort_orders: Vec<SortOrder>,
    default_sort_order_id: Option<i64>,
    snapshot: Option<Snapshot>,
    refs: HashMap<String, SnapshotReference>,
    new: bool,
}

impl TableMetadataBuilder {
    /// Initialize new table metadata.
    #[must_use]
    pub fn new(location: String) -> Self {
        let table_metadata = TableMetadata {
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
        };

        Self {
            metadata: table_metadata,
            added_schema_ids: vec![],
            current_schema_id: None,
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
            current_schema_id: None,
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
        self.metadata.table_uuid = uuid;

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
                        .code(StatusCode::CONFLICT.as_u16())
                        .message("Cannot downgrade FormatVersion from V2 to V1".to_string())
                        .r#type("FormatVersionNoDowngrade".to_string())
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
    pub fn remove_properties(&mut self, properties: &Vec<String>) -> Result<&mut Self> {
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
        for (key, value) in properties {
            self.metadata.properties.insert(key, value);
        }
        Ok(self)
    }

    /// Add a schema to the table metadata.
    ///
    /// If `last_column_id` is not provided, the highest field id in the schema is used.
    ///
    /// # Errors
    /// - Last column id is lower than the current last column id.
    /// - Schema ID already exists.
    pub fn add_schema(&mut self, schema: Schema, last_column_id: Option<i32>) -> Result<&mut Self> {
        // ToDo: Additional validations? Fields?
        let last_column_id = last_column_id.unwrap_or(schema.highest_field_id());
        if last_column_id < self.metadata.last_column_id {
            return Err(ErrorModel::builder()
                .message(format!(
                    "Invalid last column id {}, must be >= {}",
                    last_column_id, self.metadata.last_column_id
                ))
                .r#type("LastColumnIdTooLow".to_string())
                .code(StatusCode::CONFLICT.into())
                .build());
        }

        if self.metadata.schemas.contains_key(&schema.schema_id()) {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("ID of the schema to add already exists".to_string())
                .r#type("SchemaIDAlreadyExists".to_string())
                .build());
        }

        self.added_schema_ids.push(schema.schema_id());
        self.metadata.last_column_id = last_column_id;
        self.metadata
            .schemas
            .insert(schema.schema_id(), schema.into());

        Ok(self)
    }

    /// Set the current schema id.
    /// If this `schema_id` does not exist yet, it must be added before
    /// `build` is called. If "-1" is specified, the highest added `schema_id` added during this
    /// build is used.
    ///
    /// # Errors
    /// - Current schema already set.
    pub fn set_current_schema(&mut self, schema_id: i32) -> Result<&mut Self> {
        if self.current_schema_id.is_some() {
            return Err(ErrorModel::builder()
                .message("Current schema already set".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("CurrentSchemaAlreadySet".to_string())
                .build());
        }

        self.current_schema_id = Some(schema_id);

        Ok(self)
    }

    fn finalize_current_schema(&mut self) -> Result<()> {
        // Finalize `current_schema_id`
        if let Some(current_schema_id) = self.current_schema_id {
            if current_schema_id < 0 {
                match self.added_schema_ids.iter().max() {
                            None => {
                                return Err(ErrorModel::builder()
                                    .message("Cannot set current schema to last added schema when no schema has been added".to_string())
                                    .code(StatusCode::CONFLICT.into())
                                    .r#type("SetCurrentSchemaWithoutAddedSchema".to_string())
                                    .build())
                            }
                            Some(max_added_schema_id) => {self.metadata.current_schema_id = max_added_schema_id.to_owned();}
                        };
            } else {
                if !self.metadata.schemas.contains_key(&current_schema_id) {
                    return Err(ErrorModel::builder()
                        .message(format!(
                            "Schema ID to set {current_schema_id} does not exist."
                        ))
                        .code(StatusCode::CONFLICT.into())
                        .r#type("SetCurrentSchemaDoesNotExist".to_string())
                        .build());
                };
                self.metadata.current_schema_id = current_schema_id;
            };
        };

        Ok(())
    }

    /// Add a partition spec to the table metadata.
    ///
    /// # Errors
    /// None yet. Fails during build if `spec` cannot be bound.
    pub fn add_partition_spec(&mut self, spec: UnboundPartitionSpec) -> Result<&mut Self> {
        self.partition_specs.push(spec);

        Ok(self)
    }

    /// Set the default partition spec.
    ///
    /// # Errors
    /// None yet. Fails during build if the default spec does not exist.
    pub fn set_default_partition_spec(&mut self, spec_id: i32) -> Result<&mut Self> {
        self.default_spec_id = Some(spec_id);
        Ok(self)
    }

    /// Returns added partition spec IDs
    fn finalize_partition_spec(&mut self) -> Result<Vec<i32>> {
        let mut added_partition_specs = vec![];
        for spec in &self.partition_specs {
            // ToDo: Properly bind partitions & support partitions.
            // At this point the schema has already been finalized.
            // ToDo: Disallow partition spec overwrites?
            if !spec.fields.is_empty() {
                return Err(ErrorModel::builder()
                    .message("Partitioned Tables not supported.".to_string())
                    .code(StatusCode::CONFLICT.into())
                    .r#type("PartitionedTablesUnsupported".to_string())
                    .build());
            }

            let partition_spec = PartitionSpec::builder()
                .with_spec_id(spec.spec_id.unwrap_or(0))
                .with_fields(vec![])
                .build()
                .map_err(|e| {
                    ErrorModel::builder()
                        .code(StatusCode::CONFLICT.into())
                        .message("Failed to build Partition spec".to_string())
                        .r#type("FailedToBuildPartitionSpec".to_string())
                        .stack(Some(vec![e.to_string()]))
                        .build()
                })?;
            let partition_spec_id = partition_spec.spec_id;
            self.metadata
                .partition_specs
                .insert(partition_spec.spec_id, partition_spec.into());
            added_partition_specs.push(partition_spec_id);
        }
        Ok(added_partition_specs)
    }

    fn finalize_default_partition_spec(&mut self, added_partition_specs: &[i32]) -> Result<()> {
        if let Some(default_spec_id) = self.default_spec_id {
            if default_spec_id < 0 {
                match added_partition_specs.iter().max() {
                            None => {
                                return Err(ErrorModel::builder()
                                    .message("Cannot set current partition spec to last added spec when no spec has been added".to_string())
                                    .code(StatusCode::CONFLICT.into())
                                    .r#type("SetDefaultSpecWithoutAddedSpec".to_string())
                                    .build())
                            }
                            Some(max_added_spec) => {self.metadata.default_spec_id = max_added_spec.to_owned();}
                        };
            } else {
                if !self.metadata.partition_specs.contains_key(&default_spec_id) {
                    return Err(ErrorModel::builder()
                        .message(format!(
                            "Partition spec id to set {default_spec_id} does not exist.",
                        ))
                        .code(StatusCode::CONFLICT.into())
                        .r#type("SetDefaultSpecNotExist".to_string())
                        .build());
                };
                self.metadata.default_spec_id = default_spec_id;
            };
        };

        Ok(())
    }

    /// Add a sort order to the table metadata.
    ///
    /// # Errors
    /// - Sort Order ID to add already exists.
    pub fn add_sort_order(&mut self, sort_order: SortOrder) -> Result<&mut Self> {
        if self.metadata.sort_orders.contains_key(&sort_order.order_id) {
            return Err(ErrorModel::builder()
                .message("Sort Order ID to add already exists.".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("SortOrderIDAlreadyExists".to_string())
                .build());
        };

        self.sort_orders.push(sort_order);

        Ok(self)
    }

    /// Set the default sort order.
    ///
    /// # Errors
    /// - Default Sort Order already set.
    pub fn set_default_sort_order(&mut self, order_id: i64) -> Result<&mut Self> {
        if self.default_sort_order_id.is_some() {
            return Err(ErrorModel::builder()
                .message("Default Sort Order already set.".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("DefaultSortOrderAlreadySet".to_string())
                .build());
        };

        self.default_sort_order_id = Some(order_id);
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

    fn finalize_default_sort_order(&mut self, added_sort_orders: &[i64]) -> Result<()> {
        if let Some(default_sort_order_id) = self.default_sort_order_id {
            if default_sort_order_id < 0 {
                match added_sort_orders.iter().max() {
                            None => {
                                return Err(ErrorModel::builder()
                                    .message("Cannot set current sort order to last added sort order when no sort order has been added".to_string())
                                    .code(StatusCode::CONFLICT.into())
                                    .r#type("SetDefaultSortOrderWithoutAddedSortOrder".to_string())
                                    .build())
                            }
                            Some(max_added_sort_order) => {self.metadata.default_sort_order_id = max_added_sort_order.to_owned();}
                        };
            } else {
                if !self
                    .metadata
                    .sort_orders
                    .contains_key(&default_sort_order_id)
                {
                    return Err(ErrorModel::builder()
                        .message(format!(
                            "Sort order id to set {default_sort_order_id} does not exist."
                        ))
                        .code(StatusCode::CONFLICT.into())
                        .r#type("SetDefaultSortOrderNotExist".to_string())
                        .build());
                };
                self.metadata.default_sort_order_id = default_sort_order_id;
            };
        };

        Ok(())
    }

    /// Set the location of the table metadata.
    ///
    /// # Errors
    /// None yet.
    pub fn set_location(&mut self, location: String) -> Result<&mut Self> {
        self.metadata.location = location;

        Ok(self)
    }

    /// Add a snapshot to the table metadata.
    ///
    /// # Errors
    /// - Only one snapshot update is allowed per commit.
    pub fn add_snapshot(&mut self, snapshot: Snapshot) -> Result<&mut Self> {
        if self.snapshot.is_some() {
            return Err(ErrorModel::builder()
                .message("Only one Snapshot Update is allowed per commit".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("SnapshotAlreadyExists".to_string())
                .build());
        };
        self.snapshot = Some(snapshot);

        Ok(self)
    }

    fn finalize_snapshot(&mut self) -> Result<Option<i64>> {
        if self.metadata.schemas.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a schema is added".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforeSchema".to_string())
                .build());
        } else if self.metadata.partition_specs.is_empty() {
            return Err(ErrorModel::builder()
                .message(
                    "Attempting to add a snapshot before a partition spec is added".to_string(),
                )
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforePartitionSpec".to_string())
                .build());
        } else if self.metadata.sort_orders.is_empty() {
            return Err(ErrorModel::builder()
                .message("Attempting to add a snapshot before a sort order is added".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("AddSnapshotBeforeSortOrder".to_string())
                .build());
        }

        let snapshot_id = self
            .snapshot
            .as_ref()
            .map(iceberg::spec::Snapshot::snapshot_id);

        if let Some(snapshot) = &self.snapshot {
            if self
                .metadata
                .snapshot_by_id(snapshot.snapshot_id())
                .is_some()
            {
                return Err(ErrorModel::builder()
                    .message(format!(
                        "Snapshot with id {} already exists.",
                        snapshot.snapshot_id()
                    ))
                    .code(StatusCode::CONFLICT.into())
                    .r#type("SnapshotIDAlreadyExists".to_string())
                    .build());
            };

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
                    .r#type("AddSnapshotOlderThanLast".to_string())
                    .build()
                    );
            };

            self.metadata
                .snapshots
                .insert(snapshot.snapshot_id(), snapshot.to_owned().into());

            self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
            self.metadata.last_sequence_number = snapshot.sequence_number();
        }

        Ok(snapshot_id)
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
        if self.refs.contains_key(&ref_name) {
            return Err(ErrorModel::builder()
                .message(format!("Reference {ref_name} already set in this commit."))
                .code(StatusCode::CONFLICT.into())
                .r#type("ReferenceAlreadyExists".to_string())
                .build());
        };

        self.refs.insert(ref_name, reference);

        Ok(self)
    }

    fn finalize_refs(&mut self) -> Result<&mut Self> {
        for (ref_name, reference) in &self.refs {
            // ToDo: Respect retention policy
            let SnapshotReference {
                snapshot_id,
                retention: _,
            } = &reference;

            let existing_ref = self.metadata.refs.get(ref_name);

            // Maybe there is nothing to update
            if existing_ref.is_some() && existing_ref == Some(reference) {
                return Ok(self);
            }

            // Check if snapshot exists
            let snapshot = self.metadata.snapshot_by_id(*snapshot_id);
            if snapshot.is_none() {
                return Err(ErrorModel::builder()
                    .message(format!(
                        "Cannot set {ref_name} to unknown snapshot {snapshot_id}"
                    ))
                    .code(StatusCode::CONFLICT.into())
                    .r#type("SetRefToUnknownSnapshot".to_string())
                    .build());
            }

            if let Some(snapshot) = snapshot {
                self.metadata.last_updated_ms = snapshot.timestamp().timestamp_millis();
            }

            if ref_name == MAIN_BRANCH {
                self.metadata.current_snapshot_id = Some(*snapshot_id);
                if self.metadata.last_updated_ms == 0 {
                    self.metadata.last_updated_ms = chrono::Utc::now().timestamp_millis();
                }

                self.metadata.snapshot_log.push(SnapshotLog {
                    snapshot_id: *snapshot_id,
                    timestamp_ms: self.metadata.last_updated_ms,
                });
            }

            self.metadata
                .refs
                .insert(ref_name.to_owned(), reference.to_owned());
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
                .message("Cannot create a table without a schema".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("CreateTableWithoutSchema".to_string())
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
                .message("Cannot create a table without last_column_id".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("LastColumnIdMissing".to_string())
                .build());
        }

        if self.metadata.current_schema_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without current_schema_id".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("CurrentSchemaIdMissing".to_string())
                .build());
        }

        if self.metadata.default_spec_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without default_spec_id".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("DefaultSpecIdMissing".to_string())
                .build());
        }

        if self.metadata.default_sort_order_id < 0 {
            return Err(ErrorModel::builder()
                .message("Cannot create a table without default_sort_order_id".to_string())
                .code(StatusCode::CONFLICT.into())
                .r#type("DefaultSortOrderIdMissing".to_string())
                .build());
        }

        Ok(self.metadata)
    }
}
