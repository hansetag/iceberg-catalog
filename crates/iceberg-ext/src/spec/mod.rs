// If required, replace structs with own implementations here.

use iceberg::spec::SchemaBuilder;
#[allow(clippy::module_name_repetitions)]
pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

mod partition_binder;
mod table_metadata;
pub use table_metadata::TableMetadataAggregate;
