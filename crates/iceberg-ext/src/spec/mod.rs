// If required, replace structs with own implementations here.

#[allow(clippy::module_name_repetitions)]
pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

mod partition_binder;
pub(crate) mod table_metadata;
pub use table_metadata::TableMetadataAggregate;
