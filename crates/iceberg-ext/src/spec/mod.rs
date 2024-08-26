// If required, replace structs with own implementations here.

pub use iceberg::spec::{
    NullOrder, PartitionSpec, Schema, Snapshot, SortDirection, SortField, SortOrder, TableMetadata,
    UnboundPartitionField, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};

mod partition_binder;
mod table_metadata;
pub use table_metadata::TableMetadataAggregate;
