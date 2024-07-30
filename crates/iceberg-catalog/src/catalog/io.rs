use crate::api::{ErrorModel, Result};
use flate2::{write::GzEncoder, Compression};
use iceberg::io::FileIO;
use object_store::{ObjectStore, PutPayloadIter};
use serde::Serialize;
use std::io::Write;
use std::sync::Arc;

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    table_metadata: impl Serialize,
    file_io: Arc<dyn ObjectStore>,
) -> Result<()> {
    let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
    let buf = serde_json::to_vec(&table_metadata).map_err(|e| {
        ErrorModel::internal(
            "Failed to serialize table metadata.",
            "TableMetadataSerializationFailed",
            Some(Box::new(e)),
        )
    })?;

    compressed_metadata.write_all(&buf).map_err(|e| {
        ErrorModel::internal(
            "Failed to write table metadata to compressed buffer.",
            "TableMetadataWriteFailed",
            Some(Box::new(e)),
        )
    })?;

    let compressed_metadata = compressed_metadata.finish().map_err(|e| {
        ErrorModel::internal(
            "Failed to finish compressing metadata file.",
            "MetadataFileCompressionFailed",
            Some(Box::new(e)),
        )
    })?;
    file_io
        .put(
            &object_store::path::Path::parse(metadata_location).map_err(|e| {
                ErrorModel::internal(
                    "Failed to parse metadata location.",
                    "MetadataLocationParseFailed",
                    Some(Box::new(e)),
                )
            })?,
            compressed_metadata.into(),
        )
        .await
        .map_err(|e| {
            ErrorModel::failed_dependency(
                "Failed to write metadata file. Please check the storage credentials.",
                "MetadataFileWriteFailed",
                Some(Box::new(e)),
            )
        })?;

    Ok(())
}
