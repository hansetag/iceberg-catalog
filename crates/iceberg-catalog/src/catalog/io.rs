use crate::api::{ErrorModel, Result};
use crate::service::storage::path_utils;
use flate2::{write::GzEncoder, Compression};
use iceberg::io::FileIO;
use serde::Serialize;
use std::io::Write;

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    table_metadata: impl Serialize,
    file_io: &FileIO,
) -> Result<()> {
    tracing::debug!("Received location: {}", metadata_location);
    let metadata_location = if metadata_location.starts_with("abfs") {
        path_utils::reduce_scheme_string(metadata_location, false)
    } else {
        metadata_location.to_string()
    };
    tracing::debug!("Going to write metadata file to {}", metadata_location);

    let metadata_file = file_io.new_output(metadata_location).map_err(|e| {
        ErrorModel::failed_dependency(
            "Failed to create metadata file. Please check the storage credentials.",
            "MetadataFileCreationFailed",
            Some(Box::new(e)),
        )
    })?;

    let mut writer = metadata_file.writer().await.map_err(|e| {
        ErrorModel::failed_dependency(
            "Failed to create metadata file writer. Please check the storage credentials.",
            "MetadataFileWriterCreationFailed",
            Some(Box::new(e)),
        )
    })?;

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

    writer
        .write(compressed_metadata.into())
        .await
        .map_err(|e| {
            ErrorModel::failed_dependency(
                "Failed to write metadata file. Please check the storage credentials.",
                "MetadataFileWriteFailed",
                Some(Box::new(e)),
            )
        })?;

    writer.close().await.map_err(|e| {
        ErrorModel::failed_dependency(
            "Failed to close metadata file. Please check the storage credentials.",
            "MetadataFileCloseFailed",
            Some(Box::new(e)),
        )
    })?;

    Ok(())
}
