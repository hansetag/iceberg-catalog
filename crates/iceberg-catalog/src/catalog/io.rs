use crate::api::{ErrorModel, Result};
use flate2::{write::GzEncoder, Compression};
use iceberg::{io::FileIO, spec::view_properties::METADATA_COMPRESSION};
use serde::Serialize;
use std::io::Write;

use super::CommonMetadata;

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    metadata: impl CommonMetadata + Serialize,
    file_io: &FileIO,
) -> Result<()> {
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

    let buf = serde_json::to_vec(&metadata).map_err(|e| {
        ErrorModel::internal(
            "Failed to serialize table metadata.",
            "TableMetadataSerializationFailed",
            Some(Box::new(e)),
        )
    })?;

    let do_compression = match metadata.properties().get(METADATA_COMPRESSION) {
        Some(value) if value.to_lowercase().trim() == "gzip" => true,
        None => true,
        _ => false,
    };
    let metadata_bytes = if do_compression {
        let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
        compressed_metadata.write_all(&buf).map_err(|e| {
            ErrorModel::internal(
                "Failed to write table metadata to compressed buffer.",
                "TableMetadataWriteFailed",
                Some(Box::new(e)),
            )
        })?;

        compressed_metadata.finish().map_err(|e| {
            ErrorModel::internal(
                "Failed to finish compressing metadata file.",
                "MetadataFileCompressionFailed",
                Some(Box::new(e)),
            )
        })?
    } else {
        buf
    };

    writer.write(metadata_bytes.into()).await.map_err(|e| {
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
