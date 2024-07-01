use crate::api::{ErrorModel, Result};
use flate2::{write::GzEncoder, Compression};
use http::StatusCode;
use iceberg::io::FileIO;
use serde::Serialize;
use std::io::Write;

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    table_metadata: impl Serialize,
    file_io: &FileIO,
) -> Result<()> {
    let metadata_file = file_io.new_output(metadata_location).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::FAILED_DEPENDENCY.into())
            .message(
                "Failed to create metadata file. Please check the storage credentials.".to_string(),
            )
            .r#type("MetadataFileCreationFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let mut writer = metadata_file.writer().await.map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::FAILED_DEPENDENCY.into())
            .message(
                "Failed to create metadata file writer. Please check the storage credentials."
                    .to_string(),
            )
            .r#type("MetadataFileWriterCreationFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
    let buf = serde_json::to_vec(&table_metadata).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Failed to serialize table metadata.".to_string())
            .r#type("TableMetadataSerializationFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    compressed_metadata.write_all(&buf).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(format!("Failed to compress metadata file: {e}"))
            .r#type("MetadataFileCompressionFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let compressed_metadata = compressed_metadata.finish().map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(format!("Failed to finish compressing metadata file: {e}"))
            .r#type("MetadataFileCompressionFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    writer
        .write(compressed_metadata.into())
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::FAILED_DEPENDENCY.into())
                .message(format!("Failed to write metadata file: {e}"))
                .r#type("MetadataFileWriteFailed".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

    writer.close().await.map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::FAILED_DEPENDENCY.into())
            .message(format!("Failed to close metadata file: {e}"))
            .r#type("MetadataFileCloseFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    Ok(())
}
