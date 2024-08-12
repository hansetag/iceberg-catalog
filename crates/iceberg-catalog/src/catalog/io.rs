use crate::api::{ErrorModel, Result};
use crate::service::storage::path_utils;
use iceberg::io::FileIO;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use serde::Serialize;

use super::compression_codec::{CompressionCodec, UnsupportedCompressionCodec};

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    metadata: impl Serialize,
    compression_codec: CompressionCodec,
    file_io: &FileIO,
) -> Result<(), IoError> {
    tracing::debug!("Received location: {}", metadata_location);
    let metadata_location = if metadata_location.starts_with("abfs") {
        path_utils::reduce_scheme_string(metadata_location, false)
    } else {
        metadata_location.to_string()
    };
    tracing::debug!("Going to write metadata file to {}", metadata_location);

    let metadata_file = file_io
        .new_output(metadata_location)
        .map_err(IoError::FileCreation)?;

    let mut writer = metadata_file
        .writer()
        .await
        .map_err(IoError::FileWriterCreation)?;

    let buf = serde_json::to_vec(&metadata).map_err(IoError::Serialization)?;

    let metadata_bytes = compression_codec.compress(&buf[..])?;

    writer
        .write(metadata_bytes.into())
        .await
        .map_err(IoError::FileWrite)?;

    writer.close().await.map_err(IoError::FileClose)?;

    Ok(())
}

pub(crate) async fn delete_file(file_io: &FileIO, location: &str) -> Result<(), IoError> {
    let location = if location.starts_with("abfs") {
        path_utils::reduce_scheme_string(location, false)
    } else {
        location.to_string()
    };

    file_io
        .delete(location)
        .await
        .map_err(IoError::FileDelete)?;

    Ok(())
}

pub(crate) async fn read_file(file_io: &FileIO, file: &str) -> Result<Vec<u8>, IoError> {
    let inp = file_io.new_input(file).map_err(IoError::FileCreation)?;
    inp.read()
        .await
        .map_err(|e| IoError::FileRead(Box::new(e)))
        .map(|r| r.to_vec())
}

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IoError {
    #[error("Failed to create file. Please check the storage credentials.")]
    FileCreation(#[source] iceberg::Error),
    #[error("Failed to create file writer. Please check the storage credentials.")]
    FileWriterCreation(#[source] iceberg::Error),
    #[error("Failed to serialize data.")]
    Serialization(#[source] serde_json::Error),
    #[error("Failed to write table metadata to compressed buffer.")]
    Write(#[source] iceberg::Error),
    #[error("Failed to finish compressing file.")]
    FileCompression(#[source] std::io::Error),
    #[error("Failed to write file. Please check the storage credentials.")]
    FileWrite(#[source] iceberg::Error),
    #[error("Failed to write file. Please check the storage credentials.")]
    FileRead(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to close file. Please check the storage credentials.")]
    FileClose(#[source] iceberg::Error),
    #[error("Failed to delete file. Please check the storage credentials.")]
    FileDelete(#[source] iceberg::Error),
    #[error(transparent)]
    UnsupportedCompressionCodec(#[from] UnsupportedCompressionCodec),
}

impl IoError {
    pub fn to_type(&self) -> &'static str {
        self.into()
    }
}

impl From<IoError> for IcebergErrorResponse {
    fn from(value: IoError) -> Self {
        let typ = value.to_type();
        let boxed = Box::new(value);
        let message = boxed.to_string();

        match boxed.as_ref() {
            IoError::FileRead(_)
            | IoError::FileDelete(_)
            | IoError::FileClose(_)
            | IoError::FileWrite(_)
            | IoError::FileWriterCreation(_)
            | IoError::FileCreation(_) => {
                ErrorModel::failed_dependency(message, typ, Some(boxed)).into()
            }

            IoError::UnsupportedCompressionCodec(_) => {
                ErrorModel::bad_request(message, typ, Some(boxed)).into()
            }

            IoError::FileCompression(_) | IoError::Write(_) | IoError::Serialization(_) => {
                ErrorModel::internal(message, typ, Some(boxed)).into()
            }
        }
    }
}
