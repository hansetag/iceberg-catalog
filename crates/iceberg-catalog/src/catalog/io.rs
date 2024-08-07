use crate::api::{ErrorModel, Result};
use crate::service::storage::path_utils;
use flate2::{write::GzEncoder, Compression};
use iceberg::{io::FileIO, spec::view_properties::METADATA_COMPRESSION};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;

use super::CommonMetadata;

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum CompressionCodecError {
    #[error("Unsupported compression codec: {0}")]
    UnsupportedCompressionCodec(String),
}

impl CompressionCodecError {
    pub fn to_type(&self) -> &'static str {
        self.into()
    }
}

impl From<CompressionCodecError> for IcebergErrorResponse {
    fn from(value: CompressionCodecError) -> Self {
        let typ = value.to_type();
        let boxed = Box::new(value);
        let message = boxed.to_string();

        match value {
            CompressionCodecError::UnsupportedCompressionCodec(_) => {
                ErrorModel::bad_request(message, typ, Some(boxed)).into()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionCodec {
    None,
    Gzip,
}

impl CompressionCodec {
    pub fn compress(self, payload: &[u8]) -> Result<Vec<u8>, IoError> {
        match self {
            CompressionCodec::None => Ok(payload.to_vec()),
            CompressionCodec::Gzip => {
                let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
                compressed_metadata
                    .write_all(payload)
                    .map_err(IoError::FileCompression)?;

                compressed_metadata
                    .finish()
                    .map_err(IoError::FileCompression)
            }
        }
    }

    pub fn as_file_extension(self) -> &'static str {
        match self {
            CompressionCodec::None => "",
            CompressionCodec::Gzip => ".gz",
        }
    }

    pub fn try_from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, CompressionCodecError> {
        properties
            .get(METADATA_COMPRESSION)
            .map(String::as_str)
            .map_or(Ok(Self::default()), |value| match value {
                "gzip" => Ok(Self::Gzip),
                "none" => Ok(Self::None),
                unknown => Err(CompressionCodecError::UnsupportedCompressionCodec(
                    unknown.into(),
                )),
            })
    }

    pub fn try_from_maybe_properties(
        maybe_properties: Option<&HashMap<String, String>>,
    ) -> Result<Self, CompressionCodecError> {
        match maybe_properties {
            Some(properties) => Self::try_from_properties(properties),
            None => Ok(Self::default()),
        }
    }

    pub fn try_from_metadata<T: CommonMetadata>(
        metadata: &T,
    ) -> Result<Self, CompressionCodecError> {
        Self::try_from_properties(metadata.properties())
    }
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::Gzip
    }
}

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    metadata: impl Serialize + CommonMetadata,
    file_io: &FileIO,
) -> Result<(), IoError> {
    tracing::debug!("Received location: {}", metadata_location);
    let metadata_location = if metadata_location.starts_with("abfs") {
        path_utils::reduce_scheme_string(metadata_location, false)
    } else {
        metadata_location.to_string()
    };
    tracing::debug!("Going to write metadata file to {}", metadata_location);

    let compression_codec =
        CompressionCodec::try_from_metadata(&metadata).map_err(IoError::UnknownCompressionCodec)?;

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
    #[error("Failed to write metadata because of unknown compression codec.")]
    UnknownCompressionCodec(#[source] CompressionCodecError),
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

            IoError::UnknownCompressionCodec(_) => {
                ErrorModel::bad_request(message, typ, Some(boxed)).into()
            }

            IoError::FileCompression(_) | IoError::Write(_) | IoError::Serialization(_) => {
                ErrorModel::internal(message, typ, Some(boxed)).into()
            }
        }
    }
}
