use crate::api::{ErrorModel, Result};
use flate2::{write::GzEncoder, Compression};
use iceberg::io::FileIO;
use serde::Serialize;
use std::io::Write;

pub(crate) async fn write_metadata_file(
    metadata_location: &str,
    table_metadata: impl Serialize,
    file_io: &FileIO,
) -> Result<(), IoError> {
    let metadata_file =
        file_io
            .new_output(metadata_location)
            .map_err(|e| IoError::FailedDependency {
                message: "Failed to create metadata file. Please check the storage credentials."
                    .to_string(),
                source: Some(Box::new(e)),
            })?;

    let mut writer = metadata_file
        .writer()
        .await
        .map_err(|e| IoError::FailedDependency {
            message: "Failed to create metadata file writer. Please check the storage credentials."
                .to_string(),
            source: Some(Box::new(e)),
        })?;

    let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
    let buf = serde_json::to_vec(&table_metadata).map_err(|e| IoError::InternalError {
        message: "Failed to serialize table metadata.".to_string(),
        source: Some(Box::new(e)),
    })?;

    compressed_metadata
        .write_all(&buf)
        .map_err(|e| IoError::InternalError {
            message: "Failed to compress metadata file.".to_string(),
            source: Some(Box::new(e)),
        })?;

    let compressed_metadata = compressed_metadata
        .finish()
        .map_err(|e| IoError::InternalError {
            message: "Failed to finish compressing metadata file.".to_string(),
            source: Some(Box::new(e)),
        })?;

    writer
        .write(compressed_metadata.into())
        .await
        .map_err(|e| IoError::FailedDependency {
            message: "Failed to write metadata file.".to_string(),
            source: Some(Box::new(e)),
        })?;

    writer
        .close()
        .await
        .map_err(|e| IoError::FailedDependency {
            message: "Failed to close metadata file.".to_string(),
            source: Some(Box::new(e)),
        })?;

    Ok(())
}

#[allow(clippy::module_name_repetitions)]
#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IoError {
    #[error("{message}")]
    FailedDependency {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("{message}")]
    InternalError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl From<IoError> for ErrorModel {
    fn from(value: IoError) -> Self {
        let typ: &'static str = (&value).into();
        match &value {
            IoError::FailedDependency { message, source: _ } => {
                ErrorModel::failed_dependency(message.clone(), typ, Some(Box::new(value)))
            }
            IoError::InternalError { message, source: _ } => {
                ErrorModel::internal(message.clone(), typ, Some(Box::new(value)))
            }
        }
    }
}
