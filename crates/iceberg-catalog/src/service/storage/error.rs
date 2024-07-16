use crate::service::storage::{S3Error, StorageType, UserOrInternal};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum Error {
    #[error(transparent)]
    S3Error(#[from] S3Error),
    #[error("{message}, Storage type: {storage_type:?}")]
    InvalidStorageProfile {
        message: String,
        origin: UserOrInternal,
        storage_type: StorageType,
    },
    #[cfg(test)]
    #[error(transparent)]
    FileIOCreationFailed(#[from] iceberg::Error),
}

impl From<Error> for IcebergErrorResponse {
    fn from(value: Error) -> Self {
        ErrorModel::from(value).into()
    }
}

impl From<Error> for ErrorModel {
    fn from(value: Error) -> Self {
        let typ: &'static str = (&value).into();
        match value {
            Error::S3Error(e) => e.into(),
            Error::InvalidStorageProfile {
                ref message,
                ref origin,
                storage_type: _,
            } => match origin {
                UserOrInternal::User => {
                    ErrorModel::conflict(message.clone(), typ, Some(Box::new(value)))
                }
                UserOrInternal::Internal => {
                    ErrorModel::internal(message.clone(), typ, Some(Box::new(value)))
                }
            },
            #[cfg(test)]
            Error::FileIOCreationFailed(_) => ErrorModel::internal(
                "Failed to create file IO".to_string(),
                typ,
                Some(Box::new(value)),
            ),
        }
    }
}
