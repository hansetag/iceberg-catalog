use crate::catalog::io::IoError;
use crate::service::storage::{StorageProfile, StorageType};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("{0}")]
    IoOperationFailed(#[source] IoError, Box<StorageProfile>),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
    #[error("{reason}")]
    InvalidProfile {
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
        reason: String,
        entity: String,
    },
    #[error(transparent)]
    FileIoError(#[from] FileIoError),
}

impl From<TableConfigError> for ValidationError {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => e.into(),
        }
    }
}

impl From<ValidationError> for IcebergErrorResponse {
    fn from(value: ValidationError) -> Self {
        match value {
            ValidationError::IoOperationFailed(e, _) => e.into(),
            ValidationError::Credentials(e) => {
                if let CredentialsError::Mismatch(_) = e {
                    ErrorModel::bad_request(
                        e.to_string(),
                        "CredentialMismatchError",
                        Some(Box::new(e)),
                    )
                    .into()
                } else {
                    e.into()
                }
            }
            ValidationError::InvalidProfile {
                source,
                reason,
                entity,
            } => ErrorModel::bad_request(reason, format!("Invalid{entity}"), source).into(),
            ValidationError::FileIoError(e) => e.into(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FileIoError {
    #[error("{0} not supported")]
    UnsupportedAction(String),
    #[error(transparent)]
    FileIoCreationFailed(#[from] iceberg::Error),
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
}

impl From<FileIoError> for IcebergErrorResponse {
    fn from(err: FileIoError) -> Self {
        match &err {
            FileIoError::UnsupportedAction(ref action) => ErrorModel::not_implemented(
                err.to_string(),
                format!("{action}NotSupported"),
                Some(Box::new(err)),
            )
            .into(),
            FileIoError::FileIoCreationFailed(_) => ErrorModel::precondition_failed(
                "Error creating file io",
                "FileIoCreationFailed",
                Some(Box::new(err)),
            )
            .into(),
            e @ FileIoError::Credentials(_) => {
                ErrorModel::internal(e.to_string(), "ConversionError", Some(Box::new(err))).into()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TableConfigError {
    #[error(transparent)]
    Credentials(#[from] CredentialsError),
}

impl From<TableConfigError> for IcebergErrorResponse {
    fn from(value: TableConfigError) -> Self {
        match value {
            TableConfigError::Credentials(e) => e.into(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateError {
    #[error("Field `{0}` cannot be updated to prevent dataloss.")]
    FieldUnchangable(String),
    #[error("Incompatible profiles: {0} cannot be updated with {0}")]
    IncompatibleProfiles(String, String),
}

impl From<UpdateError> for IcebergErrorResponse {
    fn from(value: UpdateError) -> Self {
        ErrorModel::bad_request(value.to_string(), "UpdateError", None).into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CredentialsError {
    #[error("Credential is missing, a credential is required for: {0}")]
    MissingCredential(StorageType),
    #[error("Credential not supported: {0}")]
    UnsupportedCredential(String),
    #[error("Failed to create short-term credential: {reason}")]
    ShortTermCredential {
        reason: String,
        source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    },
    #[error("Failed to convert credential: {0}")]
    Mismatch(#[from] ConversionError),
}

impl From<CredentialsError> for IcebergErrorResponse {
    fn from(value: CredentialsError) -> Self {
        let boxed = Box::new(value);
        let message = boxed.to_string();
        match boxed.as_ref() {
            CredentialsError::ShortTermCredential { .. } => {
                ErrorModel::precondition_failed(message, "ShortTermCredentialError", Some(boxed))
                    .into()
            }
            CredentialsError::Mismatch(_) => {
                ErrorModel::internal(message, "CredentialMismatchError", Some(boxed)).into()
            }
            CredentialsError::MissingCredential(_) => {
                ErrorModel::bad_request(message, "MissingCredentialError", Some(boxed)).into()
            }
            CredentialsError::UnsupportedCredential(_) => {
                ErrorModel::not_implemented(message, "UnsupportedCredentialError", Some(boxed))
                    .into()
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to convert to {to}, is: {is}")]
pub struct ConversionError {
    pub is: StorageType,
    pub to: StorageType,
}

impl From<ConversionError> for IcebergErrorResponse {
    fn from(value: ConversionError) -> Self {
        ErrorModel::internal(
            format!(
                "Failed to convert'{is}' to '{to}'",
                to = value.to,
                is = value.is
            ),
            "ConversionError",
            Some(Box::new(value)),
        )
        .into()
    }
}
