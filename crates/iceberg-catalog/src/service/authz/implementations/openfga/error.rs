use crate::service::authz::implementations::FgaType;
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use openfga_rs::tonic::metadata::errors::InvalidMetadataValue;
use openfga_rs::tonic::{self, Code};
use openfga_rs::{
    authentication::CredentialRefreshError, ReadRequest, ReadRequestTupleKey, WriteRequest,
};

pub type OpenFGAResult<T> = Result<T, OpenFGAError>;

#[derive(Debug, thiserror::Error)]
pub enum OpenFGAError {
    #[error("Authorization Model ID failed: {reason}")]
    AuthorizationModelIdFailed { reason: String },
    #[error("Client Credential failed: {0}")]
    ClientCredentialFailed(#[from] CredentialRefreshError),
    #[error("Connection to OpenFGA failed: {0}")]
    ConnectionFailed(#[from] tonic::transport::Error),
    #[error("Internal OpenFGA Error: {0}")]
    Internal(tonic::Status),
    #[error("Invalid Bearer Token for OpenFGA: {0}")]
    InvalidBearerToken(InvalidMetadataValue),
    #[error("Listing authentication models failed: {0}")]
    ListAuthenticationModelsFailed(tonic::Status),
    #[error("Listing stores failed: {0}")]
    ListStoresFailed(tonic::Status),
    #[error("Reading tuples failed: {source}")]
    ReadFailed {
        read_request: ReadRequest,
        source: tonic::Status,
    },
    #[error("Store creation failed: {0}")]
    StoreCreationFailed(tonic::Status),
    #[error("Store {store} not found. Please ensure to run migration first.")]
    StoreNotFound { store: String },
    #[error("Too many authorization models in database. Max allowed pages: {0}")]
    TooManyAuthorizationModels(u32),
    #[error("Too many pages")]
    TooManyPages {
        max_pages: u32,
        tuple: ReadRequestTupleKey,
    },
    #[error("Unauthenticated: {0}")]
    Unauthenticated(tonic::Status),
    #[error("Unexpected entity for type {r#type}: {value}")]
    UnexpectedEntity { r#type: FgaType, value: String },
    #[error("Unknown model version currently applied {0}")]
    UnknownModelVersionApplied(i32),
    #[error("Failed to write Authorization model: {0}")]
    WriteAuthorizationModelFailed(tonic::Status),
    #[error("Failed to write Authorization tuples: {source}")]
    WriteFailed {
        write_request: WriteRequest,
        source: tonic::Status,
    },
    #[error("Too many writes and deletes in single Authorization transaction (actual) {actual} > {max} (max)")]
    TooManyWrites { actual: i32, max: i32 },
}

impl OpenFGAError {
    fn known_status(status: &tonic::Status) -> Option<Self> {
        match status.code() {
            Code::Unauthenticated => Some(OpenFGAError::Unauthenticated(status.clone())),
            Code::Internal => Some(OpenFGAError::Internal(status.clone())),
            _ => None,
        }
    }

    pub fn store_creation(status: tonic::Status) -> Self {
        Self::known_status(&status).unwrap_or(OpenFGAError::StoreCreationFailed(status))
    }

    pub fn list_stores(status: tonic::Status) -> Self {
        Self::known_status(&status).unwrap_or(OpenFGAError::ListStoresFailed(status))
    }

    pub fn list_authentication_models(status: tonic::Status) -> Self {
        Self::known_status(&status).unwrap_or(OpenFGAError::ListAuthenticationModelsFailed(status))
    }

    #[must_use]
    pub fn bearer_token(invalid: InvalidMetadataValue) -> Self {
        OpenFGAError::InvalidBearerToken(invalid)
    }

    pub fn write_authorization_model(status: tonic::Status) -> Self {
        Self::known_status(&status).unwrap_or(OpenFGAError::WriteAuthorizationModelFailed(status))
    }
}

impl From<OpenFGAError> for ErrorModel {
    fn from(err: OpenFGAError) -> Self {
        ErrorModel::new(
            err.to_string(),
            "AuthorizationError",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            Some(Box::new(err)),
        )
    }
}

impl From<OpenFGAError> for IcebergErrorResponse {
    fn from(err: OpenFGAError) -> Self {
        let err_model = ErrorModel::from(err);
        err_model.into()
    }
}
