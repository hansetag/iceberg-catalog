mod s3;

use std::collections::HashMap;

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, ErrorModel, Result};
use crate::service::tabular_idents::TabularIdentUuid;
pub use s3::{S3Credential, S3Profile};
use serde::{Deserialize, Serialize};

use crate::WarehouseIdent;

use super::{secrets::SecretInStorage, NamespaceIdentUuid, TableIdentUuid};

/// Storage profile for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageProfile {
    /// S3 storage profile
    #[serde(rename = "s3")]
    S3(S3Profile),
}

#[derive(Debug, Clone, strum_macros::Display)]
#[allow(clippy::module_name_repetitions)]
pub enum StorageType {
    #[strum(serialize = "s3")]
    S3,
}

#[allow(clippy::module_name_repetitions)]
impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(&self, warehouse_id: &WarehouseIdent) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => profile.generate_catalog_config(warehouse_id),
        }
    }

    /// Check if the profile can be updated with the other profile.
    /// This function should fail if the new profile might point to a different location
    /// and thus existing data might be lost.
    ///
    /// # Errors
    /// Fails if the profiles are not compatible, typically because the location changed
    pub fn can_be_updated_with(&self, other: &Self) -> Result<()> {
        match (self, other) {
            (StorageProfile::S3(this_profile), StorageProfile::S3(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
        }
    }

    /// Create a new file IO instance for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's file IO creation fails.
    pub fn file_io(&self, secret: Option<&StorageCredential>) -> Result<iceberg::io::FileIO> {
        match self {
            StorageProfile::S3(profile) => profile.file_io(secret.map(|s| match s {
                StorageCredential::S3(s) => s,
            })),
        }
    }

    #[must_use]
    pub fn tabular_location(
        &self,
        namespace_id: &NamespaceIdentUuid,
        table_id: &TabularIdentUuid,
    ) -> String {
        match self {
            StorageProfile::S3(profile) => profile.tabular_location(namespace_id, table_id),
        }
    }

    #[must_use]
    pub fn metadata_location(&self, table_location: &str, metadata_id: &uuid::Uuid) -> String {
        format!(
            "{}/metadata/{metadata_id}.gz.metadata.json",
            table_location.trim_end_matches('/')
        )
    }

    #[must_use]
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageProfile::S3(_) => StorageType::S3,
        }
    }

    /// Generate the table config for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's generation fails.
    pub async fn generate_table_config(
        &self,
        warehouse_id: &WarehouseIdent,
        namespace_id: &NamespaceIdentUuid,
        table_id: &TableIdentUuid,
        data_access: &DataAccess,
        secret: Option<&StorageCredential>,
    ) -> Result<HashMap<String, String>> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        warehouse_id,
                        table_id,
                        namespace_id,
                        data_access,
                        secret.map(|s| match s {
                            StorageCredential::S3(s) => s,
                        }),
                    )
                    .await
            }
        }
    }

    /// Validate the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's validation fails.
    pub async fn validate(&mut self, secret: Option<&StorageCredential>) -> Result<()> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .validate(secret.map(|s| match s {
                        StorageCredential::S3(s) => s,
                    }))
                    .await
            }
        }
    }

    /// Try to convert the storage profile into an S3 profile.
    ///
    /// # Errors
    /// Fails if the profile is not an S3 profile.
    pub fn try_into_s3(self, code: u16) -> Result<S3Profile> {
        match self {
            Self::S3(profile) => Ok(profile),
            #[allow(unreachable_patterns)] // More profiles will be added in the future
            _ => Err(ErrorModel::builder()
                .code(code)
                .message("Storage profile is not S3".to_string())
                .r#type("StorageProfileNotS3".to_string())
                .stack(Some(vec![format!("Storage Type: {}", self.storage_type())]))
                .build()
                .into()),
        }
    }
}

/// Storage secret for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageCredential {
    /// Credentials for S3 storage
    #[serde(rename = "s3")]
    S3(S3Credential),
}

impl SecretInStorage for StorageCredential {}

impl StorageCredential {
    #[must_use]
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageCredential::S3(_) => StorageType::S3,
        }
    }

    /// Try to convert the credential into an S3 credential.
    ///
    /// # Errors
    /// Fails if the credential is not an S3 credential.
    pub fn try_into_s3(self, code: u16) -> Result<S3Credential> {
        match self {
            Self::S3(profile) => Ok(profile),
            #[allow(unreachable_patterns)] // More profiles will be added in the future
            _ => Err(ErrorModel::builder()
                .code(code)
                .message("Storage profile is not S3".to_string())
                .r#type("StorageProfileNotS3".to_string())
                .stack(Some(vec![format!("Storage Type: {}", self.storage_type())]))
                .build()
                .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact() {
        let secrets: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id: "
                AKIAIOSFODNN7EXAMPLE
            "
            .to_string(),
            aws_secret_access_key: "
                wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
            "
            .to_string(),
        }
        .into();

        let debug_print = format!("{secrets:?}");
        assert!(!debug_print.contains("tnFEMI"));
    }

    #[test]
    fn test_s3_profile_de_from_v1() {
        let value = serde_json::json!({
            "type": "s3",
            "bucket": "my-bucket",
            "endpoint": "http://localhost:9000",
            "region": "us-east-1"
        });

        let profile: StorageProfile = serde_json::from_value(value).unwrap();
        assert_eq!(
            profile,
            StorageProfile::S3(S3Profile {
                bucket: "my-bucket".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                region: "us-east-1".to_string(),
                assume_role_arn: None,
                path_style_access: None,
                key_prefix: None,
            })
        );
    }

    #[test]
    fn test_s3_secret_de_from_v1() {
        let value = serde_json::json!({
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "aws-secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        });

        let secret: StorageCredential = serde_json::from_value(value).unwrap();
        assert_eq!(
            secret,
            StorageCredential::S3(S3Credential::AccessKey {
                aws_access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()
            })
        );
    }
}
