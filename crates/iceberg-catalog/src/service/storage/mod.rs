mod az;
mod s3;

use std::collections::HashMap;

use super::{secrets::SecretInStorage, NamespaceIdentUuid, TableIdentUuid};
use crate::api::{iceberg::v1::DataAccess, CatalogConfig, ErrorModel, Result};
use crate::service::storage::az::{AzCredential, AzProfile};
use crate::service::tabular_idents::TabularIdentUuid;
use crate::WarehouseIdent;
pub use s3::{S3Credential, S3Profile};
use serde::{Deserialize, Serialize};

/// Storage profile for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageProfile {
    #[serde(rename = "azdls")]
    Az(AzProfile),
    /// S3 storage profile
    #[serde(rename = "s3")]
    S3(S3Profile),
    #[cfg(test)]
    Test(TestProfile),
}

#[derive(Debug, Clone, strum_macros::Display)]
#[allow(clippy::module_name_repetitions)]
pub enum StorageType {
    #[strum(serialize = "s3")]
    S3,
    #[cfg(test)]
    #[strum(serialize = "test")]
    Test,
    Az,
}

#[allow(clippy::module_name_repetitions)]
impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(&self, warehouse_id: WarehouseIdent) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => profile.generate_catalog_config(warehouse_id),
            #[cfg(test)]
            StorageProfile::Test(_) => CatalogConfig {
                overrides: HashMap::default(),
                defaults: HashMap::default(),
            },
            StorageProfile::Az(prof) => prof.generate_catalog_config(warehouse_id),
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
            (StorageProfile::Az(this_profile), StorageProfile::Az(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
            #[cfg(test)]
            (StorageProfile::Test(_), _) => Ok(()),
            #[cfg(test)]
            (_, StorageProfile::Test(_)) => Ok(()),
            (_, _) => Err(ErrorModel::bad_request(
                "Storage profiles are not compatible",
                "StorageProfilesNotCompatible".to_string(),
                None,
            )
            .append_details(&[
                format!("This: {:?}", self.storage_type()),
                format!("Other: {:?}", other.storage_type()),
            ])
            .into()),
        }
    }

    /// Create a new file IO instance for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's file IO creation fails.
    pub fn file_io(&self, secret: Option<&StorageCredential>) -> Result<iceberg::io::FileIO> {
        match self {
            StorageProfile::S3(profile) => profile.file_io(
                secret
                    .map(|s| match s {
                        StorageCredential::S3(s) => Ok(s),
                        _ => Err(ErrorModel::bad_request(
                            "Invalid storage credential, expected s3 but received: {}",
                            "InvalidStorageCredential",
                            None,
                        )),
                    })
                    .transpose()?,
            ),
            #[cfg(test)]
            StorageProfile::Test(_) => {
                let file_io = iceberg::io::FileIOBuilder::new("file")
                    .build()
                    .map_err(|e| {
                        ErrorModel::builder()
                            .code(500)
                            .message("Failed to create file IO".to_string())
                            .r#type("FileIOCreationFailed".to_string())
                            .stack(vec![format!("{:?}", e)])
                            .build()
                    })?;
                Ok(file_io)
            }
            StorageProfile::Az(prof) => prof.file_io(
                secret
                    .map(|s| match s {
                        StorageCredential::Az(s) => Ok(s),
                        StorageCredential::S3(_) => {
                            return Err(ErrorModel::bad_request(
                                "Invalid storage credential, expected az but received: {}",
                                "InvalidStorageCredential",
                                None,
                            ))
                        }
                    })
                    .transpose()?,
            ),
        }
    }

    #[must_use]
    pub fn tabular_location(
        &self,
        namespace_id: NamespaceIdentUuid,
        table_id: TabularIdentUuid,
    ) -> String {
        match self {
            StorageProfile::S3(profile) => profile.tabular_location(namespace_id, table_id),
            #[cfg(test)]
            StorageProfile::Test(_) => format!("/tmp/{namespace_id}/{table_id}"),
            StorageProfile::Az(profile) => profile.tabular_location(namespace_id, table_id),
        }
    }

    #[must_use]
    pub fn metadata_location(&self, table_location: &str, metadata_id: uuid::Uuid) -> String {
        format!(
            "{}/metadata/{metadata_id}.gz.metadata.json",
            table_location.trim_end_matches('/')
        )
    }

    #[must_use]
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageProfile::S3(_) => StorageType::S3,
            #[cfg(test)]
            StorageProfile::Test(_) => StorageType::Test,
            StorageProfile::Az(_) => StorageType::Az,
        }
    }

    /// Generate the table config for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's generation fails.
    pub async fn generate_table_config(
        &self,
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        table_id: TableIdentUuid,
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
                        secret
                            .map(|s| match s {
                                StorageCredential::S3(s) => Ok(s),
                                // TODO error
                                _ => Err(ErrorModel::bad_request(
                                    "Invalid storage credential, expected s3 but received: {}",
                                    "InvalidStorageCredential",
                                    None,
                                )),
                            })
                            .transpose()?,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(HashMap::default()),
            StorageProfile::Az(profile) => {
                profile
                    .generate_table_config(
                        warehouse_id,
                        table_id,
                        namespace_id,
                        data_access,
                        match secret.ok_or_else(|| {
                            ErrorModel::bad_request(
                                "Storage credential is required for Azure storage",
                                "StorageCredentialRequired",
                                None,
                            )
                        })? {
                            StorageCredential::Az(s) => s,
                            // TODO error
                            _ => {
                                return Err(ErrorModel::bad_request(
                                    "Invalid storage credential, expected az but received: {}",
                                    "InvalidStorageCredential",
                                    None,
                                )
                                .into())
                            }
                        },
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
                    .validate(
                        secret
                            .map(|s| match s {
                                StorageCredential::S3(s) => Ok(s),
                                // TODO error
                                _ => Err(ErrorModel::bad_request(
                                    "Invalid storage credential, expected s3 but received: {}",
                                    "InvalidStorageCredential",
                                    None,
                                )),
                            })
                            .transpose()?,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(()),
            StorageProfile::Az(prof) => {
                prof.validate(
                    secret
                        .map(|s| match s {
                            StorageCredential::Az(s) => Ok(s),
                            _ => {
                                // TODO error
                                Err(ErrorModel::bad_request(
                                    "Invalid storage credential, expected az but received: {}",
                                    "InvalidStorageCredential",
                                    None,
                                ))
                            }
                        })
                        .transpose()?,
                )
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
            #[cfg(test)]
            Self::Test(_) => Err(ErrorModel::builder()
                .code(code)
                .message("Storage profile is not S3".to_string())
                .r#type("StorageProfileNotS3".to_string())
                .stack(vec![format!("Storage Type: {}", self.storage_type())])
                .build()
                .into()),
            #[allow(unreachable_patterns)] // More profiles will be added in the future
            _ => Err(ErrorModel::builder()
                .code(code)
                .message("Storage profile is not S3".to_string())
                .r#type("StorageProfileNotS3".to_string())
                .stack(vec![format!("Storage Type: {}", self.storage_type())])
                .build()
                .into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestProfile;

/// Storage secret for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageCredential {
    /// Credentials for S3 storage
    #[serde(rename = "s3")]
    S3(S3Credential),
    #[serde(rename = "az")]
    Az(AzCredential),
}

impl SecretInStorage for StorageCredential {}

impl StorageCredential {
    #[must_use]
    pub fn storage_type(&self) -> StorageType {
        match self {
            StorageCredential::S3(_) => StorageType::S3,
            StorageCredential::Az(_) => StorageType::Az,
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
                .stack(vec![format!("Storage Type: {}", self.storage_type())])
                .build()
                .into()),
        }
    }
}

// ToDo: Move somewhere so that other profiles can use it as well?
async fn validate_file_io(file_io: &iceberg::io::FileIO, test_location: &str) -> Result<()> {
    // Validate the file_io instance by creating a test file.
    crate::catalog::io::write_metadata_file(test_location, "test", file_io).await?;

    file_io.delete(test_location).await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating Storage Profile: {e}"),
            "TestFileDeleteError",
            Some(Box::new(e)),
        )
    })?;

    Ok(())
}

pub mod path_utils {
    use regex_lite::Regex;
    use std::sync::OnceLock;

    static AZDLS_REGEX: OnceLock<Regex> = OnceLock::new();
    static AZDLS_STR: &'static str = r"^(?<protocol>abfss?://)[^/@]+@[^/]+(?<path>/.+)";

    pub fn sanitize_azdls_path(path: &str) -> String {
        let re = AZDLS_REGEX.get_or_init(|| Regex::new(AZDLS_STR).unwrap());
        let caps = re.captures(path).unwrap();
        let mut metadata_location = String::new();
        caps.expand("$protocol$path", &mut metadata_location);
        metadata_location
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
