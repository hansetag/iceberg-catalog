#![allow(clippy::match_wildcard_for_single_variants)]

mod az;
mod error;
mod s3;

use std::collections::HashMap;

use super::{secrets::SecretInStorage, NamespaceIdentUuid, TableIdentUuid};
use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::service::tabular_idents::TabularIdentUuid;
use crate::WarehouseIdent;
pub use az::{AzCredential, AzdlsProfile};
use error::{
    ConversionError, CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
pub use s3::{S3Credential, S3Flavor, S3Profile};
use serde::{Deserialize, Serialize};

/// Storage profile for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageProfile {
    /// Azure storage profile
    #[serde(rename = "azdls")]
    Azdls(AzdlsProfile),
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
    #[strum(serialize = "azdls")]
    Azdls,
    #[cfg(test)]
    #[strum(serialize = "test")]
    Test,
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
            StorageProfile::Azdls(prof) => prof.generate_catalog_config(warehouse_id),
        }
    }

    /// Check if the profile can be updated with the other profile.
    /// This function should fail if the new profile might point to a different location
    /// and thus existing data might be lost.
    ///
    /// # Errors
    /// Fails if the profiles are not compatible, typically because the location changed
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
        match (self, other) {
            (StorageProfile::S3(this_profile), StorageProfile::S3(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
            (StorageProfile::Azdls(this_profile), StorageProfile::Azdls(other_profile)) => {
                this_profile.can_be_updated_with(other_profile)
            }
            #[cfg(test)]
            (StorageProfile::Test(_), _) => Ok(()),
            #[cfg(test)]
            (_, StorageProfile::Test(_)) => Ok(()),
            (_, _) => Err(UpdateError::IncompatibleProfiles(
                self.storage_type().to_string(),
                other.storage_type().to_string(),
            )),
        }
    }

    /// Create a new file IO instance for the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's file IO creation fails.
    pub fn file_io(
        &self,
        secret: Option<&StorageCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        match self {
            StorageProfile::S3(profile) => {
                profile.file_io(secret.map(|s| s.try_to_s3()).transpose()?)
            }
            StorageProfile::Azdls(prof) => prof.file_io(secret.map(|s| s.try_to_az()).transpose()?),
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(iceberg::io::FileIOBuilder::new("file").build()?),
        }
    }

    #[must_use]
    pub fn initial_tabular_location(
        &self,
        namespace_id: NamespaceIdentUuid,
        table_id: TabularIdentUuid,
    ) -> String {
        match self {
            StorageProfile::S3(profile) => profile.tabular_location(namespace_id, table_id),
            StorageProfile::Azdls(profile) => {
                profile.initial_tabular_location(namespace_id, table_id)
            }
            #[cfg(test)]
            StorageProfile::Test(_) => format!("/tmp/{namespace_id}/{table_id}"),
        }
    }

    #[must_use]
    pub fn initial_metadata_location(
        &self,
        table_location: &str,
        metadata_id: uuid::Uuid,
    ) -> String {
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
            StorageProfile::Azdls(_) => StorageType::Azdls,
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
        table_location: &str,
    ) -> Result<HashMap<String, String>, TableConfigError> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        warehouse_id,
                        table_id,
                        namespace_id,
                        data_access,
                        table_location,
                        secret.map(|s| s.try_to_s3()).transpose()?,
                    )
                    .await
            }
            StorageProfile::Azdls(profile) => {
                profile
                    .generate_table_config(
                        warehouse_id,
                        table_id,
                        namespace_id,
                        data_access,
                        table_location,
                        secret
                            .ok_or_else(|| {
                                CredentialsError::MissingCredential(self.storage_type())
                            })?
                            .try_to_az()?,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(HashMap::default()),
        }
    }

    /// Validate the storage profile.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's validation fails.
    pub async fn validate(
        &mut self,
        secret: Option<&StorageCredential>,
    ) -> Result<(), ValidationError> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .validate(secret.map(|s| s.try_to_s3()).transpose()?)
                    .await
            }
            StorageProfile::Azdls(prof) => {
                prof.validate(secret.map(|s| s.try_to_az()).transpose()?)
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(()),
        }
    }

    /// Try to convert the storage profile into an S3 profile.
    ///
    /// # Errors
    /// Fails if the profile is not an S3 profile.
    pub fn try_into_s3(self) -> Result<S3Profile, ConversionError> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::S3,
            }),
        }
    }

    /// Try to convert the storage profile into an Az profile.
    ///
    /// # Errors
    /// Fails if the profile is not an Az profile.
    pub fn try_into_az(self) -> Result<AzdlsProfile, ConversionError> {
        match self {
            Self::Azdls(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::Azdls,
            }),
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
            StorageCredential::Az(_) => StorageType::Azdls,
        }
    }

    /// Try to convert the credential into an S3 credential.
    ///
    /// # Errors
    /// Fails if the credential is not an S3 credential.
    pub fn try_to_s3(&self) -> Result<&S3Credential, CredentialsError> {
        match self {
            Self::S3(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::S3,
            }
            .into()),
        }
    }

    /// Try to convert the credential into an Az credential.
    ///
    /// # Errors
    /// Fails if the credential is not an Az credential.
    pub fn try_to_az(&self) -> Result<&AzCredential, CredentialsError> {
        match self {
            Self::Az(profile) => Ok(profile),
            _ => Err(ConversionError {
                is: self.storage_type(),
                to: StorageType::Azdls,
            }
            .into()),
        }
    }
}

pub mod path_utils {
    use lazy_regex::regex;

    /// Reduce the scheme string to only the path.
    #[must_use]
    pub fn reduce_scheme_string(path: &str, only_path: bool) -> String {
        let re = regex!("^(?<protocol>abfss?://)[^/@]+@[^/]+(?<path>/.+)");
        if let Some(caps) = re.captures(path) {
            let mut metadata_location = String::new();
            if only_path {
                caps.expand("$path", &mut metadata_location);
            } else {
                caps.expand("$protocol$path", &mut metadata_location);
            }
            return metadata_location;
        };
        path.to_string()
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
                sts_role_arn: None,
                flavor: S3Flavor::Aws,
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
