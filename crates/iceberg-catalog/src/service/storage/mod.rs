#![allow(clippy::match_wildcard_for_single_variants)]

mod az;
mod error;
mod s3;

use super::{secrets::SecretInStorage, NamespaceIdentUuid, TableIdentUuid};
use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::catalog::compression_codec::CompressionCodec;
use crate::catalog::io::{list_location, DEFAULT_LIST_LOCATION_PAGE_SIZE};
use crate::service::tabular_idents::TabularIdentUuid;
use crate::WarehouseIdent;
pub use az::{AzCredential, AzdlsLocation, AzdlsProfile};
pub(crate) use error::ValidationError;
use error::{ConversionError, CredentialsError, FileIoError, TableConfigError, UpdateError};
use futures::StreamExt;
use iceberg::io::FileIO;
use iceberg_ext::configs::table::TableProperties;
use iceberg_ext::configs::Location;
pub use s3::S3Location;
pub use s3::{S3Credential, S3Flavor, S3Profile};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Storage profile for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
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

pub enum StorageType {
    #[strum(serialize = "s3")]
    S3,
    #[strum(serialize = "azdls")]
    Azdls,
    #[cfg(test)]
    #[strum(serialize = "test")]
    Test,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum StoragePermissions {
    Read,
    ReadWrite,
    ReadWriteDelete,
}

impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(&self, warehouse_id: WarehouseIdent) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => profile.generate_catalog_config(warehouse_id),
            #[cfg(test)]
            StorageProfile::Test(_) => {
                use std::collections::HashMap;
                CatalogConfig {
                    overrides: HashMap::default(),
                    defaults: HashMap::default(),
                }
            }
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
            StorageProfile::S3(profile) => profile.file_io(
                secret
                    .map(|s| s.try_to_s3())
                    .transpose()?
                    .map(Into::into)
                    .as_ref(),
            ),
            StorageProfile::Azdls(prof) => prof.file_io(secret.map(|s| s.try_to_az()).transpose()?),
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(iceberg::io::FileIOBuilder::new("file").build()?),
        }
    }

    /// Get the base location of this Storage Profiles
    ///
    /// # Errors
    /// Can fail for un-normalized profiles.
    pub fn base_location(&self) -> Result<Location, ValidationError> {
        match self {
            StorageProfile::S3(profile) => profile.base_location().map(Into::into),
            StorageProfile::Azdls(profile) => profile.base_location(),
            #[cfg(test)]
            StorageProfile::Test(_) => std::str::FromStr::from_str("file://tmp/").map_err(|_| {
                ValidationError::InvalidLocation {
                    reason: "Invalid namespace location".to_string(),
                    location: "file://tmp/".to_string(),
                    source: None,
                    storage_type: self.storage_type(),
                }
            }),
        }
    }

    /// Get the default location for the namespace.
    ///
    /// # Errors
    /// Fails if the `key_prefix` is not valid for S3 URLs.
    pub fn default_namespace_location(
        &self,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<Location, ValidationError> {
        let mut base_location: Location = self.base_location()?;
        base_location
            .without_trailing_slash()
            .push(&namespace_id.to_string());
        Ok(base_location)
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
        data_access: &DataAccess,
        secret: Option<&StorageCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableProperties, TableConfigError> {
        match self {
            StorageProfile::S3(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        secret.map(|s| s.try_to_s3()).transpose()?,
                        table_location,
                        storage_permissions,
                    )
                    .await
            }
            StorageProfile::Azdls(profile) => {
                profile
                    .generate_table_config(
                        data_access,
                        table_location,
                        secret
                            .ok_or_else(|| {
                                CredentialsError::MissingCredential(self.storage_type())
                            })?
                            .try_to_az()?,
                        storage_permissions,
                    )
                    .await
            }
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(TableProperties::default()),
        }
    }

    /// Try to normalize the storage profile.
    /// Fails if some validation fails. This does not check physical filesystem access.
    ///
    /// # Errors
    /// Fails if the underlying storage profile's normalization fails.
    pub fn normalize(&mut self) -> Result<(), ValidationError> {
        // ------------- Common validations -------------
        // Test if we can generate a default namespace location
        let ns_location = self.default_namespace_location(NamespaceIdentUuid::default())?;
        self.default_tabular_location(&ns_location, TableIdentUuid::default().into());

        // ------------- Profile specific validations -------------
        match self {
            StorageProfile::S3(profile) => profile.normalize(),
            StorageProfile::Azdls(prof) => prof.normalize(),
            #[cfg(test)]
            StorageProfile::Test(_) => Ok(()),
        }
    }

    /// Validate physical access
    ///
    /// If location is not provided, a dummy table location is used.
    ///
    /// # Errors
    /// Fails if a file cannot be written and deleted.
    pub async fn validate_access(
        &self,
        credential: Option<&StorageCredential>,
        location: Option<&Location>,
    ) -> Result<(), ValidationError> {
        let file_io = self.file_io(credential)?;

        let ns_id = NamespaceIdentUuid::default();
        let table_id = TableIdentUuid::default();
        let ns_location = self.default_namespace_location(ns_id)?;
        let test_location = location.map_or_else(
            || self.default_tabular_location(&ns_location, table_id.into()),
            std::borrow::ToOwned::to_owned,
        );
        // Validate direct read/write access
        self.validate_read_write(&file_io, &test_location, false)
            .await?;

        // Test vended-credentials access
        let test_vended_credentials = match self {
            StorageProfile::S3(profile) => profile.sts_enabled,
            StorageProfile::Azdls(_) => true,
            #[cfg(test)]
            StorageProfile::Test(_) => false,
        };

        if test_vended_credentials {
            let tbl_config = self
                .generate_table_config(
                    &DataAccess {
                        remote_signing: false,
                        vended_credentials: true,
                    },
                    credential,
                    &test_location,
                    StoragePermissions::ReadWriteDelete,
                )
                .await?;
            match &self {
                StorageProfile::S3(_) => {
                    let sts_file_io = s3::get_file_io_from_table_config(&tbl_config)?;
                    self.validate_read_write(&sts_file_io, &test_location, true)
                        .await?;
                }
                StorageProfile::Azdls(_) => {
                    az::validate_vended_credentials(&tbl_config, &test_location, self).await?;
                }
                #[cfg(test)]
                StorageProfile::Test(_) => {}
            }
        }
        tracing::info!("Cleanup started");
        // Cleanup
        crate::catalog::io::remove_all(&file_io, &test_location)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        tracing::info!("Cleanup finished");

        check_location_is_empty(&file_io, &test_location, self, || {
            ValidationError::InvalidLocation {
                reason: "Files are left after remove_all on test location".to_string(),
                source: None,
                location: test_location.to_string(),
                storage_type: self.storage_type(),
            }
        })
        .await?;
        tracing::info!("checked location is empty");
        Ok(())
    }

    async fn validate_read_write(
        &self,
        file_io: &iceberg::io::FileIO,
        test_location: &Location,
        is_vended_credentials: bool,
    ) -> Result<(), ValidationError> {
        let compression_codec = CompressionCodec::Gzip;

        let mut test_file_write =
            self.default_metadata_location(test_location, &compression_codec, uuid::Uuid::now_v7());
        if is_vended_credentials {
            test_file_write.push("test_vended_credentials");
            tracing::debug!(
                "Validating vended credential access to: {}",
                test_file_write
            );
        } else {
            test_file_write.pop().push("test");
            tracing::debug!("Validating access to: {}", test_file_write);
        }

        // Test write
        crate::catalog::io::write_metadata_file(
            &test_file_write,
            "test",
            compression_codec,
            file_io,
        )
        .await
        .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        // Test read
        let _ = crate::catalog::io::read_file(file_io, &test_file_write)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        // Test delete
        crate::catalog::io::delete_file(file_io, &test_file_write)
            .await
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(self.clone())))?;

        tracing::debug!(
            "Successfully wrote, read and deleted file at: {}",
            test_file_write
        );

        Ok(())
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

    #[must_use]
    pub fn is_allowed_location(&self, other: &Location) -> bool {
        let base_location = self.base_location().ok();

        if let Some(mut base_location) = base_location {
            base_location.with_trailing_slash();
            if other == &base_location {
                return false;
            }

            other.is_sublocation_of(&base_location)
        } else {
            false
        }
    }
}

pub trait StorageLocations {
    /// Get the default tabular location for the storage profile.
    fn default_tabular_location(
        &self,
        namespace_location: &Location,
        table_id: TabularIdentUuid,
    ) -> Location {
        let mut l = namespace_location.clone();
        l.without_trailing_slash().push(&table_id.to_string());
        l
    }

    #[must_use]
    /// Get the default metadata location for the storage profile.
    fn default_metadata_location(
        &self,
        table_location: &Location,
        compression_codec: &CompressionCodec,
        metadata_id: uuid::Uuid,
    ) -> Location {
        let filename_extension_compression = compression_codec.as_file_extension();
        let filename = format!("{metadata_id}{filename_extension_compression}.metadata.json",);
        let mut l = table_location.clone();

        l.without_trailing_slash().extend(&["metadata", &filename]);
        l
    }
}

impl StorageLocations for StorageProfile {}
impl StorageLocations for S3Profile {}
impl StorageLocations for AzdlsProfile {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestProfile;

/// Storage secret for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
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

pub(crate) async fn check_location_is_empty(
    file_io: &FileIO,
    location: &Location,
    storage_profile: &StorageProfile,
    error_fn: impl FnOnce() -> ValidationError,
) -> Result<(), ValidationError> {
    tracing::info!("Checking location is empty: {location}",);
    let mut entry_stream = list_location(file_io, location, DEFAULT_LIST_LOCATION_PAGE_SIZE)
        .await
        .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone())))?;
    while let Some(entries) = entry_stream.next().await {
        tracing::debug!("Got page: {entries:?}");
        let entries = entries.map_err(|e| {
            ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone()))
        })?;

        if !entries.is_empty() {
            tracing::debug!("Location is not empty: {location}, entries: {entries:?}",);
            let er = error_fn();
            return Err(er);
        }
    }
    Ok(())
}

pub(crate) async fn ensure_location_content_matches(
    file_io: &FileIO,
    location: &Location,
    expected: &[&str],
    storage_profile: &StorageProfile,
) -> Result<(), ValidationError> {
    let mut base_location = storage_profile.base_location()?;
    base_location.with_trailing_slash();

    let mut expected_set = expected
        .iter()
        .filter_map(|s| {
            let p = s.split('/').next_back().map(ToString::to_string);
            if p.as_deref() == Some("") {
                None
            } else {
                p
            }
        })
        .collect::<HashSet<String>>();
    let mut unexpected = vec![];

    // TODO: come up with a better way than just taking the filenames.
    //       stripping location doesn't work since listing doesn't prepend the fs + root directory
    //       and we can't just strip the base location since it might contain a key-prefix
    //       we're mostly using metadata files here and they contain uuids so we're probably fine..
    let mut entry_stream = list_location(file_io, location, DEFAULT_LIST_LOCATION_PAGE_SIZE)
        .await
        .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone())))?;
    while let Some(item) = entry_stream.next().await {
        tracing::info!("Got page: {item:?}",);
        let entries = item
            .map_err(|e| ValidationError::IoOperationFailed(e, Box::new(storage_profile.clone())))?
            .into_iter()
            .filter_map(|s| {
                // azdls has directories in lists, we only want files
                let p = s.split('/').next_back().map(ToString::to_string);
                if p.as_deref() == Some("") {
                    None
                } else {
                    p
                }
            });

        for entry in entries {
            if !expected_set.remove(&entry) {
                unexpected.push(entry);
            }
        }
    }

    let missing = expected_set;

    if !unexpected.is_empty() || !missing.is_empty() {
        tracing::warn!(
            "Location '{}' is not usable: Unexpected {unexpected:?}, missing: {missing:?}, expected: {expected:?}", location
        );
    }

    if !unexpected.is_empty() {
        return Err(ValidationError::InvalidLocation {
            reason: "Unexpected files in location, tabular locations have to be empty, found more files than expected after writing.".to_string(),
            location: location.to_string(),
            source: None,
            storage_type: storage_profile.storage_type(),
        });
    }

    if !missing.is_empty() {
        return Err(ValidationError::InvalidLocation {
            reason: "Written files could not be found, please check storage permissions."
                .to_string(),
            location: location.to_string(),
            source: None,
            storage_type: storage_profile.storage_type(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_reduce_scheme_string() {
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        let reduced_path = path_utils::reduce_scheme_string(path, true);
        assert_eq!(reduced_path, "/path/_test");

        let reduced_path = path_utils::reduce_scheme_string(path, false);
        // ToDo Tobi: Is this correct?
        assert_eq!(reduced_path, "abfss:///path/_test");
    }

    #[test]
    fn test_default_locations() {
        let profile = StorageProfile::S3(S3Profile {
            bucket: "my-bucket".to_string(),
            endpoint: Some("http://localhost:9000".parse().unwrap()),
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: Some("subfolder".to_string()),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        });

        let target_location = "s3://my-bucket/subfolder/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";

        let namespace_id: NamespaceIdentUuid =
            uuid::uuid!("00000000-0000-0000-0000-000000000001").into();
        let namespace_location = profile.default_namespace_location(namespace_id).unwrap();
        let table_id = TabularIdentUuid::View(uuid::uuid!("00000000-0000-0000-0000-000000000002"));
        let table_location = profile.default_tabular_location(&namespace_location, table_id);
        assert_eq!(table_location.to_string(), target_location);

        let mut namespace_location_without_slash = namespace_location.clone();
        namespace_location_without_slash.without_trailing_slash();
        let table_location =
            profile.default_tabular_location(&namespace_location_without_slash, table_id);
        assert!(!namespace_location_without_slash.to_string().ends_with('/'));
        assert_eq!(table_location.to_string(), target_location);
    }

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
            "region": "us-east-1",
            "sts-enabled": false,
        });

        let profile: StorageProfile = serde_json::from_value(value).unwrap();
        assert_eq!(
            profile,
            StorageProfile::S3(S3Profile {
                bucket: "my-bucket".to_string(),
                endpoint: Some("http://localhost:9000".parse().unwrap()),
                region: "us-east-1".to_string(),
                assume_role_arn: None,
                path_style_access: None,
                key_prefix: None,
                sts_role_arn: None,
                sts_enabled: false,
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

    #[test]
    fn test_is_allowed_location() {
        let profile = StorageProfile::S3(S3Profile {
            bucket: "my.bucket".to_string(),
            endpoint: Some("http://localhost:9000".parse().unwrap()),
            region: "us-east-1".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: Some("my/subpath".to_string()),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        });

        let cases = vec![
            ("s3://my.bucket/my/subpath/ns-id", true),
            ("s3://my.bucket/my/subpath/ns-id/", true),
            ("s3://my.bucket/my/subpath/ns-id/tbl-id", true),
            ("s3://my.bucket/my/subpath/ns-id/tbl-id/", true),
            ("s3://other.bucket/my/subpath/ns-id/tbl-id/", false),
            ("s3://my.bucket/other/subpath/ns-id/tbl-id/", false),
            // Exact path should not be accepted
            ("s3://my.bucket/my/subpath", false),
            ("s3://my.bucket/my/subpath/", false),
        ];

        for (sublocation, expected_result) in cases {
            let sublocation = Location::from_str(sublocation).unwrap();
            assert_eq!(
                profile.is_allowed_location(&sublocation),
                expected_result,
                "Base Location: {}, Maybe sublocation: {sublocation}",
                profile.base_location().unwrap(),
            );
        }
    }
}
