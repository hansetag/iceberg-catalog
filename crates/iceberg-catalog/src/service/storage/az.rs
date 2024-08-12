use crate::{
    catalog::compression_codec::CompressionCodec,
    service::{NamespaceIdentUuid, TableIdentUuid},
    WarehouseIdent,
};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, Result};
use crate::catalog::io::IoError;
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::path_utils::reduce_scheme_string;
use crate::service::storage::{StorageProfile, StorageType};
use crate::service::tabular_idents::TabularIdentUuid;
use azure_storage::prelude::{BlobSasPermissions, BlobSignedResource};
use azure_storage::shared_access_signature::service_sas::BlobSharedAccessSignature;
use azure_storage::shared_access_signature::SasToken;
use azure_storage::StorageCredentials;
use futures::StreamExt;

use iceberg::io::AzdlsConfigKeys;
use iceberg_ext::table_config::{custom, TableConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use url::Url;
use uuid::Uuid;
use veil::Redact;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct AzdlsProfile {
    /// Name of the azdls filesystem, in blobstorage also known as container.
    pub filesystem: String,
    /// Subpath in the filesystem to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
    /// Name of the azure storage account.
    pub account_name: String,
    /// The authority host to use for authentication. Default: `https://login.microsoftonline.com`.
    pub authority_host: Option<Url>,
    /// The endpoint suffix to use for the storage account. Default: `dfs.core.windows.net`.
    pub endpoint_suffix: Option<String>,
    /// The validity of the sas token in seconds. Default: 3600.
    pub sas_token_validity_seconds: Option<u64>,
}

const DEFAULT_ENDPOINT_SUFFIX: &str = "dfs.core.windows.net";

impl AzdlsProfile {
    /// Validate the Azure storage profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the region is too long.
    /// - Fails if the key prefix is too long.
    /// - Fails if the region or endpoint is missing.
    /// - Fails if the endpoint is not a valid URL.
    pub async fn validate(
        &mut self,
        credential: Option<&AzCredential>,
    ) -> Result<(), ValidationError> {
        // If key_prefix is provided, remove any trailing and leading slashes.
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        let AzdlsProfile {
            filesystem,
            key_prefix,
            account_name: _,
            authority_host: _,
            endpoint_suffix,
            sas_token_validity_seconds: _,
        } = self;

        if endpoint_suffix.as_deref().is_some_and(|s| s.contains('/')) {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `endpoint_suffix` must not contain slashes.".to_string(),
                entity: "EndpointSuffix".to_string(),
            });
        }

        is_valid_container_name(filesystem)?;

        // Azure supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = key_prefix {
            if key_prefix.len() > 512 {
                return Err(ValidationError::InvalidProfile {
                    source: None,

                    reason: "Storage Profile `key_prefix` must be less than 512 characters."
                        .to_string(),
                    entity: "KeyPrefix".to_string(),
                });
            }
            is_valid_directory_path(key_prefix)?;
        }

        let file_io = self.file_io(credential)?;
        let ns_id = Uuid::now_v7().into();
        let namespace_location = self.initial_namespace_location(ns_id);
        let table_id = Uuid::now_v7();
        let test_location =
            self.initial_tabular_location(ns_id, TabularIdentUuid::Table(table_id)) + "/test_file";
        let sanitized_ns_location = reduce_scheme_string(&namespace_location, false);

        // Validate the file_io instance by creating a test file.
        let compression_codec = CompressionCodec::try_from_maybe_properties(None)
            .map_err(ValidationError::UnsupportedCompressionCodec)?;
        crate::catalog::io::write_metadata_file(
            &test_location,
            "test",
            compression_codec,
            &file_io,
        )
        .await
        .map_err(|e| {
            ValidationError::IoOperationFailed(e, Box::new(StorageProfile::Azdls(self.clone())))
        })?;

        tracing::debug!(
            "Successfully validated metadata writing, moving on to validating table config.",
        );

        // Validate the table config by generating a sas token and reading the test file using it.
        self.validate_sas(
            credential.ok_or_else(|| CredentialsError::MissingCredential(StorageType::Azdls))?,
            ns_id,
            table_id,
            &test_location,
        )
        .await?;
        tracing::debug!("Successfully validated table_config by using sas token, moving on to cleaning up by deleting contents of: '{}'.", sanitized_ns_location);

        // Test that we can delete the test file
        crate::catalog::io::delete_file(&file_io, &test_location)
            .await
            .map_err(|e| {
                ValidationError::IoOperationFailed(e, Box::new(StorageProfile::Azdls(self.clone())))
            })?;

        // Remove the test namespace location
        file_io
            .remove_all(sanitized_ns_location)
            .await
            .map_err(|e| {
                ValidationError::IoOperationFailed(
                    IoError::FileDelete(e),
                    Box::new(StorageProfile::Azdls(self.clone())),
                )
            })?;

        tracing::debug!("Cleaned up");

        Ok(())
    }

    /// Check if the profile can be updated with the other profile.
    /// `key_prefix`, `region` and `bucket` must be the same.
    /// We enforce this to avoid issues by accidentally changing the bucket or region
    /// of a warehouse, after which all tables would not be accessible anymore.
    /// Changing an endpoint might still result in an invalid profile, but we allow it.
    ///
    /// # Errors
    /// Fails if the `bucket`, `region` or `key_prefix` is different.
    pub fn can_be_updated_with(&self, other: &Self) -> Result<(), UpdateError> {
        if self.filesystem != other.filesystem {
            return Err(UpdateError::ImmutableField("filesystem".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        if self.authority_host != other.authority_host {
            return Err(UpdateError::ImmutableField("authority_host".to_string()));
        }

        if self.endpoint_suffix != other.endpoint_suffix {
            return Err(UpdateError::ImmutableField("endpoint_suffix".to_string()));
        }

        Ok(())
    }

    // may change..
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::default(),
            overrides: HashMap::default(),
        }
    }

    #[must_use]
    pub fn initial_tabular_location(
        &self,
        namespace_id: NamespaceIdentUuid,
        tabular_id: TabularIdentUuid,
    ) -> String {
        format!(
            "{}{tabular_id}",
            self.initial_namespace_location(namespace_id)
        )
    }

    fn initial_namespace_location(&self, namespace_id: NamespaceIdentUuid) -> String {
        format!("{}{namespace_id}/", self.location())
    }

    fn location(&self) -> String {
        if let Some(key_prefix) = &self.key_prefix {
            format!(
                "abfss://{}@{}.{}/{}/",
                self.filesystem,
                self.account_name,
                self.endpoint_suffix
                    .as_deref()
                    .unwrap_or(DEFAULT_ENDPOINT_SUFFIX),
                key_prefix.trim_matches('/')
            )
        } else {
            format!(
                "abfss:/{}@{}.{}/",
                self.filesystem,
                self.account_name,
                self.endpoint_suffix
                    .as_deref()
                    .unwrap_or(DEFAULT_ENDPOINT_SUFFIX)
                    .trim_end_matches('/'),
            )
        }
    }

    /// Generate the table configuration for Azure Datalake Storage Gen2.
    ///
    /// # Errors
    /// Fails if sas token cannot be generated.
    ///
    /// # Panics
    /// This function internally parses "<https://login.microsoftonline.com>" into a `Url`, if this
    /// fails it'll panic which should never happen since "<https://login.microsoftonline.com>" is a
    /// valid `Url`.
    pub async fn generate_table_config(
        &self,
        _: WarehouseIdent,
        _: TableIdentUuid,
        _: NamespaceIdentUuid,
        _: &DataAccess,
        table_location: &str,
        creds: &AzCredential,
    ) -> Result<TableConfig, TableConfigError> {
        let AzCredential::ClientCredentials {
            client_id,
            tenant_id,
            client_secret,
        } = creds;
        let http_client = azure_core::new_http_client();
        let token = azure_identity::ClientSecretCredential::new(
            http_client,
            self.authority_host
                .clone()
                .unwrap_or(Url::parse("https://login.microsoftonline.com").unwrap()),
            tenant_id.clone(),
            client_id.clone(),
            client_secret.clone(),
        );
        let cred = azure_storage::StorageCredentials::token_credential(Arc::new(token));
        let mut config = TableConfig::default();

        let sas = self.get_sas_token(table_location, cred).await?;
        config.insert(&custom::Pair {
            key: self.iceberg_sas_property_key(),
            value: sas,
        });
        Ok(config)
    }

    /// Create a new `FileIO` instance for Azdls.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(
        &self,
        credential: Option<&AzCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("azdls");

        builder = builder
            .with_prop(
                AzdlsConfigKeys::Endpoint,
                format!(
                    "https://{}.{}",
                    self.account_name,
                    self.endpoint_suffix
                        .as_deref()
                        .unwrap_or(DEFAULT_ENDPOINT_SUFFIX)
                ),
            )
            .with_prop(AzdlsConfigKeys::AccountName, self.account_name.clone())
            .with_prop(AzdlsConfigKeys::Filesystem, self.filesystem.clone());

        if let Some(credential) = credential {
            match credential {
                AzCredential::ClientCredentials {
                    client_id,
                    tenant_id,
                    client_secret,
                } => {
                    builder = builder
                        .with_prop(
                            AzdlsConfigKeys::ClientSecret.to_string(),
                            client_secret.to_string(),
                        )
                        .with_prop(AzdlsConfigKeys::ClientId, client_id.to_string())
                        .with_prop(AzdlsConfigKeys::TenantId, tenant_id.to_string());
                    if let Some(authority_host) = &self.authority_host {
                        builder = builder
                            .with_prop(AzdlsConfigKeys::AuthorityHost, authority_host.to_string());
                    }
                }
            }
        }

        Ok(builder.build()?)
    }

    async fn validate_sas(
        &mut self,
        credential: &AzCredential,
        ns_id: NamespaceIdentUuid,
        table_id: Uuid,
        test_location: &str,
    ) -> Result<(), ValidationError> {
        let table_config = self
            .generate_table_config(
                Uuid::now_v7().into(),
                table_id.into(),
                ns_id,
                &DataAccess {
                    vended_credentials: false,
                    remote_signing: false,
                },
                test_location,
                credential,
            )
            .await?;

        // TODO: replace with iceberg_rust's FileIO + opendal once sas is available
        let cred = azure_storage::StorageCredentials::sas_token(
            table_config
                .get_custom_prop(self.iceberg_sas_property_key().as_str())
                .ok_or(CredentialsError::ShortTermCredential {
                    reason: "Couldn't find sas token in table config.".to_string(),
                    source: None,
                })?,
        )
        .map_err(|e| CredentialsError::ShortTermCredential {
            reason: "Error creating azure sas token.".to_string(),
            source: Some(Box::new(e)),
        })?;
        let client =
            azure_storage_blobs::prelude::BlobServiceClient::new(self.account_name.as_str(), cred);
        let container = client.container_client(self.filesystem.as_str());
        let blob = container.blob_client(reduce_scheme_string(test_location, true));
        let mut get = blob.get().into_stream();
        while let Some(n) = get.next().await {
            if let Err(e) = n {
                return Err(ValidationError::IoOperationFailed(
                    IoError::FileRead(Box::new(e)),
                    Box::new(StorageProfile::Azdls(self.clone())),
                ));
            }
        }

        Ok(())
    }

    async fn get_sas_token(
        &self,
        path: &str,
        cred: StorageCredentials,
    ) -> Result<String, CredentialsError> {
        let client =
            azure_storage_blobs::prelude::BlobServiceClient::new(self.account_name.as_str(), cred);
        let delegation_key = client
            .get_user_deligation_key(
                // TODO: should we pull in `time` instead?
                SystemTime::now().into(),
                SystemTime::now()
                    .checked_add(std::time::Duration::from_secs(
                        self.sas_token_validity_seconds.unwrap_or(3600),
                    ))
                    .unwrap()
                    .into(),
            )
            .await
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Error getting azure user delegation key.".to_string(),
                source: Some(Box::new(e)),
            })?;
        let path = reduce_scheme_string(path, true);
        let rootless_path = path.trim_start_matches('/');
        let depth = rootless_path.split('/').count();
        let canonical_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            rootless_path
        );

        let sas = BlobSharedAccessSignature::new(
            delegation_key.user_deligation_key.clone(),
            canonical_resource,
            BlobSasPermissions {
                read: true,
                write: true,
                delete: true,
                list: true,
                ..Default::default()
            },
            delegation_key.user_deligation_key.signed_expiry,
            BlobSignedResource::Directory,
        )
        .signed_directory_depth(depth);

        sas.token()
            .map_err(|e| CredentialsError::ShortTermCredential {
                reason: "Error getting azure sas token.".to_string(),
                source: Some(Box::new(e)),
            })
    }

    fn iceberg_sas_property_key(&self) -> String {
        format!(
            "adls.sas-token.{}.{}",
            self.account_name.as_str(),
            self.endpoint_suffix
                .as_deref()
                .unwrap_or("dfs.core.windows.net")
        )
    }
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum AzCredential {
    #[serde(rename_all = "kebab-case")]
    ClientCredentials {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
}

// https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
fn is_valid_container_name(container: &str) -> Result<(), ValidationError> {
    // Container names must not contain consecutive hyphens.
    if container.contains("--") {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Container name must not contain consecutive hyphens.".to_string(),
            entity: "ContainerName".to_string(),
        });
    }

    let container = container.chars().collect::<Vec<char>>();
    // Container names must be between 3 (min) and 63 (max) characters long.
    if container.len() < 3 || container.len() > 63 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Storage Profile `container` must be between 3 and 63 characters long."
                .to_string(),
            entity: "ContainerName".to_string(),
        });
    }

    // Container names can consist only of lowercase letters, numbers, and hyphens (-).
    if !container
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason:
                "Container name can consist only of lowercase letters, numbers, and hyphens (-)."
                    .to_string(),
            entity: "ContainerName".to_string(),
        });
    }

    // Container names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !container.first().is_some_and(char::is_ascii_alphanumeric)
        || !container.last().is_some_and(char::is_ascii_alphanumeric)
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Container name must begin and end with a letter or number.".to_string(),
            entity: "ContainerName".to_string(),
        });
    }

    Ok(())
}

// https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
fn is_valid_directory_path(path: &str) -> Result<(), ValidationError> {
    for path_segment in path.split('/') {
        // Check if the path contains reserved URL characters that are not properly escaped.
        if path_segment.contains(|c: char| {
            c == ' '
                || c == '!'
                || c == '*'
                || c == '\''
                || c == '('
                || c == ')'
                || c == ';'
                || c == ':'
                || c == '@'
                || c == '&'
                || c == '='
                || c == '+'
                || c == '$'
                || c == ','
                || c == '/'
                || c == '?'
                || c == '%'
                || c == '#'
                || c == '['
                || c == ']'
        }) {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason:
                    "Directory path contains reserved URL characters that are not properly escaped."
                        .to_string(),
                entity: "DirectoryPath".to_string(),
            });
        }

        // Check if the directory name ends with a dot (.), a backslash (\), or a combination of these.
        if path_segment.ends_with('.') || path_segment.ends_with('\\') {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: format!(
                    "Directory: '{path_segment}' must not end with a dot (.), or a backslash (\\)."
                ),
                entity: "DirectoryPath".to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_AZURE)]
    mod azure_tests {
        use crate::service::storage::AzCredential;
        use crate::service::storage::AzdlsProfile;

        #[tokio::test]
        async fn test_can_validate() {
            let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
            let client_id = std::env::var("AZURE_CLIENT_ID").unwrap();
            let client_secret = std::env::var("AZURE_CLIENT_SECRET").unwrap();
            let tenant_id = std::env::var("AZURE_TENANT_ID").unwrap();
            let filesystem = std::env::var("AZURE_STORAGE_FILESYSTEM").unwrap();
            let key_prefix = vec!['b'; 512].into_iter().collect::<String>();
            let mut prof = AzdlsProfile {
                filesystem,
                key_prefix: Some(key_prefix.to_string()),
                account_name,
                authority_host: None,
                endpoint_suffix: None,
                sas_token_validity_seconds: None,
            };

            let cred = AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            };

            prof.validate(Some(&cred))
                .await
                .expect("failed to validate profile");
        }
    }

    use super::*;

    #[test]
    fn test_valid_container_names() {
        for name in &[
            "abc", "a1b2c3", "a-b-c", "1-2-3", "a1-b2-c3", "abc123", "123abc",
        ] {
            assert!(is_valid_container_name(name).is_ok(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_container_length() {
        assert!(is_valid_container_name("ab").is_err(), "ab");
        assert!(
            is_valid_container_name(&"a".repeat(64)).is_err(),
            "64 character long string"
        );
    }

    #[test]
    fn test_invalid_container_characters() {
        for name in &[
            "Abc",     // Uppercase letter
            "abc!",    // Special character
            "abc.def", // Dot character
            "abc_def", // Underscore character
        ] {
            assert!(is_valid_container_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_start_end() {
        for name in &[
            "-abc",   // Starts with hyphen
            "abc-",   // Ends with hyphen
            "-abc-",  // Starts and ends with hyphen
            "1-2-3-", // Ends with hyphen
        ] {
            assert!(is_valid_container_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_consecutive_hyphens_container_name() {
        for name in &[
            "a--b", // Consecutive hyphens
            "1--2", // Consecutive hyphens
            "a--1", // Consecutive hyphens
        ] {
            assert!(is_valid_container_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_valid_directory_paths() {
        for path in &[
            "valid/path",
            "another/valid/path",
            "valid/path/with123",
            "valid/path/with-dash",
            "valid/path/with_underscore",
        ] {
            assert!(is_valid_directory_path(path).is_ok(), "{}", path);
        }
    }

    #[test]
    fn test_path_reserved_characters() {
        for path in &[
            "invalid path",
            "invalid/path!",
            "invalid/path*",
            "invalid/path'",
            "invalid/path(",
            "invalid/path)",
            "invalid/path;",
            "invalid/path:",
            "invalid/path@",
            "invalid/path&",
            "invalid/path=",
            "invalid/path+",
            "invalid/path$",
            "invalid/path,",
            "invalid/path?",
            "invalid/path%",
            "invalid/path#",
            "invalid/path[",
            "invalid/path]",
        ] {
            assert!(is_valid_directory_path(path).is_err(), "{}", path);
        }
    }

    #[test]
    fn test_path_ending_characters() {
        for path in &[
            "invalid/path.",
            "invalid/path\\",
            "invalid/path/.",
            "invalid/path/\\",
        ] {
            assert!(is_valid_directory_path(path).is_err(), "{}", path);
        }
    }
}
