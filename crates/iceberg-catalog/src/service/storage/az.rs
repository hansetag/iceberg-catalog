use crate::{
    service::{NamespaceIdentUuid, TableIdentUuid},
    WarehouseIdent,
};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, ErrorModel, Result};
use crate::service::storage;
use crate::service::tabular_idents::TabularIdentUuid;
use azure_storage::prelude::{BlobSasPermissions, BlobSignedResource};
use azure_storage::shared_access_signature::service_sas::BlobSharedAccessSignature;
use azure_storage::shared_access_signature::SasToken;
use azure_storage::StorageCredentials;
use futures::StreamExt;
use iceberg::io::azdls;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use url::Url;
use uuid::Uuid;
use veil::Redact;

fn is_valid_bucket_name(bucket: &str) -> Result<()> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(ErrorModel::bad_request(
            "Bucket name must be between 3 and 63 characters long.",
            "InvalidBucketName",
            None,
        )
        .into());
    }

    // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(ErrorModel::bad_request(
            "Bucket name can consist only of lowercase letters, numbers, dots (.), and hyphens (-).",
            "InvalidBucketName",
            None,
        ).into());
    }

    // Bucket names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !bucket.chars().next().unwrap().is_ascii_alphanumeric()
        || !bucket.chars().last().unwrap().is_ascii_alphanumeric()
    {
        return Err(ErrorModel::bad_request(
            "Bucket name must begin and end with a letter or number.",
            "InvalidBucketName",
            None,
        )
        .into());
    }

    // Bucket names must not contain two adjacent periods.
    if bucket.contains("..") {
        return Err(ErrorModel::bad_request(
            "Bucket name must not contain two adjacent periods.",
            "InvalidBucketName",
            None,
        )
        .into());
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct AzProfile {
    /// Name of the S3 bucket
    pub filesystem: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
    pub account_name: String,
    pub authority_host: Option<Url>,
    pub endpoint_suffix: Option<String>,
    pub sas_token_validity_seconds: Option<u64>,
}

const DEFAULT_ENDPOINT_SUFFIX: &'static str = "dfs.core.windows.net";

impl AzProfile {
    /// Validate the S3 profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the region is too long.
    /// - Fails if the key prefix is too long.
    /// - Fails if the region or endpoint is missing.
    /// - Fails if the endpoint is not a valid URL.
    pub async fn validate(&mut self, credential: Option<&AzCredential>) -> Result<()> {
        // If key_prefix is provided, remove any trailing and leading slashes.
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        let AzProfile {
            filesystem,
            key_prefix,
            account_name: _,
            authority_host: _,
            endpoint_suffix: _,
            sas_token_validity_seconds: _,
        } = self;

        is_valid_bucket_name(filesystem)?;

        // Aws supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = key_prefix {
            if key_prefix.len() > 512 {
                return Err(ErrorModel::bad_request(
                    "Storage Profile `key_prefix` must be less than 512 characters.",
                    "InvalidKeyPrefix",
                    None,
                )
                .into());
            }
        }

        let file_io = self.file_io(credential)?;
        let ns_id = uuid::Uuid::now_v7().into();
        let namespace_location = self.namespace_location(ns_id);
        let table_id = uuid::Uuid::now_v7();
        let test_location =
            self.tabular_location(ns_id, TabularIdentUuid::Table(table_id)) + "/test_file";
        let sanitized_ns_location = super::path_utils::sanitize_azdls_path(&namespace_location);
        storage::validate_file_io(&file_io, &test_location)
            .await
            .map_err(|e| e.error.append_detail(format!("Profile: {self:?}")))?;

        tracing::debug!(
            "Successfully validated metadata writing, moving on to validating table config.",
        );

        self.validate_sas(credential, ns_id, table_id, test_location)
            .await?;

        tracing::debug!("Successfully validated table_config by using sas token, moving on to cleaning up by deleting contents of: '{}'.", sanitized_ns_location);

        file_io
            .remove_all(sanitized_ns_location)
            .await
            .map_err(|err| {
                ErrorModel::internal(
                    "Failed to delete test location.",
                    "AzStorageDeletionError",
                    Some(Box::new(err)),
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
    pub fn can_be_updated_with(&self, other: &Self) -> Result<()> {
        if self.filesystem != other.filesystem {
            return Err(ErrorModel::bad_request(
                "Storage Profile `filesystem` cannot be updated to prevent data loss.",
                "InvalidFileSystem",
                None,
            )
            .into());
        }

        if self.key_prefix != other.key_prefix {
            return Err(ErrorModel::bad_request(
                "Storage Profile `key_prefix` cannot be updated to prevent data loss.",
                "InvalidKeyPrefix",
                None,
            )
            .into());
        }

        Ok(())
    }

    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::default(),
            overrides: HashMap::default(),
        }
    }

    #[must_use]
    pub fn tabular_location(
        &self,
        namespace_id: NamespaceIdentUuid,
        tabular_id: TabularIdentUuid,
    ) -> String {
        format!("{}{tabular_id}", self.namespace_location(namespace_id))
    }

    fn namespace_location(&self, namespace_id: NamespaceIdentUuid) -> String {
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
                key_prefix.trim_matches('/').to_string()
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

    // Vended credentials will be async.
    #[allow(clippy::unused_async)]
    /// Generate the table configuration for S3.
    ///
    /// # Errors
    /// Fails if vended credentials are used - currently not supported.
    pub async fn generate_table_config(
        &self,
        _: WarehouseIdent,
        _: TableIdentUuid,
        _: NamespaceIdentUuid,
        _: &DataAccess,
        creds: &AzCredential,
    ) -> Result<HashMap<String, String>> {
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
        let mut config = HashMap::new();

        let sas = self.get_sas_token(cred).await?;
        config.insert(self.iceberg_sas_property_key(), sas);
        Ok(config)
    }

    /// Create a new `FileIO` instance for S3.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(&self, credential: Option<&AzCredential>) -> Result<iceberg::io::FileIO> {
        let mut builder = iceberg::io::FileIOBuilder::new("azdls");

        builder = builder
            .with_prop(
                azdls::ConfigKeys::Endpoint,
                format!(
                    "https://{}.{}",
                    self.account_name,
                    self.endpoint_suffix
                        .as_deref()
                        .unwrap_or(DEFAULT_ENDPOINT_SUFFIX)
                ),
            )
            .with_prop(azdls::ConfigKeys::AccountName, self.account_name.clone())
            .with_prop(azdls::ConfigKeys::Filesystem, self.filesystem.clone());

        if let Some(credential) = credential {
            match credential {
                AzCredential::ClientCredentials {
                    client_id,
                    tenant_id,
                    client_secret,
                } => {
                    builder = builder
                        .with_prop(
                            azdls::ConfigKeys::ClientSecret.to_string(),
                            client_secret.to_string(),
                        )
                        .with_prop(azdls::ConfigKeys::ClientId, client_id.to_string())
                        .with_prop(azdls::ConfigKeys::TenantId, tenant_id.to_string());
                    if let Some(authority_host) = &self.authority_host {
                        builder = builder.with_prop(
                            azdls::ConfigKeys::AuthorityHost,
                            authority_host.to_string(),
                        );
                    }
                }
            }
        }

        builder.build().map_err(|e| {
            ErrorModel::precondition_failed(
                "Error creating S3 filesystem.",
                "S3FileIOError",
                Some(Box::new(e)),
            )
            .into()
        })
    }

    async fn validate_sas(
        &mut self,
        credential: Option<&AzCredential>,
        ns_id: NamespaceIdentUuid,
        table_id: Uuid,
        test_location: String,
    ) -> Result<()> {
        let table_config = self
            .generate_table_config(
                Uuid::now_v7().into(),
                table_id.into(),
                ns_id.into(),
                &DataAccess {
                    vended_credentials: false,
                    remote_signing: false,
                },
                credential.unwrap(),
            )
            .await
            .map_err(|e| {
                e.error
                    .append_detail("Couldn't generate table config while validating.")
            })?;

        // TODO: replace with iceberg_rust's FileIO + opendal once sas is available
        let cred = azure_storage::StorageCredentials::sas_token(
            table_config
                .get(self.iceberg_sas_property_key().as_str())
                .ok_or(ErrorModel::internal(
                    "Couldn't find sas token in table config.",
                    "AzStorageSasTokenNotFound",
                    None,
                ))?,
        )
        .map_err(|e| {
            ErrorModel::internal(
                "Couldn't create storage credentials from sas token.",
                "AzStorageSasTokenCreationError",
                Some(Box::new(e)),
            )
        })?;
        let client =
            azure_storage_blobs::prelude::BlobServiceClient::new(self.account_name.as_str(), cred);
        let container = client.container_client(self.filesystem.as_str());
        let blob = container.blob_client(
            // Blob client expects the path without protocol
            super::path_utils::sanitize_azdls_path(test_location.as_str())
                .trim_start_matches("abfss://"),
        );
        let mut get = blob.get().into_stream();
        while let Some(n) = get.next().await {
            if let Err(e) = n {
                return Err(ErrorModel::internal(
                    "Failed to read test location.",
                    "AzStorageReadError",
                    Some(Box::new(e)),
                )
                .into());
            }
        }

        Ok(())
    }

    async fn get_sas_token(&self, cred: StorageCredentials) -> Result<String> {
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
            .map_err(|e| {
                ErrorModel::precondition_failed(
                    "Error getting user delegation key.",
                    "AzStorageDelegationError",
                    Some(Box::new(e)),
                )
            })?;

        let depth = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').count())
            .unwrap_or(1);
        let canonicalized_resource = format!(
            "/blob/{}/{}/{}",
            self.account_name.as_str(),
            self.filesystem.as_str(),
            self.key_prefix.as_deref().unwrap_or("")
        );

        let sas = BlobSharedAccessSignature::new(
            delegation_key.user_deligation_key.clone(),
            canonicalized_resource,
            BlobSasPermissions {
                read: true,
                // create: true,
                write: true,
                delete: true,
                list: true,
                ..Default::default()
            },
            delegation_key.user_deligation_key.signed_expiry,
            BlobSignedResource::Directory,
        )
        .signed_directory_depth(depth);

        sas.token().map_err(|e| {
            ErrorModel::precondition_failed(
                "Error getting sas token.",
                "AzStorageDelegationError",
                Some(Box::new(e)),
            )
            .into()
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

#[cfg(test)]
mod test {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_AZURE)]
    mod azure_tests {
        use crate::service::storage::AzCredential;
        use crate::service::storage::AzProfile;
        use uuid::Uuid;

        #[tokio::test]
        async fn test_can_validate() {
            let account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
            let client_id = std::env::var("AZURE_CLIENT_ID").unwrap();
            let client_secret = std::env::var("AZURE_CLIENT_SECRET").unwrap();
            let tenant_id = std::env::var("AZURE_TENANT_ID").unwrap();
            let filesystem = std::env::var("AZURE_STORAGE_FILESYSTEM").unwrap();
            let key_prefix = Uuid::now_v7();
            eprintln!("");
            let mut prof = AzProfile {
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
}
