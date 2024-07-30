use crate::{
    service::{NamespaceIdentUuid, TableIdentUuid},
    WarehouseIdent, CONFIG,
};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, ErrorModel, Result};
use crate::service::storage::validate_file_io;
use crate::service::tabular_idents::TabularIdentUuid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;
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
    pub bucket: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
    pub account_name: String,
}

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
            bucket,
            key_prefix,
            account_name: _,
        } = self;

        is_valid_bucket_name(bucket)?;

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
        let test_location = self.tabular_location(
            uuid::Uuid::now_v7().into(),
            TabularIdentUuid::Table(uuid::Uuid::now_v7()),
        );

        validate_file_io(Arc::new(file_io), &test_location)
            .await
            .map_err(|e| e.error.append_detail(format!("Profile: {self:?}")))?;

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
        if self.bucket != other.bucket {
            return Err(ErrorModel::bad_request(
                "Storage Profile `bucket` cannot be updated to prevent data loss.",
                "InvalidBucket",
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
    pub fn generate_catalog_config(&self, warehouse_id: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            // ToDo: s3.delete-enabled?
            defaults: HashMap::default(),
            overrides: HashMap::from_iter(vec![(
                "s3.signer.uri".to_string(),
                CONFIG.s3_signer_uri_for_warehouse(warehouse_id).to_string(),
            )]),
        }
    }

    #[must_use]
    pub fn tabular_location(
        &self,
        namespace_id: NamespaceIdentUuid,
        tabular_id: TabularIdentUuid,
    ) -> String {
        if let Some(key_prefix) = &self.key_prefix {
            format!("/{key_prefix}/{namespace_id}/{tabular_id}")
        } else {
            format!("/{namespace_id}/{tabular_id}")
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
        data_access: &DataAccess,
        _: Option<&AzCredential>,
    ) -> Result<HashMap<String, String>> {
        let DataAccess {
            vended_credentials,
            remote_signing,
        } = data_access;
        // If vended_credentials is False and remote_signing is False,
        // use remote_signing.
        let mut remote_signing = if !vended_credentials && !remote_signing {
            true
        } else {
            *remote_signing
        };

        let mut config = HashMap::new();

        if *vended_credentials {
            // ToDo: Find a better way.
            // Vended-Credentials are requested by pyiceberg. However, we can trick pyiceberg in using
            // remote signing by setting the following config keys:
            config.insert("s3.signer".to_string(), "S3V4RestSigner".to_string());
            config.insert(
                "py-io-impl".to_string(),
                "pyiceberg.io.fsspec.FsspecFileIO".to_string(),
            );
            remote_signing = true;

            // return Err(ErrorModel::builder()
            //     .code(StatusCode::NOT_IMPLEMENTED.into())
            //     .message("Vended credentials not supported.".to_string())
            //     .r#type("VendedCredentialsNotSupported".to_string())
            //     .build()
            //     .into());
        }

        if remote_signing {
            config.insert("s3.remote-signing-enabled".to_string(), "true".to_string());
            // Currently per-table signer uris are not supported by Spark.
            // The URI is cached for one table, and then re-used for another.
            // let signer_uri = CONFIG.s3_signer_uri_for_table(warehouse_id, namespace_id, table_id);
            // config.insert("s3.signer.uri".to_string(), signer_uri.to_string());
        }

        Ok(config)
    }

    /// Create a new `FileIO` instance for S3.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(
        &self,
        credential: Option<&AzCredential>,
    ) -> Result<object_store::azure::MicrosoftAzure> {
        let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
            .with_account(self.account_name.clone())
            .with_container_name(self.bucket.clone());

        if let Some(credential) = credential {
            match credential {
                AzCredential::AccessKey {
                    client_id,
                    secret_key,
                    tenant_id,
                } => {
                    builder =
                        builder.with_client_secret_authorization(client_id, secret_key, tenant_id);
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
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum AzCredential {
    #[serde(rename_all = "kebab-case")]
    AccessKey {
        client_id: String,
        #[redact(partial)]
        secret_key: String,
        tenant_id: String,
    },
}

#[cfg(test)]
mod test {
    use super::{AzCredential, AzProfile};

    #[tokio::test]
    async fn test_can_validate() {
        tracing_subscriber::fmt::init();
        let account_name = "ht0cht0tabular0eastus";
        let bucket_name = "tobias";
        let mut prof = AzProfile {
            bucket: "tobias".to_string(),
            key_prefix: Some("test".to_string()),
            account_name: "ht0cht0tabular0eastus".to_string(),
        };
        prof.validate(Some(&AzCredential::AccessKey {
            client_id: "7af37ff1-e011-45d2-b302-1c51f8de8bcf".to_string(),
            secret_key: "".to_string(),
            tenant_id: "7a891ae2-bb88-41c2-8e25-13a407dca040".to_string(),
        }))
        .await
        .unwrap();
    }
}
