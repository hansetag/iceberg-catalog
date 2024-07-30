use crate::{
    service::{NamespaceIdentUuid, TableIdentUuid},
    WarehouseIdent, CONFIG,
};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig, ErrorModel, Result};
use crate::service::tabular_idents::TabularIdentUuid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    pub account_key: String,
    pub endpoint: Url,
    /// Region to use for S3 requests.
    pub region: String,
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
            account_key: _,
            endpoint,
            region,
        } = self;

        is_valid_bucket_name(bucket)?;

        if region.len() > 128 {
            return Err(ErrorModel::bad_request(
                "Storage Profile `region` must be less than 128 characters.",
                "InvalidRegion",
                None,
            )
            .into());
        }

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

        // Protocol must be http or https
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err(ErrorModel::bad_request(
                "Storage Profile `endpoint` must have http or https protocol.",
                "InvalidS3Endpoint",
                None,
            )
            .into());
        }

        let file_io = self.file_io(credential)?;
        let test_location = self.tabular_location(
            uuid::Uuid::now_v7().into(),
            TabularIdentUuid::Table(uuid::Uuid::now_v7()),
        );

        validate_file_io(&file_io, &test_location)
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

        if self.region != other.region {
            return Err(ErrorModel::bad_request(
                "Storage Profile `region` cannot be updated to prevent data loss.",
                "InvalidRegion",
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

    #[cfg(feature = "s3-signer")]
    /// Get the AWS SDK credentials for the S3 profile.
    ///
    /// # Errors
    /// Fails if the assume role ARN is provided.
    /// Fails if the credential is missing.
    pub fn get_aws_sdk_credentials(
        &self,
        credential: Option<&AzCredential>,
    ) -> Result<aws_credential_types::Credentials> {
        let Self {
            bucket,
            key_prefix,
            account_name,
            account_key,
            endpoint,
            region,
        } = self;

        // Currently there is no supported configuration without Credential
        if let Some(credential) = credential {
            match credential {
                AzCredential::AccessKey {
                    aws_access_key_id,
                    aws_secret_access_key,
                } => {
                    let credentials = aws_credential_types::Credentials::new(
                        aws_access_key_id.clone(),
                        aws_secret_access_key.clone(),
                        None,
                        None,
                        "iceberg-rest-secret-storage",
                    );
                    Ok(credentials)
                }
            }
        } else {
            Err(ErrorModel::bad_request(
                "Storage Credentials missing.",
                "MissingStorageCredential",
                None,
            )
            .into())
        }
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
        // s3://bucket-name/<path_prefix>/<namespace-uuid>/<table-uuid>/
        if let Some(key_prefix) = &self.key_prefix {
            format!(
                "s3://{}/{key_prefix}/{namespace_id}/{tabular_id}",
                &self.bucket
            )
        } else {
            format!("s3://{}/{namespace_id}/{tabular_id}", &self.bucket)
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

        config.insert("s3.region".to_string(), self.region.to_string());
        config.insert("region".to_string(), self.region.to_string());
        config.insert("client.region".to_string(), self.region.to_string());

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
    pub fn file_io(&self, credential: Option<&AzCredential>) -> Result<iceberg::io::FileIO> {
        let mut builder = iceberg::io::FileIOBuilder::new("azdls");

        builder = builder
            .with_prop("endpoint", self.endpoint.to_string())
            .with_prop("account_name", self.account_name.clone())
            .with_prop("account_key", self.account_key.clone())
            .with_prop("container", self.bucket.clone());

        if let Some(credential) = credential {
            match credential {
                AzCredential::AccessKey {
                    aws_access_key_id,
                    aws_secret_access_key,
                } => {
                    builder = builder
                        .with_prop(iceberg::io::S3_ACCESS_KEY_ID, aws_access_key_id)
                        .with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, aws_secret_access_key);
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
        aws_access_key_id: String,
        #[redact(partial)]
        aws_secret_access_key: String,
    },
}

// ToDo: Move somewhere so that other profiles can use it as well?
async fn validate_file_io(file_io: &iceberg::io::FileIO, test_location: &str) -> Result<()> {
    // Validate the file_io instance by creating a test file.

    let test_file = file_io.new_output(test_location).map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating S3 Storage Profile: {e}"),
            "S3TestFileCreationError",
            Some(Box::new(e)),
        )
    })?;
    let mut writer = test_file.writer().await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating S3 Storage Profile: {e}"),
            "S3TestFileWriterError",
            Some(Box::new(e)),
        )
    })?;

    let buf: &[u8; 4] = b"test";
    writer.write(buf.to_vec().into()).await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating S3 Storage Profile: {e}"),
            "S3TestFileWriterError",
            Some(Box::new(e)),
        )
    })?;

    writer.close().await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating S3 Storage Profile: {e}"),
            "S3TestFileCloseError",
            Some(Box::new(e)),
        )
    })?;

    file_io.delete(test_location).await.map_err(|e| {
        ErrorModel::bad_request(
            format!("Error validating S3 Storage Profile: {e}"),
            "S3TestFileDeleteError",
            Some(Box::new(e)),
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::{validate_file_io, AzProfile};
    use std::str::FromStr;
    use url::Url;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_can_validate() {
        let account_name = "ht0cht0tabular0eastus";
        let prof = AzProfile {
            bucket: "tobias".to_string(),
            key_prefix: None,
            account_name: "ht0cht0tabular0eastus".to_string(),
            account_key: "...==".to_string(),
            endpoint: Url::from_str(&format!("https://{account_name}.dfs.core.windows.net"))
                .unwrap(),
            region: "eastus".to_string(),
        };
        let fio = prof.file_io(None);
        let loc = Uuid::now_v7();
        validate_file_io(&fio.unwrap(), &format!("/{loc}"))
            .await
            .unwrap();
    }
}
