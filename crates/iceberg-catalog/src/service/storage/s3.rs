#![allow(clippy::module_name_repetitions)]

use crate::{
    service::{NamespaceIdentUuid, TableIdentUuid},
    WarehouseIdent, CONFIG,
};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::{StorageProfile, StorageType};
use crate::service::tabular_idents::TabularIdentUuid;
use aws_config::{BehaviorVersion, SdkConfig};

use iceberg::io::FileIO;
use iceberg_ext::table_config::{client, custom, s3, TableConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use veil::Redact;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct S3Profile {
    /// Name of the S3 bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
    #[serde(default)]
    /// Optional ARN to assume when accessing the bucket
    pub assume_role_arn: Option<String>,
    /// Optional endpoint to use for S3 requests, if not provided
    /// the region will be used to determine the endpoint.
    /// If both region and endpoint are provided, the endpoint will be used.
    /// Example: `http://s3-de.my-domain.com:9000`
    #[serde(default)]
    pub endpoint: Option<url::Url>,
    /// Region to use for S3 requests.
    pub region: String,
    /// Path style access for S3 requests.
    #[serde(default)]
    pub path_style_access: Option<bool>,
    /// Optional role ARN to assume for sts vended-credentials
    pub sts_role_arn: Option<String>,
    pub sts_enabled: bool,
    /// S3 flavor to use.
    /// Defaults to AWS
    #[serde(default)]
    pub flavor: S3Flavor,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
#[schema(rename_all = "kebab-case")]
#[derive(Default)]
pub enum S3Flavor {
    #[default]
    Aws,
    Minio,
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum S3Credential {
    #[serde(rename_all = "kebab-case")]
    AccessKey {
        aws_access_key_id: String,
        #[redact(partial)]
        aws_secret_access_key: String,
    },
}

impl From<&S3Credential> for aws_credential_types::Credentials {
    fn from(cred: &S3Credential) -> Self {
        match &cred {
            S3Credential::AccessKey {
                aws_access_key_id,
                aws_secret_access_key,
            } => aws_credential_types::Credentials::new(
                aws_access_key_id.clone(),
                aws_secret_access_key.clone(),
                None,
                None,
                "iceberg-rest-secret-storage",
            ),
        }
    }
}

impl S3Profile {
    /// Create a new `FileIO` instance for S3.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(
        &self,
        credential: Option<&aws_credential_types::Credentials>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("s3");

        builder = builder.with_prop(iceberg::io::S3_REGION, self.region.clone());

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_prop(iceberg::io::S3_ENDPOINT, endpoint);
        }
        if let Some(_assume_role_arn) = &self.assume_role_arn {
            return Err(FileIoError::UnsupportedAction(
                "S3 Assume role ARN".to_string(),
            ));
        }
        if let Some(credential) = credential {
            if let Some(session_token) = &credential.session_token() {
                builder = builder.with_prop(iceberg::io::S3_SESSION_TOKEN, session_token);
            }
            builder = builder
                .with_prop(iceberg::io::S3_ACCESS_KEY_ID, credential.access_key_id())
                .with_prop(
                    iceberg::io::S3_SECRET_ACCESS_KEY,
                    credential.secret_access_key(),
                );
        }

        Ok(builder.build()?)
    }

    /// Validate the S3 profile.
    ///
    /// # Errors
    /// - Fails if the bucket name is invalid.
    /// - Fails if the region is too long.
    /// - Fails if the key prefix is too long.
    /// - Fails if the region or endpoint is missing.
    /// - Fails if the endpoint is not a valid URL.
    pub async fn validate(&self, credential: Option<&S3Credential>) -> Result<(), ValidationError> {
        // If key_prefix is provided, remove any trailing and leading slashes.

        let S3Profile {
            bucket,
            key_prefix,
            // Validated via file_io
            assume_role_arn: _,
            endpoint,
            region,
            // Validated via file_io
            path_style_access: _,
            sts_role_arn: _,
            sts_enabled,
            flavor,
        } = self;

        if *sts_enabled && matches!(flavor, S3Flavor::Aws) && self.sts_role_arn.is_none() {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `sts_role_arn` is required for AWS flavor.".to_string(),
                entity: "sts_role_arn".to_string(),
            });
        }

        if *sts_enabled && credential.is_none() {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `credential` is required for STS enabled profiles."
                    .to_string(),
                entity: "credential".to_string(),
            });
        }

        let key_prefix = key_prefix
            .as_deref()
            .unwrap_or_default()
            .trim_start_matches('/');

        is_valid_bucket_name(bucket)?;

        if region.len() > 128 {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `region` must be less than 128 characters.".to_string(),
                entity: "region".to_string(),
            });
        }

        // Aws supports a max of 1024 chars and we need some buffer for tables.
        if key_prefix.len() > 512 {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `key_prefix` must be less than 512 characters."
                    .to_string(),
                entity: "key_prefix".to_string(),
            });
        }

        if let Some(endpoint) = endpoint {
            // Protocol must be http or https
            if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `endpoint` must have http or https protocol."
                        .to_string(),
                    entity: "S3Endpoint".to_string(),
                });
            }

            // No path allowed
            if !endpoint.path().is_empty() && endpoint.path() != "/" {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `endpoint` must not have a path.".to_string(),
                    entity: "S3Endpoint".to_string(),
                });
            }
        }

        let file_io = self.file_io(credential.map(Into::into).as_ref())?;
        let test_location = self.tabular_location(
            uuid::Uuid::now_v7().into(),
            TabularIdentUuid::Table(uuid::Uuid::now_v7()),
        );
        self.validate_file_io(&file_io, &test_location).await?;
        tracing::debug!("Validated FileIO for S3 profile");

        if self.sts_enabled {
            if let (Some(arn), S3Flavor::Aws) = (self.sts_role_arn.as_ref(), self.flavor) {
                tracing::debug!("S3 Flavor is AWS, getting sts token for arn: '{}'", arn);
                let token = self
                    .get_aws_sts_token(
                        &test_location,
                        credential.ok_or(CredentialsError::MissingCredential(StorageType::S3))?,
                        arn,
                    )
                    .await?;
                tracing::debug!("Validating STS token.");
                self.validate_sts_token(token, bucket, &test_location)
                    .await?;
            } else if matches!(flavor, S3Flavor::Minio) {
                tracing::debug!("S3 Flavor is Minio, getting sts token");
                let token = self
                    .get_minio_sts_token(
                        &test_location,
                        credential.ok_or(CredentialsError::MissingCredential(StorageType::S3))?,
                    )
                    .await?;
                tracing::debug!("Validating STS token.");
                self.validate_sts_token(token, bucket, &test_location)
                    .await?;
            }
        }

        Ok(())
    }

    async fn validate_file_io(
        &self,
        file_io: &FileIO,
        test_location: &str,
    ) -> Result<(), ValidationError> {
        let test_location = dbg!(test_location.trim_end_matches('/').to_string() + "/test.txt.gz");
        // Test that we can write a metadata file
        crate::catalog::io::write_metadata_file(&test_location, "test", file_io)
            .await
            .map_err(|e| {
                ValidationError::IoOperationFailed(e, Box::new(StorageProfile::S3(self.clone())))
            })?;
        tracing::debug!("Successfully wrote test file to: {}", test_location);
        crate::catalog::io::read_file(file_io, &test_location)
            .await
            .map_err(|e| {
                ValidationError::IoOperationFailed(e, Box::new(StorageProfile::S3(self.clone())))
            })?;
        // Test that we can delete the test file
        crate::catalog::io::delete_file(file_io, &test_location)
            .await
            .map_err(|e| {
                ValidationError::IoOperationFailed(e, Box::new(StorageProfile::S3(self.clone())))
            })?;
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
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }

        if self.region != other.region {
            return Err(UpdateError::ImmutableField("region".to_string()));
        }

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
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
        credential: Option<&S3Credential>,
    ) -> Result<aws_credential_types::Credentials, CredentialsError> {
        let Self {
            assume_role_arn,
            endpoint: _,
            region: _,
            path_style_access: _,
            bucket: _,
            key_prefix: _,
            sts_role_arn: _,
            sts_enabled: _,
            flavor: _,
        } = self;

        // assume_role_arn is not supported currently
        if let Some(_assume_role_arn) = assume_role_arn {
            return Err(CredentialsError::UnsupportedCredential(
                "S3 Assume role ARN not supported.".to_string(),
            ));
        }

        // Currently there is no supported configuration without Credential
        if let Some(credential) = credential {
            match credential {
                S3Credential::AccessKey {
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
            Err(CredentialsError::MissingCredential(StorageType::S3))
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

    /// Generate the table configuration for S3.
    ///
    /// # Errors
    /// Fails if vended credentials are used - currently not supported.
    pub async fn generate_table_config(
        &self,
        _: WarehouseIdent,
        _: TableIdentUuid,
        _: NamespaceIdentUuid,
        DataAccess {
            vended_credentials,
            remote_signing,
        }: &DataAccess,
        table_location: &str,
        cred: Option<&S3Credential>,
    ) -> Result<TableConfig, TableConfigError> {
        // If vended_credentials is False and remote_signing is False,
        // use remote_signing.
        let mut remote_signing = !vended_credentials || *remote_signing;

        let mut config = TableConfig::default();

        if let Some(true) = self.path_style_access {
            config.insert(&s3::PathStyleAccess(true));
        }

        config.insert(&s3::Region(self.region.to_string()));
        config.insert(&custom::Pair {
            key: "region".to_string(),
            value: self.region.to_string(),
        });
        config.insert(&client::Region(self.region.to_string()));

        if let Some(endpoint) = &self.endpoint {
            config.insert(&s3::Endpoint(endpoint.to_string()));
        }

        if *vended_credentials {
            if self.sts_enabled {
                let aws_sdk_sts::types::Credentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                    expiration: _,
                    ..
                } = if let (S3Flavor::Minio, Some(cred)) = (self.flavor, cred) {
                    self.get_minio_sts_token(table_location, cred).await?
                } else if let (Some(cred), Some(arn)) = (cred, self.sts_role_arn.as_ref()) {
                    self.get_aws_sts_token(table_location, cred, arn).await?
                } else {
                    // This error should never be returned since we validate this when creating the profile.
                    // We should consider using an enum instead of 3 independent fields.
                    return Err(TableConfigError::Misconfiguration(
                        "STS either needs Flavor Minio and credentials OR Flavor aws, credentials and a sts role arn.".to_string(),
                    ));
                };
                config.insert(&s3::AccessKeyId(access_key_id));
                config.insert(&s3::SecretAccessKey(secret_access_key));
                config.insert(&s3::SessionToken(session_token));
            } else {
                insert_pyiceberg_hack(&mut config);
                remote_signing = true;
            }
        }

        if remote_signing {
            config.insert(&s3::RemoteSigningEnabled(true));
            // Currently per-table signer uris are not supported by Spark.
            // The URI is cached for one table, and then re-used for another.
            // let signer_uri = CONFIG.s3_signer_uri_for_table(warehouse_id, namespace_id, table_id);
            // config.insert("s3.signer.uri".to_string(), signer_uri.to_string());
        }

        Ok(config)
    }

    async fn get_aws_sts_token(
        &self,
        table_location: &str,
        cred: &S3Credential,
        arn: &String,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        self.get_sts_token(table_location, cred, Some(arn)).await
    }

    async fn get_minio_sts_token(
        &self,
        table_location: &str,
        cred: &S3Credential,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        self.get_sts_token(table_location, cred, None).await
    }

    async fn get_sts_token(
        &self,
        table_location: &str,
        cred: &S3Credential,
        arn: Option<&str>,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        let cred = self
            .get_config(self.get_aws_sdk_credentials(Some(cred))?)
            .await;

        let assume_role_builder = aws_sdk_sts::Client::new(&cred)
            .assume_role()
            .role_session_name("iceberg")
            .policy(Self::get_aws_policy_string(table_location));
        let assume_role_builder = if let Some(arn) = arn {
            assume_role_builder.role_arn(arn)
        } else {
            assume_role_builder
        };

        let v = assume_role_builder.send().await.map_err(|e| {
            TableConfigError::FailedDependency(format!(
                "aws::sts::assume_role token call failed: {e:?}"
            ))
        })?;

        v.credentials.ok_or(TableConfigError::FailedDependency(
            "aws::sts::assume_role token call response didn't contain credentials".to_string(),
        ))
    }

    async fn validate_sts_token(
        &self,
        aws_sdk_sts::types::Credentials {
            access_key_id,
            secret_access_key,
            session_token,
            expiration: _,
            ..
        }: aws_sdk_sts::types::Credentials,
        bucket: &String,
        test_location: &String,
    ) -> Result<(), ValidationError> {
        tracing::debug!("Validating STS token for bucket: '{}'", bucket);
        let file_io = self.file_io(Some(&aws_credential_types::Credentials::new(
            access_key_id,
            secret_access_key,
            Some(session_token),
            None,
            "iceberg-rest-secret-storage",
        )))?;

        self.validate_file_io(&file_io, test_location).await?;
        tracing::debug!("Successfully read test file from: {}", test_location);
        Ok(())
    }

    async fn get_config(&self, creds: aws_credential_types::Credentials) -> SdkConfig {
        let loader = aws_config::ConfigLoader::default()
            .region(Some(aws_config::Region::new(
                self.region.as_str().to_string(),
            )))
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(creds);

        if let Some(endpoint) = &self.endpoint {
            loader.endpoint_url(endpoint.to_string()).load().await
        } else {
            loader.load().await
        }
    }

    fn get_aws_policy_string(table_location: &str) -> String {
        let resource = dbg!(format!(
            "arn:aws:s3:::{}/*",
            table_location.trim_start_matches("s3://")
        ));
        format!(
            r#"{{
        "Version": "2012-10-17",
        "Statement": [
            {{
                "Sid": "{}",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    "{}"
                ]
            }}
        ]
    }}"#,
            uuid::Uuid::now_v7().simple(),
            resource
        )
    }
}

fn is_valid_bucket_name(bucket: &str) -> Result<(), ValidationError> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Storage Profile `bucket` must be between 3 and 63 characters long."
                .to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(
            ValidationError::InvalidProfile {
                source: None,
                reason: "Bucket name can consist only of lowercase letters, numbers, dots (.), and hyphens (-).".to_string(),
                entity: "BucketName".to_string(),
            }
        );
    }

    // Bucket names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !bucket.chars().next().unwrap().is_ascii_alphanumeric()
        || !bucket.chars().last().unwrap().is_ascii_alphanumeric()
    {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Bucket name must begin and end with a letter or number.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    // Bucket names must not contain two adjacent periods.
    if bucket.contains("..") {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "Bucket name must not contain two adjacent periods.".to_string(),
            entity: "BucketName".to_string(),
        });
    }

    Ok(())
}

fn insert_pyiceberg_hack(config: &mut TableConfig) {
    config.insert(&s3::Signer("S3V4RestSigner".to_string()));
    config.insert(&custom::Pair {
        key: "py-io-impl".to_string(),
        value: "pyiceberg.io.fsspec.FsspecFileIO".to_string(),
    });
}

// S3Location exists as part of aws_sdk_s3::types, however we don't depend on it yet
// and there is no parse() function available. The prefix is also represented as a
// String, which makes it harder to work with.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct S3Location {
    pub(crate) bucket_name: String,
    pub(crate) prefix: Vec<String>,
}

impl std::fmt::Display for S3Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3://{}/{}", self.bucket_name, self.prefix.join("/"))
    }
}

impl S3Location {
    // Check if the location is a sublocation of the other location.
    // If the locations are the same, it is considered a sublocation.
    pub(crate) fn is_sublocation_of(&self, other: &S3Location) -> bool {
        if self.bucket_name != other.bucket_name {
            return false;
        }

        let other_is_folder = other.prefix.last() == Some(&String::new());

        // Other is a folder if the last prefix element is empty.
        let other_prefix = if other_is_folder {
            &other.prefix[..other.prefix.len() - 1]
        } else {
            &other.prefix
        };

        // Check if the other prefix is a prefix of the current prefix.
        self.prefix.starts_with(other_prefix)
    }
}

/// Parse the location of an object in S3. Will fail if only the bucket name is provided.
/// prefix has at least one element, which might be an empty string.
pub(crate) fn parse_s3_location(location: &str) -> Result<S3Location, ValidationError> {
    let location = url::Url::parse(location).map_err(|e| ValidationError::InvalidS3Location {
        reason: "S3 location is no valid URL.".to_string(),
        location: location.to_string(),
        source: Some(Box::new(e)),
    })?;

    // Protocol must be s3
    if location.scheme() != "s3" {
        return Err(ValidationError::InvalidS3Location {
            reason: "S3 location must use s3 protocol.".to_string(),
            location: location.to_string(),
            source: None,
        });
    }

    let bucket_name = location
        .host_str()
        .ok_or_else(|| ValidationError::InvalidS3Location {
            reason: "S3 location does not have a bucket name.".to_string(),
            location: location.to_string(),
            source: None,
        })?;

    let prefix: Vec<String> = location
        .path_segments()
        .map(|segments| segments.map(std::string::ToString::to_string).collect())
        .ok_or(ValidationError::InvalidS3Location {
            reason: "S3 location does not have a table key.".to_string(),
            location: location.to_string(),
            source: None,
        })?;

    Ok(S3Location {
        bucket_name: bucket_name.to_string(),
        prefix,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::tabular_idents::TabularIdentUuid;
    use needs_env_var::needs_env_var;

    #[test]
    fn test_is_valid_bucket_name() {
        let cases = vec![
            ("foo".to_string(), true),
            ("my-bucket".to_string(), true),
            ("my.bucket".to_string(), true),
            ("my..bucket".to_string(), false),
            // 64 characters
            ("a".repeat(63), true),
            ("a".repeat(64), false),
            // 2 characters
            ("a".repeat(2), false),
            ("a".repeat(3), true),
            // Special-chars
            ("1bucket".to_string(), true),
            ("my_bucket".to_string(), false),
            ("my-ö-bucket".to_string(), false),
            // Invalid start / end chars
            (".my-bucket".to_string(), false),
            ("my-bucket.".to_string(), false),
        ];

        for (bucket, expected) in cases {
            let result = is_valid_bucket_name(&bucket);
            if expected {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_s3_location() {
        let profile = S3Profile {
            bucket: "test_bucket".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            assume_role_arn: None,
            endpoint: None,
            region: "dummy".to_string(),
            path_style_access: Some(true),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        };

        let namespace_id = NamespaceIdentUuid::from(uuid::Uuid::now_v7());
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());

        let location = profile.tabular_location(namespace_id, table_id);
        assert_eq!(
            location,
            format!("s3://test_bucket/test_prefix/{namespace_id}/{table_id}")
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;

        let location = profile.tabular_location(namespace_id, table_id);
        assert_eq!(
            location,
            format!("s3://test_bucket/{namespace_id}/{table_id}")
        );
    }

    #[needs_env_var(TEST_MINIO)]
    mod minio {
        use crate::service::storage::{S3Credential, S3Flavor, S3Profile};

        #[tokio::test]
        async fn test_can_validate() {
            let bucket = std::env::var("ICEBERG_REST_TEST_S3_BUCKET").unwrap();
            let region = std::env::var("ICEBERG_REST_TEST_S3_REGION").unwrap_or("local".into());
            let aws_access_key_id = std::env::var("ICEBERG_REST_TEST_S3_ACCESS_KEY").unwrap();
            let aws_secret_access_key = std::env::var("ICEBERG_REST_TEST_S3_SECRET_KEY").unwrap();
            let endpoint = std::env::var("ICEBERG_REST_TEST_S3_ENDPOINT").unwrap();

            let cred = S3Credential::AccessKey {
                aws_access_key_id,
                aws_secret_access_key,
            };

            let profile = S3Profile {
                bucket,
                key_prefix: Some("test_prefix".to_string()),
                assume_role_arn: None,
                endpoint: Some(endpoint.parse().unwrap()),
                region,
                path_style_access: Some(true),
                sts_role_arn: None,
                flavor: S3Flavor::Minio,
                sts_enabled: true,
            };

            profile.validate(Some(&cred)).await.unwrap();
        }
    }

    #[needs_env_var(TEST_AWS)]
    mod aws {
        use super::super::*;

        #[tokio::test]
        async fn test_can_validate() {
            let bucket = std::env::var("AWS_S3_BUCKET").unwrap();
            let region = std::env::var("AWS_S3_REGION").unwrap();
            let sts_role_arn = std::env::var("AWS_S3_STS_ROLE_ARN").unwrap();
            let cred = S3Credential::AccessKey {
                aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
            };

            let profile = S3Profile {
                bucket,
                key_prefix: Some("test_prefix".to_string()),
                assume_role_arn: None,
                endpoint: None,
                region,
                path_style_access: Some(true),
                sts_role_arn: Some(sts_role_arn),
                flavor: S3Flavor::Aws,
                sts_enabled: true,
            };

            profile.validate(Some(&cred)).await.unwrap();
        }
    }

    #[test]
    fn test_parse_s3_location() {
        let cases = vec![
            (
                "s3://test_bucket/test_prefix/namespace/table",
                "test_bucket",
                vec!["test_prefix", "namespace", "table"],
            ),
            (
                "s3://test_bucket/test_prefix/namespace/table/",
                "test_bucket",
                vec!["test_prefix", "namespace", "table", ""],
            ),
            (
                "s3://test_bucket/test_prefix",
                "test_bucket",
                vec!["test_prefix"],
            ),
            (
                "s3://test_bucket/test_prefix/",
                "test_bucket",
                vec!["test_prefix", ""],
            ),
            ("s3://test_bucket/", "test_bucket", vec![""]),
        ];

        for (location, bucket, prefix) in cases {
            let result = parse_s3_location(location).unwrap();
            assert_eq!(result.bucket_name, bucket);
            assert_eq!(result.prefix, prefix);
        }
    }

    #[test]
    fn policy_string_is_json() {
        let table_location = "s3://bucket-name/path/to/table";
        let policy = S3Profile::get_aws_policy_string(table_location);
        let _ = serde_json::from_str::<serde_json::Value>(&policy).unwrap();
    }

    #[test]
    fn test_parse_s3_location_bucket_only() {
        parse_s3_location("s3://test_bucket").unwrap_err();
    }

    #[test]
    fn test_parse_s3_location_invalid_proto() {
        parse_s3_location("adls://test_bucket/foo/").unwrap_err();
    }

    #[test]
    fn test_is_sublocation_of() {
        let cases = vec![
            ("s3://bucket/foo", "s3://bucket/foo", true),
            ("s3://bucket/foo/", "s3://bucket/foo/bar", true),
            ("s3://bucket/foo", "s3://bucket/foo/bar", true),
            ("s3://bucket/foo", "s3://bucket/baz/bar", false),
            ("s3://bucket/foo", "s3://bucket/foo-bar", false),
        ];

        for (parent, maybe_sublocation, expected) in cases {
            let parent = parse_s3_location(parent).unwrap();
            let maybe_sublocation = parse_s3_location(maybe_sublocation).unwrap();
            let result = maybe_sublocation.is_sublocation_of(&parent);
            assert_eq!(result, expected);
        }
    }
}
