#![allow(clippy::module_name_repetitions)]

use crate::{WarehouseIdent, CONFIG};

use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::StoragePermissions;
use aws_config::{BehaviorVersion, SdkConfig};

use iceberg_ext::configs::table::{client, custom, s3, ConfigValue, TableConfig};
use iceberg_ext::configs::{self, Location};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use veil::Redact;

use super::StorageType;

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
    /// If not specified, the client decides. If the underlying S3
    /// supports both, we recommend to not set `path_style_access`.
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
    pub(super) fn normalize(&mut self) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket)?;
        validate_region(&self.region)?;
        self.normalize_key_prefix()?;
        self.normalize_endpoint()?;
        self.normalize_assume_role_arn();
        self.normalize_sts_role_arn();

        if self.sts_enabled && matches!(self.flavor, S3Flavor::Aws) && self.sts_role_arn.is_none() {
            return Err(ValidationError::InvalidProfile {
                source: None,
                reason: "Storage Profile `sts-role-arn` is required for AWS flavor.".to_string(),
                entity: "sts_role_arn".to_string(),
            });
        }

        Ok(())
    }

    fn normalize_key_prefix(&mut self) -> Result<(), ValidationError> {
        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim_matches('/').to_string();
        }

        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.is_empty() {
                self.key_prefix = None;
            }
        }

        // Aws supports a max of 1024 chars and we need some buffer for tables.
        if let Some(key_prefix) = self.key_prefix.as_ref() {
            if key_prefix.len() > 896 {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `key_prefix` must be less than 1024 characters."
                        .to_string(),
                    entity: "key_prefix".to_string(),
                });
            }
        }
        Ok(())
    }

    fn normalize_endpoint(&mut self) -> Result<(), ValidationError> {
        if let Some(endpoint) = self.endpoint.as_mut() {
            if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
                return Err(ValidationError::InvalidProfile {
                    source: None,
                    reason: "Storage Profile `endpoint` must have http or https protocol."
                        .to_string(),
                    entity: "S3Endpoint".to_string(),
                });
            }

            // If a non-empty path is provided, it must be a single slash which we remove.
            if !endpoint.path().is_empty() {
                if endpoint.path() != "/" {
                    return Err(ValidationError::InvalidProfile {
                        source: None,
                        reason: "Storage Profile `endpoint` must not have a path.".to_string(),
                        entity: "S3Endpoint".to_string(),
                    });
                }

                endpoint.set_path("/");
            }
        }

        Ok(())
    }

    fn normalize_assume_role_arn(&mut self) {
        if let Some(assume_role_arn) = self.assume_role_arn.as_ref() {
            if assume_role_arn.is_empty() {
                self.assume_role_arn = None;
            }
        }
    }

    fn normalize_sts_role_arn(&mut self) {
        if let Some(sts_role_arn) = self.sts_role_arn.as_ref() {
            if sts_role_arn.is_empty() {
                self.sts_role_arn = None;
            }
        }
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
        use super::StorageType;

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
            defaults: HashMap::default(),
            overrides: HashMap::from_iter(vec![(
                configs::table::s3::SignerUri::KEY.to_string(),
                CONFIG.s3_signer_uri_for_warehouse(warehouse_id).to_string(),
            )]),
        }
    }

    /// Base Location for this storage profile.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles
    pub fn base_location(&self) -> Result<S3Location, ValidationError> {
        let prefix = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').into_iter().map(|f| f.to_owned()).collect())
            .unwrap_or_default();
        S3Location::new(self.bucket.clone(), prefix)
    }

    /// Generate the table configuration for S3.
    ///
    /// # Errors
    /// Fails if vended credentials are used - currently not supported.
    pub async fn generate_table_config(
        &self,
        DataAccess {
            vended_credentials,
            remote_signing,
        }: &DataAccess,
        cred: Option<&S3Credential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableConfig, TableConfigError> {
        // If vended_credentials is False and remote_signing is False,
        // use remote_signing.
        let mut remote_signing = !vended_credentials || *remote_signing;

        let mut config = TableConfig::default();

        if let Some(true) = self.path_style_access {
            config.insert(&s3::PathStyleAccess(true));
        }

        config.insert(&s3::Region(self.region.to_string()));
        config.insert(&client::Region(self.region.to_string()));
        config.insert(&custom::Pair {
            key: "region".to_string(),
            value: self.region.to_string(),
        });
        config.insert(&client::Region(self.region.to_string()));

        if let Some(endpoint) = &self.endpoint {
            config.insert(&s3::Endpoint(endpoint.clone()));
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
                    self.get_minio_sts_token(table_location, cred, storage_permissions)
                        .await?
                } else if let (Some(cred), Some(arn)) = (cred, self.sts_role_arn.as_ref()) {
                    self.get_aws_sts_token(table_location, cred, arn, storage_permissions)
                        .await?
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
        table_location: &Location,
        cred: &S3Credential,
        arn: &String,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        self.get_sts_token(table_location, cred, Some(arn), storage_permissions)
            .await
    }

    async fn get_minio_sts_token(
        &self,
        table_location: &Location,
        cred: &S3Credential,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        self.get_sts_token(table_location, cred, None, storage_permissions)
            .await
    }

    async fn get_sts_token(
        &self,
        table_location: &Location,
        cred: &S3Credential,
        arn: Option<&str>,
        storage_permissions: StoragePermissions,
    ) -> Result<aws_sdk_sts::types::Credentials, TableConfigError> {
        let cred = self
            .get_aws_sdk_config(self.get_aws_sdk_credentials(Some(cred))?)
            .await;

        let assume_role_builder = aws_sdk_sts::Client::new(&cred)
            .assume_role()
            .role_session_name("iceberg")
            .policy(Self::get_aws_policy_string(
                table_location.as_str(),
                storage_permissions,
            ));
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

    async fn get_aws_sdk_config(&self, creds: aws_credential_types::Credentials) -> SdkConfig {
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

    fn permission_to_actions(storage_permissions: StoragePermissions) -> &'static str {
        match storage_permissions {
            StoragePermissions::Read => "\"s3:GetObject\"",
            StoragePermissions::ReadWrite => "\"s3:GetObject\", \"s3:PutObject\"",
            StoragePermissions::ReadWriteDelete => {
                "\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:ListBucket\""
            }
        }
    }

    fn get_aws_policy_string(
        table_location: &str,
        storage_permissions: StoragePermissions,
    ) -> String {
        let resource = format!(
            "arn:aws:s3:::{}/*",
            table_location
                .trim_start_matches("s3://")
                .trim_end_matches('/')
        );
        format!(
            r#"{{
        "Version": "2012-10-17",
        "Statement": [
            {{
                "Sid": "{}",
                "Effect": "Allow",
                "Action": [
                    {}
                ],
                "Resource": [
                    "{}"
                ]
            }}
        ]
    }}"#,
            uuid::Uuid::now_v7().simple(),
            Self::permission_to_actions(storage_permissions),
            resource
        )
    }
}

pub(super) fn get_file_io_from_table_config(
    config: &TableConfig,
) -> Result<iceberg::io::FileIO, FileIoError> {
    let mut builder = iceberg::io::FileIOBuilder::new("s3");

    for key in [
        iceberg::io::S3_REGION,
        iceberg::io::S3_ENDPOINT,
        iceberg::io::S3_ACCESS_KEY_ID,
        iceberg::io::S3_SECRET_ACCESS_KEY,
        iceberg::io::S3_SESSION_TOKEN,
    ] {
        if let Some(value) = config.get_custom_prop(key) {
            builder = builder.with_prop(key, value);
        }
    }

    Ok(builder.build()?)
}

fn validate_region(region: &str) -> Result<(), ValidationError> {
    if region.len() > 128 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`region` must be less than 128 characters.".to_string(),
            entity: "region".to_string(),
        });
    }

    Ok(())
}

fn validate_bucket_name(bucket: &str) -> Result<(), ValidationError> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(ValidationError::InvalidProfile {
            source: None,
            reason: "`bucket` must be between 3 and 63 characters long.".to_string(),
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
pub struct S3Location {
    bucket_name: String,
    key: Vec<String>,
    // Location is redundant but useful for type-safe access.
    location: Location,
}

impl std::fmt::Display for S3Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.location.fmt(f)
    }
}

impl S3Location {
    /// Create a new S3 location.
    ///
    /// # Errors
    /// Fails if the bucket name is invalid or the key contains unescaped slashes.
    pub fn new(bucket_name: String, key: Vec<String>) -> Result<Self, ValidationError> {
        validate_bucket_name(&bucket_name)?;
        // Keys may not contain slashes
        if key.iter().any(|k| k.contains('/')) {
            return Err(ValidationError::InvalidLocation {
                source: None,
                reason: "S3 key contains unescaped slashes (/)".to_string(),
                location: format!("{key:?}"),
                storage_type: StorageType::S3,
            });
        }

        let location = format!("s3://{bucket_name}");
        let mut location =
            Location::from_str(&location).map_err(|e| ValidationError::InvalidLocation {
                reason: "Invalid S3 location.".to_string(),
                location: location.clone(),
                source: Some(e.into()),
                storage_type: StorageType::S3,
            })?;
        if !key.is_empty() {
            location.without_trailing_slash().extend(key.iter());
        }

        Ok(S3Location {
            bucket_name,
            key,
            location,
        })
    }

    #[must_use]
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    #[must_use]
    pub fn key(&self) -> &Vec<String> {
        &self.key
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }
}

impl TryFrom<Location> for S3Location {
    type Error = ValidationError;

    fn try_from(location: Location) -> Result<Self, Self::Error> {
        // Protocol must be s3
        if location.url().scheme() != "s3" {
            return Err(ValidationError::InvalidLocation {
                reason: "S3 location must use s3 protocol.".to_string(),
                location: location.to_string(),
                source: None,
                storage_type: StorageType::S3,
            });
        }

        let bucket_name =
            location
                .url()
                .host_str()
                .ok_or_else(|| ValidationError::InvalidLocation {
                    reason: "S3 location does not have a bucket name.".to_string(),
                    location: location.to_string(),
                    source: None,
                    storage_type: StorageType::S3,
                })?;

        let key: Vec<String> = location
            .url()
            .path_segments()
            .map_or(Vec::new(), |segments| {
                segments.map(std::string::ToString::to_string).collect()
            });

        S3Location::new(bucket_name.to_string(), key)
    }
}

impl FromStr for S3Location {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let location = Location::from_str(s).map_err(|e| ValidationError::InvalidLocation {
            reason: format!("Invalid Location: {e}"),
            location: s.to_string(),
            source: Some(e.into()),
            storage_type: StorageType::S3,
        })?;

        Self::try_from(location)
    }
}

impl From<S3Location> for Location {
    fn from(location: S3Location) -> Self {
        location.location
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::{
        storage::{StorageLocations as _, StorageProfile},
        tabular_idents::TabularIdentUuid,
        NamespaceIdentUuid,
    };
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
            let result = validate_bucket_name(&bucket);
            if expected {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_default_s3_locations() {
        let profile = S3Profile {
            bucket: "test-bucket".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            assume_role_arn: None,
            endpoint: None,
            region: "dummy".to_string(),
            path_style_access: Some(true),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        };
        let sp: StorageProfile = profile.clone().into();

        let namespace_id = NamespaceIdentUuid::from(uuid::Uuid::now_v7());
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();

        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("s3://test-bucket/test_prefix/{namespace_id}/{table_id}")
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;
        let sp: StorageProfile = profile.into();

        let namespace_location = sp.default_namespace_location(namespace_id).unwrap();
        let location = sp.default_tabular_location(&namespace_location, table_id);
        assert_eq!(
            location.to_string(),
            format!("s3://test-bucket/{namespace_id}/{table_id}")
        );
    }

    #[test]
    /// Tests that the tabular location is correctly generated when the namespace location
    /// independent of a trailing slash in the namespace location.
    fn test_tabular_location_trailing_slash() {
        let profile = S3Profile {
            bucket: "test-bucket".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            assume_role_arn: None,
            endpoint: None,
            region: "dummy".to_string(),
            path_style_access: Some(true),
            sts_role_arn: None,
            sts_enabled: false,
            flavor: S3Flavor::Aws,
        };

        let namespace_location = Location::from_str("s3://test-bucket/foo/").unwrap();
        let table_id = TabularIdentUuid::Table(uuid::Uuid::now_v7());
        // Prefix should be ignored as we specify the namespace_location explicitly.
        // Tabular locations should not have a trailing slash, otherwise pyiceberg fails.
        let expected = format!("s3://test-bucket/foo/{table_id}");

        let location = profile.default_tabular_location(&namespace_location, table_id);

        assert_eq!(location.to_string(), expected);

        let namespace_location = Location::from_str("s3://test-bucket/foo").unwrap();
        let location = profile.default_tabular_location(&namespace_location, table_id);
        assert_eq!(location.to_string(), expected);
    }

    #[needs_env_var(TEST_MINIO)]
    mod minio {
        use crate::service::storage::{
            S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
        };

        #[tokio::test]
        async fn test_can_validate() {
            let bucket = std::env::var("ICEBERG_REST_TEST_S3_BUCKET").unwrap();
            let region = std::env::var("ICEBERG_REST_TEST_S3_REGION").unwrap_or("local".into());
            let aws_access_key_id = std::env::var("ICEBERG_REST_TEST_S3_ACCESS_KEY").unwrap();
            let aws_secret_access_key = std::env::var("ICEBERG_REST_TEST_S3_SECRET_KEY").unwrap();
            let endpoint = std::env::var("ICEBERG_REST_TEST_S3_ENDPOINT").unwrap();

            let cred: StorageCredential = S3Credential::AccessKey {
                aws_access_key_id,
                aws_secret_access_key,
            }
            .into();

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
            let mut profile: StorageProfile = profile.into();

            profile.normalize().unwrap();
            profile.validate_access(Some(&cred), None).await.unwrap();
        }
    }

    #[needs_env_var(TEST_AWS)]
    mod aws {
        use crate::service::storage::{StorageCredential, StorageProfile};

        use super::super::*;

        #[tokio::test]
        async fn test_can_validate() {
            let bucket = std::env::var("AWS_S3_BUCKET").unwrap();
            let region = std::env::var("AWS_S3_REGION").unwrap();
            let sts_role_arn = std::env::var("AWS_S3_STS_ROLE_ARN").unwrap();
            let cred: StorageCredential = S3Credential::AccessKey {
                aws_access_key_id: std::env::var("AWS_S3_ACCESS_KEY_ID").unwrap(),
                aws_secret_access_key: std::env::var("AWS_S3_SECRET_ACCESS_KEY").unwrap(),
            }
            .into();

            let mut profile: StorageProfile = S3Profile {
                bucket,
                key_prefix: Some("test_prefix".to_string()),
                assume_role_arn: None,
                endpoint: None,
                region,
                path_style_access: Some(true),
                sts_role_arn: Some(sts_role_arn),
                flavor: S3Flavor::Aws,
                sts_enabled: true,
            }
            .into();

            profile.normalize().unwrap();
            profile.validate_access(Some(&cred), None).await.unwrap();
        }
    }

    #[test]
    fn test_parse_s3_location() {
        let cases = vec![
            (
                "s3://test-bucket/test_prefix/namespace/table",
                "test-bucket",
                vec!["test_prefix", "namespace", "table"],
            ),
            (
                "s3://test-bucket/test_prefix/namespace/table/",
                "test-bucket",
                vec!["test_prefix", "namespace", "table", ""],
            ),
            (
                "s3://test-bucket/test_prefix",
                "test-bucket",
                vec!["test_prefix"],
            ),
            (
                "s3://test-bucket/test_prefix/",
                "test-bucket",
                vec!["test_prefix", ""],
            ),
            ("s3://test-bucket/", "test-bucket", vec![""]),
            ("s3://test-bucket", "test-bucket", vec![]),
        ];

        for (location, bucket, prefix) in cases {
            let result = S3Location::from_str(location).unwrap();
            assert_eq!(result.bucket_name, bucket);
            assert_eq!(result.key, prefix);
            assert_eq!(result.to_string(), location);
        }
    }

    #[test]
    fn parse_invalid_s3_location() {
        let cases = vec![
            // wrong prefix
            "abc://test-bucket/foo",
            "test-bucket/foo",
            "/test-bucket/foo",
            // Invalid bucket name
            "s3://test_bucket/foo",
        ];

        for case in cases {
            let result = S3Location::from_str(case);
            assert!(result.is_err());
        }
    }

    #[test]
    fn policy_string_is_json() {
        let table_location = "s3://bucket-name/path/to/table";
        let policy =
            S3Profile::get_aws_policy_string(table_location, StoragePermissions::ReadWriteDelete);
        let _ = serde_json::from_str::<serde_json::Value>(&policy).unwrap();
    }

    #[test]
    fn test_parse_s3_location_invalid_proto() {
        S3Location::from_str("adls://test-bucket/foo/").unwrap_err();
    }

    #[test]
    fn test_s3_location_display() {
        let cases = vec![
            "s3://bucket/foo",
            "s3://bucket/foo/bar",
            "s3://bucket/foo/bar/",
        ];
        for case in cases {
            let location = S3Location::from_str(case).unwrap();
            let printed = location.to_string();
            assert_eq!(printed, case);
        }
    }
}
