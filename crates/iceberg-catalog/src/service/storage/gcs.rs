#![allow(clippy::module_name_repetitions)]

use crate::{WarehouseIdent, CONFIG};
use std::any::Any;

use crate::api::{iceberg::v1::DataAccess, CatalogConfig};
use crate::service::storage::error::{
    CredentialsError, FileIoError, TableConfigError, UpdateError, ValidationError,
};
use crate::service::storage::StoragePermissions;
use aws_config::{BehaviorVersion, SdkConfig};

use super::StorageType;
use aws_credential_types::Credentials;
use axum::extract::FromRef;
use base64::Engine;
use google_cloud_auth::token_source::service_account_token_source::OAuth2ServiceAccountTokenSource;
use google_cloud_auth::token_source::TokenSource;
use google_cloud_token::TokenSourceProvider;
use iceberg_ext::catalog::rest::ErrorModel;
use iceberg_ext::configs::table::{client, custom, s3, TableProperties};
use iceberg_ext::configs::{self, ConfigProperty, Location};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;
use veil::Redact;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct GcsProfile {
    /// Name of the S3 bucket
    pub bucket: String,
    /// Subpath in the bucket to use.
    /// The same prefix can be used for multiple warehouses.
    pub key_prefix: Option<String>,
    #[serde(default)]
    pub endpoint: Option<url::Url>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
#[schema(rename_all = "kebab-case")]
pub enum GcsCredential {
    ServiceAccountKey(GcsServiceKey),
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
struct GcsServiceKey {
    #[serde(rename = "type")]
    pub r#type: String,
    pub project_id: String,
    pub private_key_id: String,
    #[redact(partial)]
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_x509_cert_url: String,
    pub universe_domain: String,
}

impl GcsProfile {
    /// Create a new `FileIO` instance for S3.
    ///
    /// # Errors
    /// Fails if the `FileIO` instance cannot be created.
    pub fn file_io(
        &self,
        credential: Option<&GcsCredential>,
    ) -> Result<iceberg::io::FileIO, FileIoError> {
        let mut builder = iceberg::io::FileIOBuilder::new("gcs");

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_prop(iceberg::io::S3_ENDPOINT, endpoint);
        }

        if let Some(GcsCredential::ServiceAccountKey(key)) = credential {
            builder = builder.with_prop(
                iceberg::io::GCS_CREDENTIALS_JSON,
                // guess we're doing base64 now ¯\_(._.)_/¯
                base64::prelude::BASE64_STANDARD.encode(
                    serde_json::to_string(key)
                        .map_err(CredentialsError::from)?
                        .as_bytes(),
                ),
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
        self.normalize_key_prefix()?;
        self.normalize_endpoint()?;

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

        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }

        Ok(())
    }

    #[must_use]
    pub fn generate_catalog_config(&self, warehouse_id: WarehouseIdent) -> CatalogConfig {
        CatalogConfig {
            defaults: HashMap::with_capacity(0),
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
    pub fn base_location(&self) -> Result<Location, ValidationError> {
        let prefix: Vec<String> = self
            .key_prefix
            .as_ref()
            .map(|s| s.split('/').map(std::borrow::ToOwned::to_owned).collect())
            .unwrap_or_default();
        Location::from_str(&format!("gs://{}/", self.bucket))
            .map(|mut l| {
                l.extend(prefix.iter());
                l
            })
            .map_err(|e| ValidationError::InvalidLocation {
                reason: "Invalid GCS location.".to_string(),
                location: format!("gs://{}/", self.bucket),
                source: Some(e.into()),
                storage_type: StorageType::Gcs,
            })
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
        cred: Option<&GcsCredential>,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<TableProperties, TableConfigError> {
        // If vended_credentials is False and remote_signing is False,
        // use remote_signing.

        let mut config = TableProperties::default();
        if let Some(GcsCredential::ServiceAccountKey(cred)) = cred {
            config.insert(&custom::CustomConfig {
                key: "gcs.project-id".to_string(),
                value: cred.project_id.clone(),
            });

            let cred = google_cloud_auth::credentials::CredentialsFile::new_from_str(
                &serde_json::to_string(cred).unwrap(),
            )
            .await
            .unwrap();

            // TODO: Scope credential to table, use signer or some form of sts here?
            let c = google_cloud_auth::project::Config::default().with_scopes(&[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.read_write",
            ]);
            let source =
                google_cloud_auth::project::create_token_source_from_credentials(&cred, &c)
                    .await
                    .unwrap();
            let token = source.token().await.unwrap();

            let sts_client = reqwest::Client::new();
            eprintln!("Location: {}", table_location);

            let response = sts_client
                .post(
                    "https://sts.googleapis.com/v1/token"
                        .parse::<Url>()
                        .unwrap(),
                )
                .json(&STSRequest {
                    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
                    audience: None,
                    scope: None,
                    requested_token_type: "urn:ietf:params:oauth:token-type:access_token"
                        .to_string(),
                    subject_token: token.access_token,
                    subject_token_type: "urn:ietf:params:oauth:token-type:access_token".to_string(),
                    // string
                    //
                    // A JSON-format Credential Access Boundary, encoded with percent encoding.
                    // idk?
                    options: dbg!(urlencoding::encode(serde_json::to_string(&Options {
                        access_boundary: AccessBoundary {
                            access_boundary_rules: vec![AccessBoundaryRule {
                                available_resource: "//storage.googleapis.com/projects/_/buckets/ht-catalog-dev-tobias".to_string(),
                                available_permissions: vec![
                                    "inRole:roles/storage.objectViewer".to_string()
                                ],
                                availability_condition: None,
                            }],
                        },
                        // audiences: vec!["https://sts.googleapis.com".to_string()],
                        user_project: cred.project_id.clone().unwrap(),
                    })
                    .unwrap().as_str()).to_string() ),
                })
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
            panic!("{:?}", response);
            // config.insert(&custom::CustomConfig {
            //     key: "gcs.oauth2.token".to_string(),
            //     value: token.access_token,
            // });
            if let Some(expiry) = token.expiry {
                config.insert(&custom::CustomConfig {
                    key: "gcs.oauth2.token-expires-at".to_string(),
                    // TODO: this seems problematic with timezones but java code expects a unix timestamp.
                    value: expiry.unix_timestamp().to_string(),
                });
            }
        }

        if let Some(endpoint) = &self.endpoint {
            config.insert(&custom::CustomConfig {
                key: "gcs.service.host".to_string(),
                value: endpoint.to_string(),
            });
            config.insert(&custom::CustomConfig {
                key: "gcs.service.path".to_string(),
                value: endpoint.to_string(),
            });
        }

        Ok(config)
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
}

pub(super) fn get_file_io_from_table_config(
    config: &TableProperties,
) -> Result<iceberg::io::FileIO, FileIoError> {
    let mut builder = iceberg::io::FileIOBuilder::new("gcs");

    let hm = config.inner();

    Ok(builder.with_props(hm).build()?)
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

#[derive(Serialize, Deserialize)]
struct STSRequest {
    #[serde(rename = "grantType")]
    // urn:ietf:params:oauth:grant-type:token-exchange
    pub grant_type: String,
    /// The full resource name of the identity provider; for example:
    /// //iam.googleapis.com/projects/<project-number>/locations/global/workloadIdentityPools/<pool-id>/providers/<provider-id>
    /// for workload identity pool providers, or
    /// //iam.googleapis.com/locations/global/workforcePools/<pool-id>/providers/<provider-id> for
    /// workforce pool providers. Required when exchanging an external credential for a Google
    /// access token.
    pub audience: Option<String>,
    /// The OAuth 2.0 scopes to include on the resulting access token, formatted as a list of space-
    /// delimited, case-sensitive strings. Required when exchanging an external credential for a
    /// Google access token.
    pub scope: Option<String>,
    #[serde(rename = "requestedTokenType")]
    // urn:ietf:params:oauth:token-type:access_token
    pub requested_token_type: String,
    #[serde(rename = "subjectToken")]
    pub subject_token: String,
    #[serde(rename = "subjectTokenType")]
    pub subject_token_type: String,
    // serialized json string
    pub options: String,
}

#[derive(Serialize, Deserialize)]
struct Options {
    #[serde(rename = "accessBoundary")]
    access_boundary: AccessBoundary,
    // audiences: Vec<String>,
    #[serde(rename = "userProject")]
    user_project: String,
}

#[derive(Serialize, Deserialize)]
///{
//   "accessBoundaryRules": [
//     {
//       object (AccessBoundaryRule)
//     }
//   ]
// }
struct AccessBoundary {
    #[serde(rename = "accessBoundaryRules")]
    access_boundary_rules: Vec<AccessBoundaryRule>,
}

///{
//   "availableResource": string,
//   "availablePermissions": [
//     string
//   ],
//   "availabilityCondition": {
//     object (Expr)
//   }
// }
#[derive(Serialize, Deserialize)]
struct AccessBoundaryRule {
    #[serde(rename = "availableResource")]
    available_resource: String,
    #[serde(rename = "availablePermissions")]
    available_permissions: Vec<String>,
    #[serde(rename = "availabilityCondition")]
    availability_condition: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::storage::StorageCredential;
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

    #[tokio::test]
    async fn test_can_validate() {
        let cred: StorageCredential = GcsCredential::ServiceAccountKey(
            serde_json::from_str::<GcsServiceKey>(
                r#"{
}"#,
            )
            .unwrap(),
        )
        .into();

        let mut profile: StorageProfile = GcsProfile {
            bucket: "ht-catalog-dev-tobias".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            endpoint: None,
        }
        .into();

        profile.normalize().unwrap();
        profile.validate_access(Some(&cred), None).await.unwrap();
    }
}
