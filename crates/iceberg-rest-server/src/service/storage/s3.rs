use crate::CONFIG;
use iceberg_rest_service::CatalogConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use veil::Redact;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub struct S3Profile {
    /// Name of the S3 bucket
    pub bucket: String,
    #[serde(default)]
    /// Optional ARN to assume when accessing the bucket
    pub assume_role_arn: Option<String>,
    /// Optional endpoint to use for S3 requests, if not provided
    /// the region will be used to determine the endpoint.
    /// If both region and endpoint are provided, the endpoint will be used.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Optional region to use for S3 requests.
    #[serde(default)]
    pub region: Option<String>,
}

impl S3Profile {
    #[must_use]
    pub fn generate_catalog_config(&self) -> CatalogConfig {
        CatalogConfig {
            // ToDo: s3.delete-enabled?
            defaults: HashMap::default(),
            overrides: HashMap::from_iter(vec![(
                "s3.signer.url".to_string(),
                CONFIG.s3_signer_uri().to_string(),
            )]),
        }
    }
}

#[derive(Redact, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
#[allow(clippy::module_name_repetitions)]
pub enum S3Credential {
    AccessKey {
        aws_access_key_id: String,
        #[redact(partial)]
        aws_secret_access_key: String,
    },
}
