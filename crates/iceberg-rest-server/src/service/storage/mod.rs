mod s3;

use iceberg_rest_service::CatalogConfig;
pub use s3::{S3Credential, S3Profile};
use serde::{Deserialize, Serialize};

/// Storage profile for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageProfile {
    /// S3 storage profile
    #[serde(rename = "s3")]
    S3(S3Profile),
}

#[allow(clippy::module_name_repetitions)]
impl StorageProfile {
    #[must_use]
    pub fn generate_catalog_config(&self) -> CatalogConfig {
        match self {
            StorageProfile::S3(profile) => profile.generate_catalog_config(),
        }
    }
}

/// Storage secret for a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, derive_more::From, utoipa::ToSchema)]
#[serde(tag = "type")]
#[allow(clippy::module_name_repetitions)]
#[schema(rename_all = "kebab-case")]
pub enum StorageCredential {
    /// Credentials for S3 storage
    #[serde(rename = "s3")]
    S3(S3Credential),
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
                region: Some("us-east-1".to_string()),
                assume_role_arn: None
            })
        );
    }

    #[test]
    fn test_s3_secret_de_from_v1() {
        let value = serde_json::json!({
            "type": "s3",
            "credential-type": "access-key",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
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
