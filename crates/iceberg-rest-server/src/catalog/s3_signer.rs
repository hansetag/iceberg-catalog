use std::collections::HashMap;
use std::str::FromStr;
use std::time::SystemTime;
use std::vec;

use aws_sigv4::http_request::{sign as aws_sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_sigv4::{self};
use http::HeaderMap;
use iceberg_rest_service::v1::{ApiContext, Prefix, Result};
use iceberg_rest_service::{ErrorModel, IcebergErrorResponse, S3SignRequest, S3SignResponse};

use super::CatalogServer;
use crate::catalog::require_warehouse_id;
use crate::service::secrets::SecretStore;
use crate::service::storage::{S3Profile, StorageCredential};
use crate::service::{auth::AuthZHandler, Catalog, State};
use crate::service::{GetTableMetadataResult, TableIdentUuid};
use crate::WarehouseIdent;

const READ_METHODS: &[&str] = &["GET", "HEAD"];
const WRITE_METHODS: &[&str] = &["PUT", "POST", "DELETE"];
// Keep only the following headers:
const HEADERS_TO_SIGN: [&str; 6] = [
    "amz-sdk-invocation-id",
    "amz-sdk-request",
    "content-length",
    "content-type",
    "expect",
    "host",
];

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    iceberg_rest_service::v1::s3_signer::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn sign(
        prefix: Option<Prefix>,
        _namespace: Option<String>,
        table: Option<String>,
        request: S3SignRequest,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<S3SignResponse> {
        let warehouse_id = require_warehouse_id(prefix.clone())?;

        let S3SignRequest {
            region: request_region,
            uri: request_url,
            method: request_method,
            headers: request_headers,
            body: request_body,
        } = request.clone();

        let include_staged = true;

        // Unfortunately there is currently no way to pass information about warehouse_id & table_id
        // to this function from a get_table or create_table process.
        // Spark does not support per-table signer.uri.
        // Tabular uses a token-exchange to include the information there.
        // We are looking for the path in the database, which allows us to also work with AuthN solutions
        // that do not support custom data in tokens. Its however also not ideal. Perspectively, we should
        // try to get per-table signer.uri support in Spark.
        let (table_id, table_metadata) = if let Ok(table_id) = require_table_id(table.clone()) {
            (table_id, None)
        } else {
            // Ideally we could get rid of this else clause.
            let location = parse_s3_url_to_location(&request_url)?;
            let table_metadata = C::get_table_metadata_by_s3_location(
                &warehouse_id,
                &location,
                include_staged,
                state.v1_state.catalog.clone(),
            )
            .await
            .map_err(|e| {
                ErrorModel::builder()
                    .code(http::StatusCode::UNAUTHORIZED.into())
                    .message("Unauthorized".to_string())
                    .r#type("InvalidLocation".to_string())
                    .stack(Some(vec![format!("{e:?}")]))
                    .build()
            })?;

            // s3://tests/c3ebf200-1e94-11ef-9ed7-7bebc6e5a664/018fca00-6bba-7669-8a10-5dc42e37cd63/data/00001-1-840f0dc8-a888-4522-a327-12187ce32dbd-0-00001.parquet
            // s3://tests/c3ebf200-1e94-11ef-9ed7-7bebc6e5a664/018fca00-6bba-7669-8a10-5dc42e37cd63

            (table_metadata.table_id, Some(table_metadata))
        };

        // First check - fail fast if requested table is not allowed.
        // We also need to check later if the path matches the table location.
        validate_table_method::<A>(
            &request_method,
            &headers,
            &warehouse_id,
            &table_id,
            state.v1_state.auth,
        )
        .await?;
        drop(headers);

        // Included staged tables as this might be a commit
        let GetTableMetadataResult {
            table: _,
            table_id,
            warehouse_id: _,
            location,
            metadata_location: _,
            storage_secret_ident,
            storage_profile,
        } = if let Some(table_metadata) = table_metadata {
            table_metadata
        } else {
            C::get_table_metadata_by_id(
                &warehouse_id,
                &table_id,
                include_staged,
                state.v1_state.catalog,
            )
            .await?
        };

        let extend_err = |mut e: IcebergErrorResponse| {
            e.error.push_to_stack(format!("Table ID: {table_id}"));
            e.error.push_to_stack(format!("Request URI: {request_url}"));
            e.error.push_to_stack(format!("Table Location: {location}"));
            e
        };

        let storage_profile = storage_profile
            .try_into_s3(http::StatusCode::INTERNAL_SERVER_ERROR.into())
            .map_err(extend_err)?;

        validate_uri(&request_url, &location, &storage_profile).map_err(extend_err)?;
        validate_region(&request_region, &storage_profile).map_err(extend_err)?;

        // If all is good, we need the storage secret
        let storage_secret = if let Some(storage_secret_ident) = storage_secret_ident {
            Some(
                S::get_secret_by_id::<StorageCredential>(
                    &storage_secret_ident,
                    state.v1_state.secrets,
                )
                .await?
                .secret,
            )
        } else {
            None
        }
        .map(|secret| {
            secret
                .try_into_s3(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .map_err(extend_err)
        })
        .transpose()?;

        let credentials: aws_credential_types::Credentials = storage_profile
            .get_aws_sdk_credentials(storage_secret.as_ref())
            .map_err(extend_err)?;

        sign(
            credentials,
            request_body,
            &request_region,
            &request_url,
            &request_method,
            &request_headers,
        )
        .map_err(extend_err)
    }
}

fn sign(
    credentials: aws_credential_types::Credentials,
    request_body: Option<String>,
    request_region: &str,
    request_url: &url::Url,
    request_method: &http::Method,
    request_headers: &HashMap<String, Vec<String>>,
) -> Result<S3SignResponse> {
    let body = request_body.map(std::string::String::into_bytes);
    let signable_body = if let Some(body) = &body {
        SignableBody::Bytes(body)
    } else {
        SignableBody::UnsignedPayload
    };

    let mut sign_settings = SigningSettings::default();
    sign_settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(request_region)
        .name("s3")
        .time(SystemTime::now())
        .settings(sign_settings)
        .build()
        .map_err(|e| {
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Failed to create signing params".to_string())
                .r#type("FailedToCreateSigningParams".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
        .into();

    let signable_request_headers = request_headers
        .iter()
        .filter(|(k, _)| HEADERS_TO_SIGN.contains(&k.to_lowercase().as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<HashMap<_, _>>();
    let mut headers_vec: Vec<(String, String)> = Vec::new();

    for (key, values) in signable_request_headers.clone() {
        for value in values {
            headers_vec.push((key.clone(), value));
        }
    }

    let encoded_uri = partially_decode_uri(request_url)?;
    let signable_request = SignableRequest::new(
        request_method.as_str(),
        encoded_uri.to_string(),
        headers_vec.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        signable_body,
    )
    .map_err(|e| {
        ErrorModel::builder()
            .code(http::StatusCode::BAD_REQUEST.into())
            .message("Request is not signable".to_string())
            .r#type("FailedToCreateSignableRequest".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let (signing_instructions, _signature) = aws_sign(signable_request, &signing_params)
        .map_err(|e| {
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Failed to sign request".to_string())
                .r#type("FailedToSignRequest".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
        .into_parts();

    let mut output_uri = encoded_uri.clone();
    for (key, value) in signing_instructions.params() {
        output_uri.query_pairs_mut().append_pair(key, value);
    }
    let mut output_headers = signable_request_headers.clone();
    for (key, value) in signing_instructions.headers() {
        output_headers.insert(key.to_string(), vec![value.to_string()]);
    }

    let sign_response = S3SignResponse {
        uri: output_uri,
        headers: output_headers,
    };

    Ok(sign_response)
}

fn partially_decode_uri(uri: &url::Url) -> Result<url::Url> {
    // We only modify path segments. Iterate over all path segments and unr urlencoding::decode them.
    let mut new_uri = uri.clone();
    let path_segments = new_uri
        .path_segments()
        .map(std::iter::Iterator::collect::<Vec<_>>)
        .unwrap_or_default();

    let mut new_path_segments = Vec::new();
    for segment in path_segments {
        new_path_segments.push(
            urlencoding::decode(segment)
                .map(|s| s.replace(' ', "+"))
                .map_err(|e| {
                    ErrorModel::builder()
                        .code(http::StatusCode::BAD_REQUEST.into())
                        .message("Failed to decode URI segment".to_string())
                        .r#type("FailedToDecodeURISegment".to_string())
                        .stack(Some(vec![e.to_string()]))
                        .build()
                })?,
        );
    }

    new_uri.set_path(&new_path_segments.join("/"));
    Ok(new_uri)
}

fn require_table_id(table_id: Option<String>) -> Result<TableIdentUuid> {
    table_id
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message("A Table ID is required as part of the URL path".to_string())
                .r#type("TableIdRequired".to_string())
                .build()
                .into(),
        )
        .and_then(|table_id| TableIdentUuid::from_str(&table_id).map_err(Into::into))
}

fn validate_region(region: &str, storage_profile: &S3Profile) -> Result<()> {
    if region != storage_profile.region {
        return Err(ErrorModel::builder()
            .code(http::StatusCode::BAD_REQUEST.into())
            .message("Region does not match storage profile".to_string())
            .r#type("RegionMismatch".to_string())
            .build()
            .into());
    }

    Ok(())
}

async fn validate_table_method<A: AuthZHandler>(
    method: &http::Method,
    headers: &HeaderMap,
    warehouse_id: &WarehouseIdent,
    table_id: &TableIdentUuid,
    auth_state: A::State,
) -> Result<()> {
    // First check - fail fast if requested table is not allowed.
    // We also need to check later if the path matches the table location.
    if WRITE_METHODS.contains(&method.as_str()) {
        // We specify namespace as none for AuthZ check because we don't want to grant access to potentially
        // locations not known to the catalog.
        A::check_commit_table(headers, warehouse_id, Some(table_id), None, auth_state).await?;
    } else if READ_METHODS.contains(&method.as_str()) {
        A::check_load_table(headers, warehouse_id, None, Some(table_id), auth_state).await?;
    } else {
        return Err(ErrorModel::builder()
            .code(http::StatusCode::METHOD_NOT_ALLOWED.into())
            .message("Method not allowed".to_string())
            .r#type("MethodNotAllowed".to_string())
            .build()
            .into());
    }

    Ok(())
}

const AWS_S3_ACCESS_POINTS: &[&str] = &["s3", "s3.dualstack", "s3-fips.dualstack", "s3-fips"];

#[allow(clippy::too_many_lines)]
fn validate_uri(
    // i.e. https://bucket.s3.region.amazonaws.com/key
    request_uri: &url::Url,
    // i.e. s3://bucket/key
    table_location: &str,
    storage_profile: &S3Profile,
) -> Result<()> {
    let table_location = url::Url::parse(table_location.trim_end_matches('/')).map_err(|e| {
        ErrorModel::builder()
            .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Failed to parse table location".to_string())
            .r#type("FailedToParseTableLocation".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;
    let table_bucket = table_location.host_str().ok_or_else(|| {
        ErrorModel::builder()
            .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Table location does not have a bucket".to_string())
            .r#type("TableLocationNoBucket".to_string())
            .build()
    })?;
    let table_key_virtual_host: Vec<_> = table_location
        .path_segments()
        .map(std::iter::Iterator::collect)
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Table location does not have a key".to_string())
                .r#type("TableLocationNoKey".to_string())
                .build(),
        )?;
    let table_key_path_style = vec![table_bucket]
        .into_iter()
        .chain(table_key_virtual_host.clone())
        .collect::<Vec<_>>();

    let request_key: Vec<_> = request_uri
        .path_segments()
        .map(std::iter::Iterator::collect)
        .unwrap_or_default();

    // Obtain tuples of (scheme, host) for the endpoint candidates.
    // We need multiple candidates only for AWS S3, as there are multiple access points such as s3, s3.dualstack, etc.
    let table_endpoint_candidates = if let Some(endpoint) = &storage_profile.endpoint {
        let endpoint = url::Url::parse(endpoint).map_err(|e| {
            ErrorModel::builder()
                .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Failed to parse storage profile endpoint".to_string())
                .r#type("FailedToParseStorageProfileEndpoint".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

        vec![(
            endpoint.scheme().to_string(),
            endpoint
                .host()
                .ok_or(
                    ErrorModel::builder()
                        .code(http::StatusCode::INTERNAL_SERVER_ERROR.into())
                        .message("Storage profile endpoint does not have a host".to_string())
                        .r#type("StorageProfileNoHost".to_string())
                        .build(),
                )?
                .to_string(),
        )]
    } else {
        // If no endpoint is specified explicitly, we check against known AWS S3 access points.
        let table_region = &storage_profile.region;

        AWS_S3_ACCESS_POINTS
            .iter()
            .map(|access_point| {
                (
                    "https".to_string(),
                    format!("{access_point}.{table_region}.amazonaws.com"),
                )
            })
            .collect::<Vec<_>>()
    };

    // There are two ways to access S3 buckets: path style and virtual host style.
    // Virtual Host style: https://<bucket>.s3.<region>.amazonaws.com/<key>
    // Path style: https://s3.<region>.amazonaws.com/<bucket>/<key>
    // As both are valid, we need to check both.

    // Case 1: Virtual Host style
    let allowed_virtual_hosts = table_endpoint_candidates
        .iter()
        .map(|(scheme, host)| (scheme, format!("{table_bucket}.{host}")))
        .collect::<Vec<_>>();
    // Case 2: Path style
    let allowed_path_style = &table_endpoint_candidates;

    if allowed_virtual_hosts.iter().any(|(scheme, host)| {
        request_uri.scheme() == *scheme && request_uri.host_str() == Some(host)
    }) {
        // Case 1: Virtual Host style
        let len = table_key_virtual_host.len();
        if request_key.len() < len || request_key[..len] != table_key_virtual_host {
            Err(ErrorModel::builder()
                .code(http::StatusCode::FORBIDDEN.into())
                .message("Request URI does not match table location".to_string())
                .r#type("VirtualHostURIMismatch".to_string())
                .stack(Some(vec![
                    format!("Expected Key: {table_key_virtual_host:?}"),
                    format!("Actual Key: {request_key:?}"),
                ]))
                .build()
                .into())
        } else {
            Ok(())
        }
    } else if allowed_path_style.iter().any(|(scheme, host)| {
        request_uri.scheme() == scheme && request_uri.host_str() == Some(host)
    }) {
        // Case 2: Path style
        let len = table_key_path_style.len();
        if request_key.len() < len || request_key[..len] != table_key_path_style {
            Err(ErrorModel::builder()
                .code(http::StatusCode::FORBIDDEN.into())
                .message("Request URI does not match table location".to_string())
                .r#type("PathStyleHostMismatch".to_string())
                .stack(Some(vec![
                    format!("Expected Key: {table_key_path_style:?}"),
                    format!("Actual Key: {request_key:?}"),
                ]))
                .build()
                .into())
        } else {
            Ok(())
        }
    } else {
        Err(ErrorModel::builder()
            .code(http::StatusCode::FORBIDDEN.into())
            .message("Request URI does not match table location".to_string())
            .r#type("RequestUriMismatch".to_string())
            .build()
            .into())
    }
}

// Parsing from http url to s3:// url is messy.
// We should find a way to pass the required information from
// the get_table request to the signer.
// This function only supports path-style access
// if the URIs host is a single identifier (no dots) or an IP address.
fn parse_s3_url_to_location(uri: &url::Url) -> Result<String> {
    // We need to check both virtual host and path style.
    // Virtual Host style: https://<bucket>.<endpoint with optional.points>/<key>
    // Path style: https://<endpoint with optional.points>/<bucket>/<key>
    let host = uri.host().ok_or(
        ErrorModel::builder()
            .code(http::StatusCode::BAD_REQUEST.into())
            .message("URI does not have a host".to_string())
            .r#type("UriNoHost".to_string())
            .build(),
    )?;

    let path = uri.path().trim_start_matches('/');

    match host {
        url::Host::Domain(domain) => {
            if domain.contains('.') {
                // Virtual Host style
                let parts = domain.split('.').collect::<Vec<_>>();
                let bucket = parts[0];
                Ok(format!("s3://{bucket}/{path}"))
            } else {
                // Path style
                Ok(format!("s3://{path}"))
            }
        }
        url::Host::Ipv4(_) | url::Host::Ipv6(_) => Ok(format!("s3://{path}")),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TC {
        request_uri: &'static str,
        table_location: &'static str,
        region: &'static str,
        endpoint: Option<&'static str>,
        expected_outcome: bool,
    }

    fn make_storage_profile(test_case: &TC) -> S3Profile {
        S3Profile {
            bucket: "should-not-be-used".to_string(),
            endpoint: test_case.endpoint.map(std::string::ToString::to_string),
            region: test_case.region.to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
        }
    }

    fn run_validate_uri_test(test_case: &TC) {
        let storage_profile = make_storage_profile(test_case);
        let request_uri = url::Url::parse(test_case.request_uri).unwrap();
        let table_location = test_case.table_location;
        let result = validate_uri(&request_uri, table_location, &storage_profile);
        assert_eq!(result.is_ok(), test_case.expected_outcome);
    }

    #[test]
    fn test_parse_s3_url_to_location() {
        let cases = vec![
            ("https://foo.endpoint.com/bar/a/key", "s3://foo/bar/a/key"),
            ("https://endpoint/bar/a/key", "s3://bar/a/key"),
            ("http://localhost:9000/bar/a/key", "s3://bar/a/key"),
            ("http://192.168.1.1/bar/a/key", "s3://bar/a/key"),
            ("https://foo.bar.com/key", "s3://foo/key"),
        ];

        for (uri, expected) in cases {
            let uri = url::Url::parse(uri).unwrap();
            let result = parse_s3_url_to_location(&uri).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_uri_virtual_host() {
        let cases = vec![
            // Basic bucket-style
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic bucket-style with special characters in key
            TC {
                request_uri:
                    "https://bucket.s3.my-region.amazonaws.com/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://bucket.s3.my-region.amazonaws.com/key-2",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://bucket-2.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://bucket.with.point.s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_uri_path_style() {
        let cases = vec![
            // Basic path-style
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Allow subpaths
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key/foo/file.parquet",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Basic path-style with special characters in key
            TC {
                request_uri:
                    "https://s3.my-region.amazonaws.com/bucket/key/with-special-chars%20/foo",
                table_location: "s3://bucket/key/with-special-chars%20/foo",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
            // Wrong key
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket/key-2",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Wrong bucket
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket-2/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
            // Bucket with points
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/bucket.with.point/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: true,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_uri_bucket_missing() {
        let cases = vec![
            // Bucket missing
            TC {
                request_uri: "https://s3.my-region.amazonaws.com/key",
                table_location: "s3://bucket/key",
                region: "my-region",
                endpoint: None,
                expected_outcome: false,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_uri_custom_endpoint() {
        let cases = vec![
            // Endpoint specified
            TC {
                request_uri: "https://bucket.with.point.s3.my-service.example.com/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
                endpoint: Some("https://s3.my-service.example.com"),
                expected_outcome: true,
            },
            // Endpoint specified but wrong
            TC {
                request_uri: "https://bucket.with.point.s3.my-service.example.com/key",
                table_location: "s3://bucket.with.point/key",
                region: "my-region",
                endpoint: Some("https://my-service.example.com"),
                expected_outcome: false,
            },
        ];

        for tc in cases {
            run_validate_uri_test(&tc);
        }
    }

    #[test]
    fn test_validate_region() {
        let storage_profile = S3Profile {
            bucket: "should-not-be-used".to_string(),
            endpoint: None,
            region: "my-region".to_string(),
            assume_role_arn: None,
            path_style_access: None,
            key_prefix: None,
        };

        let result = validate_region("my-region", &storage_profile);
        assert!(result.is_ok());

        let result = validate_region("wrong-region", &storage_profile);
        assert!(result.is_err());
    }
}
