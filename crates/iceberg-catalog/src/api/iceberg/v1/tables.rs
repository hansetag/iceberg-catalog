use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::namespace::{NamespaceIdentUrl, NamespaceParameters, PaginationQuery};
use crate::api::{
    ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateTableRequest, ListTablesResponse, LoadTableResult, RegisterTableRequest,
    RenameTableRequest, Result,
};
use crate::request_metadata::RequestMetadata;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{async_trait, Extension, Json, Router};
use http::{HeaderMap, StatusCode};
use iceberg::TableIdent;

#[async_trait]
pub trait Service<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        query: PaginationQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse>;

    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult>;

    /// Register a table in the given namespace using given metadata file location
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult>;

    /// Load a table from the catalog
    async fn load_table(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult>;

    /// Commit updates to a table
    async fn commit_table(
        parameters: TableParameters,
        request: CommitTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse>;

    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    /// Commit updates to multiple tables in an atomic operation
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}

#[allow(clippy::too_many_lines)]
pub fn router<I: Service<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables
        .route(
            "/:prefix/namespaces/:namespace/tables",
            // Create a table in the given namespace
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<PaginationQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::list_tables(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        query,
                        api_context,
                        metadata,
                    )
                },
            )
            // Create a table in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateTableRequest>| {
                    I::create_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        parse_data_access(&headers),
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/register
        .route(
            "/:prefix/namespaces/:namespace/register",
            // Register a table in the given namespace using given metadata file location
            post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RegisterTableRequest>| {
                    I::register_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        metadata,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/tables/{table}
        .route(
            "/:prefix/namespaces/:namespace/tables/:table",
            // Load a table from the catalog
            get(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    I::load_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
                            },
                        },
                        parse_data_access(&headers),
                        api_context,
                        metadata,
                    )
                },
            )
            // Commit updates to a table
            .post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitTableRequest>| {
                    I::commit_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
                            },
                        },
                        request,
                        api_context,
                        metadata,
                    )
                },
            )
            // Drop a table from the catalog
            .delete(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::drop_table(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
                            },
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Check if a table exists
            .head(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    I::table_exists(
                        TableParameters {
                            prefix: Some(prefix),
                            table: TableIdent {
                                namespace: namespace.into(),
                                name: table,
                            },
                        },
                        api_context,
                        metadata,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        // /{prefix}/tables/rename
        .route(
            "/:prefix/tables/rename",
            // Rename a table in the given namespace
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RenameTableRequest>| {
                    async {
                        I::rename_table(Some(prefix), request, api_context, metadata)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                    }
                },
            ),
        )
        // /{prefix}/transactions/commit
        .route(
            "/:prefix/transactions/commit",
            // Commit updates to multiple tables in an atomic operation
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitTransactionRequest>| {
                    I::commit_transaction(Some(prefix), request, api_context, metadata)
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct TableParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The table to load metadata for
    pub table: TableIdent,
}

pub const DATA_ACCESS_HEADER: &str = "X-Iceberg-Access-Delegation";

#[derive(Debug, Clone)]
// Modeled as a string to enable multiple values to be specified.
pub struct DataAccess {
    pub vended_credentials: bool,
    pub remote_signing: bool,
}

pub(crate) fn parse_data_access(headers: &HeaderMap) -> DataAccess {
    let header = headers
        .get_all(DATA_ACCESS_HEADER)
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect::<Vec<_>>();
    let vended_credentials = header.contains(&"vended-credentials");
    let remote_signing = header.contains(&"remote-signing");
    DataAccess {
        vended_credentials,
        remote_signing,
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    #[test]
    fn test_parse_data_access() {
        let headers = http::header::HeaderMap::new();
        let data_access = super::parse_data_access(&headers);
        assert!(!data_access.vended_credentials);
        assert!(!data_access.remote_signing);
    }

    #[test]
    fn test_parse_data_access_capitalization() {
        let mut headers = http::header::HeaderMap::new();
        headers.insert(
            http::header::HeaderName::from_str(super::DATA_ACCESS_HEADER).unwrap(),
            http::header::HeaderValue::from_static("vended-credentials"),
        );
        let data_access = super::parse_data_access(&headers);
        assert!(data_access.vended_credentials);
        assert!(!data_access.remote_signing);

        let mut headers = http::header::HeaderMap::new();
        headers.insert(
            "x-iceberg-access-delegation",
            http::header::HeaderValue::from_static("vended-credentials"),
        );
        let data_access = super::parse_data_access(&headers);
        assert!(data_access.vended_credentials);
        assert!(!data_access.remote_signing);
    }
}
