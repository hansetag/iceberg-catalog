use super::{
    get, namespace::NamespaceIdentUrl, post, ApiContext, CommitTableRequest, CommitTableResponse,
    CommitTransactionRequest, CreateTableRequest, HeaderMap, Json, ListTablesResponse,
    LoadTableResult, NamespaceIdent, NamespaceParameters, PaginationQuery, Path, Prefix, Query,
    RegisterTableRequest, RenameTableRequest, Result, Router, State, TableIdent,
};
use axum::async_trait;
use axum::response::IntoResponse;
use http::StatusCode;

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        query: PaginationQuery,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<ListTablesResponse>;

    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadTableResult>;

    /// Register a table in the given namespace using given metadata file location
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadTableResult>;

    /// Load a table from the catalog
    async fn load_table(
        parameters: TableParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadTableResult>;

    /// Commit updates to a table
    async fn commit_table(
        parameters: TableParameters,
        request: CommitTableRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<CommitTableResponse>;

    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;

    /// Commit updates to multiple tables in an atomic operation
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;
}

#[allow(clippy::too_many_lines)]
pub fn table_router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables
        .route(
            "/:prefix/namespaces/:namespace/tables",
            // Create a table in the given namespace
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<PaginationQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::list_tables(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        query,
                        api_context,
                        headers,
                    )
                },
            )
            // Create a table in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateTableRequest>| {
                    I::create_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        headers,
                    )
                },
            ),
        )
        .route(
            "/namespaces/:namespace/tables",
            // Create a table in the given namespace
            get(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 Query(query): Query<PaginationQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::list_tables(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        query,
                        api_context,
                        headers,
                    )
                },
            )
            // Create a table in the given namespace
            .post(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateTableRequest>| {
                    I::create_table(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        headers,
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
                 headers: HeaderMap,
                 Json(request): Json<RegisterTableRequest>| {
                    I::register_table(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        headers,
                    )
                },
            ),
        )
        .route(
            "/namespaces/:namespace/register",
            // Register a table in the given namespace using given metadata file location
            post(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<RegisterTableRequest>| {
                    I::register_table(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        request,
                        api_context,
                        headers,
                    )
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/tables/{table}
        .route(
            "/:prefix/namespaces/:namespace/tables/:table",
            // Load a table from the catalog
            get(
                |Path((prefix, namespace, table)): Path<(
                    Prefix,
                    NamespaceIdentUrl,
                    TableIdent,
                )>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::load_table(
                        TableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
                    )
                },
            )
            // Commit updates to a table
            .post(
                |Path((prefix, namespace, table)): Path<(
                    Prefix,
                    NamespaceIdentUrl,
                    TableIdent,
                )>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CommitTableRequest>| {
                    I::commit_table(
                        TableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table,
                        },
                        request,
                        api_context,
                        headers,
                    )
                },
            )
            // Drop a table from the catalog
            .delete(
                |Path((prefix, namespace, table)): Path<(
                    Prefix,
                    NamespaceIdentUrl,
                    TableIdent,
                )>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::drop_table(
                        TableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Check if a table exists
            .head(
                |Path((prefix, namespace, table)): Path<(
                    Prefix,
                    NamespaceIdentUrl,
                    TableIdent,
                )>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::table_exists(
                        TableParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        .route(
            "/namespaces/:namespace/tables/:table",
            // Load a table from the catalog
            get(
                |Path((namespace, table)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::load_table(
                        TableParameters {
                            prefix: None,
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
                    )
                },
            )
            // Commit updates to a table
            .post(
                |Path((namespace, table)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CommitTableRequest>| {
                    I::commit_table(
                        TableParameters {
                            prefix: None,
                            namespace: namespace.into(),
                            table,
                        },
                        request,
                        api_context,
                        headers,
                    )
                },
            )
            // Drop a table from the catalog
            .delete(
                |Path((namespace, table)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::drop_table(
                        TableParameters {
                            prefix: None,
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Check if a table exists
            .head(
                |Path((namespace, table)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::table_exists(
                        TableParameters {
                            prefix: None,
                            namespace: namespace.into(),
                            table,
                        },
                        api_context,
                        headers,
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
                 headers: HeaderMap,
                 Json(request): Json<RenameTableRequest>| {
                    async {
                        I::rename_table(Some(prefix), request, api_context, headers)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                    }
                },
            ),
        )
        .route(
            "/tables/rename",
            // Rename a table in the given namespace
            post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<RenameTableRequest>| {
                    async {
                        I::rename_table(None, request, api_context, headers)
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
                 headers: HeaderMap,
                 Json(request): Json<CommitTransactionRequest>| {
                    I::commit_transaction(Some(prefix), request, api_context, headers)
                },
            ),
        )
        .route(
            "/transactions/commit",
            // Commit updates to multiple tables in an atomic operation
            post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CommitTransactionRequest>| {
                    I::commit_transaction(None, request, api_context, headers)
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct TableParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The namespace to load metadata for
    pub namespace: NamespaceIdent,
    /// The table to load metadata for
    pub table: TableIdent,
}
