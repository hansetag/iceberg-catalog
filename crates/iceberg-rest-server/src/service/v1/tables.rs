use super::{
    get, post, ApiContext, CommitTableRequest, CommitTransactionRequest, CreateTableRequest,
    HeaderMap, Json, NamespaceIdent, NamespaceIdentUrl, NamespaceParameters, PaginationQuery, Path,
    Prefix, Query, RegisterTableRequest, RenameTableRequest, Router, State, TableIdent,
    TablesService,
};
use axum::response::IntoResponse;
use http::StatusCode;

#[allow(clippy::too_many_lines)]
pub(crate) fn table_router<I: TablesService<S>, S: crate::service::State>() -> Router<ApiContext<S>>
{
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
