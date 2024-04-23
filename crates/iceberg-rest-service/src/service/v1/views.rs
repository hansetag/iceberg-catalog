use super::{
    get, namespace::NamespaceIdentUrl, post, ApiContext, CommitViewRequest, CreateViewRequest,
    HeaderMap, Json, ListTablesResponse, LoadViewResult, NamespaceIdent, NamespaceParameters,
    PaginationQuery, Path, Prefix, Query, RenameTableRequest, Result, Router, State, TableIdent,
};
use axum::async_trait;
use axum::response::IntoResponse;
use http::StatusCode;

#[async_trait]
pub trait Service<S: crate::service::State>
where
    Self: Send + Sync + Clone + 'static,
{
    /// List all views underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        query: PaginationQuery,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<ListTablesResponse>;

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadViewResult>;

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadViewResult>;

    /// Commit updates to a view.
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<LoadViewResult>;

    /// Remove a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;

    /// Rename a view from its current name to a new name
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<S>,
        headers: HeaderMap,
    ) -> Result<()>;
}

#[allow(clippy::too_many_lines)]
pub fn router<I: Service<S>, S: crate::service::State>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/views
        .route(
            "/:prefix/namespaces/:namespace/views",
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<PaginationQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    {
                        I::list_views(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            query,
                            api_context,
                            headers,
                        )
                    }
                },
            )
            // Create a view in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateViewRequest>| {
                    {
                        I::create_view(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            ),
        )
        .route(
            "/namespaces/:namespace/views",
            get(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 Query(query): Query<PaginationQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    {
                        I::list_views(
                            NamespaceParameters {
                                prefix: None,
                                namespace: namespace.into(),
                            },
                            query,
                            api_context,
                            headers,
                        )
                    }
                },
            )
            .post(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateViewRequest>| {
                    {
                        I::create_view(
                            NamespaceParameters {
                                prefix: None,
                                namespace: namespace.into(),
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/views/{view}
        .route(
            "/:prefix/namespaces/:namespace/views/:view",
            get(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    {
                        I::load_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                    }
                },
            )
            .post(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CommitViewRequest>| {
                    {
                        I::commit_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                                view,
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            )
            .delete(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    {
                        I::drop_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            )
            .head(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    {
                        I::view_exists(
                            ViewParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
        .route(
            "/namespaces/:namespace/views/:view",
            get(
                |Path((namespace, view)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    {
                        I::load_view(
                            ViewParameters {
                                prefix: None,
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                    }
                },
            )
            .post(
                |Path((namespace, view)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CommitViewRequest>| {
                    {
                        I::commit_view(
                            ViewParameters {
                                prefix: None,
                                namespace: namespace.into(),
                                view,
                            },
                            request,
                            api_context,
                            headers,
                        )
                    }
                },
            )
            .delete(
                |Path((namespace, view)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    {
                        I::drop_view(
                            ViewParameters {
                                prefix: None,
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            )
            .head(
                |Path((namespace, view)): Path<(NamespaceIdentUrl, TableIdent)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    {
                        I::view_exists(
                            ViewParameters {
                                prefix: None,
                                namespace: namespace.into(),
                                view,
                            },
                            api_context,
                            headers,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
        // /{prefix}/views/rename
        .route(
            "/:prefix/views/rename",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<RenameTableRequest>| async {
                    {
                        I::rename_view(Some(prefix), request, api_context, headers)
                            .await
                            .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
        .route(
            "/views/rename",
            post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<RenameTableRequest>| async {
                    {
                        I::rename_view(None, request, api_context, headers)
                            .await
                            .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct ViewParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The namespace to load metadata for
    pub namespace: NamespaceIdent,
    /// The table to load metadata for
    pub view: TableIdent,
}
