use super::{
    get, post, ApiContext, CommitViewRequest, CreateViewRequest, HeaderMap, Json, NamespaceIdent,
    NamespaceIdentUrl, NamespaceParameters, PaginationQuery, Path, Prefix, Query,
    RenameTableRequest, Router, State, TableIdent, ViewsService,
};
use axum::response::IntoResponse;
use http::StatusCode;

#[allow(clippy::too_many_lines)]
pub(crate) fn views_router<I: ViewsService<S>, S: crate::service::State>() -> Router<ApiContext<S>>
{
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
