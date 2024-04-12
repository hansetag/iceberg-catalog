use super::*;
use axum::response::IntoResponse;
use http::StatusCode;

pub(crate) fn view_router<I: V1ViewsService<S>, S: crate::service::State>() -> Router<ApiContext<S>>
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
    // ToDo: Continue
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
