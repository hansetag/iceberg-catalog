use super::{
    get, post, ApiContext, CreateNamespaceRequest, HeaderMap, Json, NamespaceIdent,
    NamespaceService, PageToken, Path, Prefix, Query, Result, Router, State,
    UpdateNamespacePropertiesRequest,
};
use axum::response::IntoResponse;
use http::StatusCode;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
pub(crate) struct NamespaceIdentUrl(Vec<String>);

impl From<NamespaceIdentUrl> for NamespaceIdent {
    fn from(param: NamespaceIdentUrl) -> Self {
        NamespaceIdent::from_vec(param.0).unwrap()
    }
}

impl<'de> Deserialize<'de> for NamespaceIdentUrl {
    fn deserialize<D>(deserializer: D) -> Result<NamespaceIdentUrl, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Split on multipart \u001f
        Ok(NamespaceIdentUrl(
            s.split('\u{1f}')
                .map(std::string::ToString::to_string)
                .collect(),
        ))
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn namespace_router<I: NamespaceService<S>, S: crate::service::State>(
) -> Router<ApiContext<S>> {
    Router::new()
        // List Namespaces
        .route(
            "/:prefix/namespaces",
            // List Namespaces
            get(
                |Path(prefix): Path<Prefix>,
                 Query(query): Query<ListNamespacesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::list_namespaces(Some(prefix), query, api_context, headers)
                },
            )
            // Create Namespace
            .post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateNamespaceRequest>| {
                    I::create_namespace(Some(prefix), request, api_context, headers)
                },
            ),
        )
        .route(
            "/namespaces",
            get(
                |Query(query): Query<ListNamespacesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::list_namespaces(None, query, api_context, headers)
                },
            ) // Create Namespace
            .post(
                |State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<CreateNamespaceRequest>| {
                    I::create_namespace(None, request, api_context, headers)
                },
            ),
        )
        // Load the metadata properties for a namespace
        .route(
            "/:prefix/namespaces/:namespace",
            // Load the metadata properties for a namespace
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::load_namespace_metadata(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        api_context,
                        headers,
                    )
                },
            )
            // Check if a namespace exists
            .head(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::namespace_exists(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Drop a namespace from the catalog. Namespace must be empty.
            .delete(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::drop_namespace(
                        NamespaceParameters {
                            prefix: Some(prefix),
                            namespace: namespace.into(),
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
            "/namespaces/:namespace",
            // Load the metadata properties for a namespace
            get(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| {
                    I::load_namespace_metadata(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        api_context,
                        headers,
                    )
                },
            )
            // Check if a namespace exists
            .head(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::namespace_exists(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            )
            // Drop a namespace from the catalog. Namespace must be empty.
            .delete(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap| async {
                    I::drop_namespace(
                        NamespaceParameters {
                            prefix: None,
                            namespace: namespace.into(),
                        },
                        api_context,
                        headers,
                    )
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
        // UpdateNamespacePropertiesResponse
        .route(
            "/:prefix/namespaces/:namespace/properties",
            // Set or remove properties on a namespace
            post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<UpdateNamespacePropertiesRequest>| {
                    I::update_namespace_properties(
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
            "/namespaces/:namespace/properties",
            // Set or remove properties on a namespace
            post(
                |Path(namespace): Path<NamespaceIdentUrl>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Json(request): Json<UpdateNamespacePropertiesRequest>| {
                    I::update_namespace_properties(
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNamespacesQuery {
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    pub page_token: PageToken,
    /// For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`.
    #[serde(rename = "pageSize")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i32>,
    /// An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (`0x1F`) byte.
    #[serde(rename = "parent")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginationQuery {
    #[serde(skip_serializing_if = "PageToken::skip_serialize")]
    pub page_token: PageToken,
    /// For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`.
    #[serde(rename = "pageSize")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i32>,
}

impl From<ListNamespacesQuery> for PaginationQuery {
    fn from(query: ListNamespacesQuery) -> Self {
        PaginationQuery {
            page_token: query.page_token,
            page_size: query.page_size,
        }
    }
}

// Deliberately not ser / de so that it can't be used in the router directly
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The namespace to load metadata for
    pub namespace: NamespaceIdent,
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use axum::async_trait;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn test_namespace_params() {
        use tower::ServiceExt;

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl crate::service::State for ThisState {}

        // ToDo: Use Mock instead for impl. I couldn't get mockall to work though.
        #[async_trait]
        impl NamespaceService<ThisState> for TestService {
            async fn list_namespaces(
                prefix: Option<Prefix>,
                query: ListNamespacesQuery,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<ListNamespacesResponse> {
                assert_eq!(prefix.unwrap().into_string(), "test".to_string());
                assert_eq!(query.page_size, Some(10));
                assert_eq!(query.page_token, PageToken::Empty);

                Err(ErrorModel::builder()
                    .message("The server does not support this operation".to_string())
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            /// Create a namespace, with an optional set of properties.
            /// The server might also add properties, such as `last_modified_time` etc.
            async fn create_namespace(
                _prefix: Option<Prefix>,
                _request: CreateNamespaceRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<CreateNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Return all stored metadata properties for a given namespace
            async fn load_namespace_metadata(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<GetNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Check if a namespace exists
            async fn namespace_exists(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Drop a namespace from the catalog. Namespace must be empty.
            async fn drop_namespace(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Set or remove properties on a namespace
            async fn update_namespace_properties(
                _parameters: NamespaceParameters,
                _request: UpdateNamespacePropertiesRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<UpdateNamespacePropertiesResponse> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = namespace_router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        let req = http::Request::builder()
            .uri("/test/namespaces?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let r = router.oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
    }

    #[tokio::test]
    async fn test_namespace_optional_prefix() {
        use super::*;
        use tower::ServiceExt;

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl crate::service::State for ThisState {}

        #[async_trait]
        impl NamespaceService<ThisState> for TestService {
            async fn list_namespaces(
                prefix: Option<Prefix>,
                query: ListNamespacesQuery,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<ListNamespacesResponse> {
                assert_eq!(prefix, None);
                assert_eq!(query.page_size, Some(10));
                assert_eq!(query.page_token, PageToken::Empty);

                Err(ErrorModel::builder()
                    .message("The server does not support this operation".to_string())
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            /// Return all stored metadata properties for a given namespace
            async fn load_namespace_metadata(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<GetNamespaceResponse> {
                panic!("Should not be called");
            }

            async fn create_namespace(
                _prefix: Option<Prefix>,
                _request: CreateNamespaceRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<CreateNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Check if a namespace exists
            async fn namespace_exists(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Drop a namespace from the catalog. Namespace must be empty.
            async fn drop_namespace(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Set or remove properties on a namespace
            async fn update_namespace_properties(
                _parameters: NamespaceParameters,
                _request: UpdateNamespacePropertiesRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<UpdateNamespacePropertiesResponse> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = namespace_router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        let req = http::Request::builder()
            .uri("/namespaces?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let r = router.oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
    }

    #[tokio::test]
    async fn test_namespace_deserialization() {
        use super::*;
        use tower::ServiceExt;

        #[derive(Debug, Clone)]
        struct TestService;

        #[derive(Debug, Clone)]
        struct ThisState;

        impl crate::service::State for ThisState {}

        #[async_trait]
        impl NamespaceService<ThisState> for TestService {
            async fn list_namespaces(
                _prefix: Option<Prefix>,
                _query: ListNamespacesQuery,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<ListNamespacesResponse> {
                panic!("Should not be called");
            }

            /// Create a namespace, with an optional set of properties.
            /// The server might also add properties, such as `last_modified_time` etc.
            async fn create_namespace(
                _prefix: Option<Prefix>,
                _request: CreateNamespaceRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<CreateNamespaceResponse> {
                panic!("Should not be called");
            }

            /// Return all stored metadata properties for a given namespace
            async fn load_namespace_metadata(
                parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<GetNamespaceResponse> {
                assert_eq!(parameters.prefix.unwrap().into_string(), "test".to_string());

                Err(ErrorModel::builder()
                    .message(serde_json::to_string(&(parameters.namespace)).unwrap())
                    .r#type("UnsupportedOperationException".to_string())
                    .code(406)
                    .build()
                    .into())
            }

            /// Check if a namespace exists
            async fn namespace_exists(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Drop a namespace from the catalog. Namespace must be empty.
            async fn drop_namespace(
                _parameters: NamespaceParameters,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<()> {
                panic!("Should not be called");
            }

            /// Set or remove properties on a namespace
            async fn update_namespace_properties(
                _parameters: NamespaceParameters,
                _request: UpdateNamespacePropertiesRequest,
                _state: ApiContext<ThisState>,
                _headers: HeaderMap,
            ) -> Result<UpdateNamespacePropertiesResponse> {
                panic!("Should not be called");
            }
        }

        let api_context = ApiContext {
            v1_state: ThisState,
        };

        let app = namespace_router::<TestService, ThisState>();
        let router = axum::Router::new().merge(app).with_state(api_context);

        // Test 1: Single identifier
        let req = http::Request::builder()
            .uri("/test/namespaces/this-namespace?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let r = router.clone().oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = r.collect().await.unwrap().to_bytes();
        let r = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&r).unwrap();
        assert_eq!(error.error.message, "[\"this-namespace\"]");

        // Test 2: Composed identifier
        let req = http::Request::builder()
            .uri("/test/namespaces/accounting%1Ftax?pageToken&pageSize=10")
            .body(axum::body::Body::empty())
            .unwrap();

        let r = router.oneshot(req).await.unwrap();
        assert_eq!(r.status().as_u16(), 406);
        let bytes = r.collect().await.unwrap().to_bytes();
        let r = String::from_utf8(bytes.to_vec()).unwrap();
        let error = serde_json::from_str::<IcebergErrorResponse>(&r).unwrap();
        assert_eq!(error.error.message, "[\"accounting\",\"tax\"]");
    }
}
