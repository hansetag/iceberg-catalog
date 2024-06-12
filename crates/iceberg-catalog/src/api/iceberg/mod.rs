pub mod types;

pub mod v1 {
    use crate::api::ThreadSafe;
    use axum::Router;
    use std::sync::Arc;
    pub mod config;
    pub mod metrics;
    pub mod namespace;
    pub mod oauth;
    pub mod s3_signer;
    pub mod tables;
    pub mod views;

    pub use iceberg_ext::catalog::{NamespaceIdent, TableIdent};

    pub use self::namespace::{ListNamespacesQuery, NamespaceParameters, PaginationQuery};
    pub use self::tables::{DataAccess, TableParameters};
    pub use self::views::ViewParameters;
    pub use crate::api::iceberg::types::*;

    pub use crate::api::{
        ApiContext, CatalogConfig, CommitTableRequest, CommitTableResponse,
        CommitTransactionRequest, CommitViewRequest, CreateNamespaceRequest,
        CreateNamespaceResponse, CreateTableRequest, CreateViewRequest, ErrorModel,
        GetNamespaceResponse, IcebergErrorResponse, ListNamespacesResponse, ListTablesResponse,
        LoadTableResult, LoadViewResult, OAuthTokenRequest, OAuthTokenResponse,
        RegisterTableRequest, RenameTableRequest, RequestMetadata, Result,
        UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
    };
    use crate::service::token_verification::Verifier;

    pub fn new_v1_full_router<
        C: config::Service<S>,
        T: namespace::Service<S> + tables::Service<S> + metrics::Service<S> + s3_signer::Service<S>,
        S: ThreadSafe,
    >(
        verifier: Option<Arc<Verifier>>,
    ) -> Router<ApiContext<S>> {
        Router::new()
            .merge(config::router::<C, S>())
            .merge(namespace::router::<T, S>())
            .merge(tables::router::<T, S>())
            .merge(s3_signer::router::<T, S>())
            .merge(metrics::router::<T, S>())
            .layer(axum::middleware::from_fn_with_state(
                verifier,
                crate::service::token_verification::auth_middleware_fn,
            ))
    }

    pub fn new_v1_config_router<C: config::Service<S>, S: ThreadSafe>() -> Router<ApiContext<S>> {
        config::router::<C, S>()
    }
}
