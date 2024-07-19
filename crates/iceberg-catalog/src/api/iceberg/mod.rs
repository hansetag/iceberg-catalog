pub mod types;

pub mod v1 {
    use crate::api::ThreadSafe;
    use axum::Router;

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
        RegisterTableRequest, RenameTableRequest, Result, UpdateNamespacePropertiesRequest,
        UpdateNamespacePropertiesResponse,
    };
    pub use crate::request_metadata::RequestMetadata;

    pub const MAX_PAGE_SIZE: i64 = 100i64;

    pub fn new_v1_full_router<
        C: config::Service<S>,
        #[cfg(feature = "s3-signer")] T: namespace::Service<S>
            + tables::Service<S>
            + metrics::Service<S>
            + s3_signer::Service<S>
            + views::Service<S>,
        #[cfg(not(feature = "s3-signer"))] T: namespace::Service<S> + tables::Service<S> + metrics::Service<S> + views::Service<S>,
        S: ThreadSafe,
    >() -> Router<ApiContext<S>> {
        let router = Router::new()
            .merge(config::router::<C, S>())
            .merge(namespace::router::<T, S>())
            .merge(tables::router::<T, S>())
            .merge(views::router::<T, S>())
            .merge(metrics::router::<T, S>());

        #[cfg(feature = "s3-signer")]
        let router = router.merge(s3_signer::router::<T, S>());

        router
    }

    pub fn new_v1_config_router<C: config::Service<S>, S: ThreadSafe>() -> Router<ApiContext<S>> {
        config::router::<C, S>()
    }
}
