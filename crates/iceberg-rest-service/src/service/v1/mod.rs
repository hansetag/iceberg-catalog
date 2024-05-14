pub mod config;
pub mod metrics;
pub mod namespace;
pub mod oauth;
pub mod s3_signer;
pub mod tables;
pub mod views;

pub use iceberg_ext::catalog::{NamespaceIdent, TableIdent};

pub mod spec {
    pub use iceberg_ext::spec::TableMetadata;
}

pub use crate::service::{
    ApiContext, CatalogConfig, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CommitViewRequest, CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest,
    CreateViewRequest, ErrorModel, GetNamespaceResponse, IcebergErrorResponse,
    ListNamespacesResponse, ListTablesResponse, LoadTableResult, LoadViewResult, OAuthTokenRequest,
    OAuthTokenResponse, RegisterTableRequest, RenameTableRequest, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};

pub use crate::types::*;
use axum::async_trait;
use axum::extract::{Form, Json, Path, Query, State};
pub(crate) use axum::{
    routing::{get, post},
    Router,
};
use http::HeaderMap;

pub use self::namespace::{ListNamespacesQuery, NamespaceParameters, PaginationQuery};
pub use self::tables::{DataAccess, TableParameters};
pub use self::views::ViewParameters;
