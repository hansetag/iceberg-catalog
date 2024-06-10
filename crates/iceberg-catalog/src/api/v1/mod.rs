pub mod config;
pub mod metrics;
pub mod namespace;
pub mod oauth;
pub mod s3_signer;
pub mod tables;
pub mod views;

pub use iceberg_ext::catalog::{NamespaceIdent, TableIdent};
use std::marker::PhantomData;

pub mod spec {
    pub use iceberg_ext::spec::TableMetadata;
}

pub use crate::api::{
    ApiContext, CatalogConfig, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CommitViewRequest, CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest,
    CreateViewRequest, ErrorModel, GetNamespaceResponse, IcebergErrorResponse,
    ListNamespacesResponse, ListTablesResponse, LoadTableResult, LoadViewResult, OAuthTokenRequest,
    OAuthTokenResponse, RegisterTableRequest, RenameTableRequest, RequestMetadata, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};

pub use crate::api::types::*;
use axum::async_trait;
use axum::extract::{Form, Path, Query};
pub use axum::routing::get;
use http::HeaderMap;

pub use self::namespace::{ListNamespacesQuery, NamespaceParameters, PaginationQuery};
pub use self::tables::{DataAccess, TableParameters};
pub use self::views::ViewParameters;

use super::{post, AuthZHandler, AxumState, Catalog, Json, Router, SecretStore};
use axum::Extension;
pub mod warehouse;
use crate::service::State;
use warehouse::WarehouseService;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ApiServer<C: Catalog, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

impl<C: Catalog, A: AuthZHandler, S: SecretStore> ApiServer<C, A, S> {
    pub fn v1_router() -> Router<ApiContext<State<A, C, S>>> {
        Router::new()
            // List Namespaces
            .route(
                "/warehouse",
                // List Namespaces
                post(
                    |AxumState(api_context): AxumState<ApiContext<State<A, C, S>>>,
                     Extension(metadata): Extension<RequestMetadata>,
                     Json(request): Json<warehouse::CreateWarehouseRequest>| {
                        Self::create_warehouse(request, api_context, metadata)
                    },
                ),
            )
    }
}
