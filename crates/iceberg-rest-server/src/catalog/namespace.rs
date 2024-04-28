use http::HeaderMap;
use http::StatusCode;
use iceberg_rest_service::v1::{
    ApiContext, CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesQuery, ListNamespacesResponse, NamespaceParameters, Prefix, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};

use super::CatalogServer;
use crate::service::CatalogState;
use crate::service::{auth::AuthHandler, AuthState, Catalog, SecretsState, State};
use crate::WarehouseIdent;

fn to_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}

#[async_trait::async_trait]
impl<C: Catalog<CS>, CS: CatalogState, A: AuthHandler<AS>, AS: AuthState, S: SecretsState>
    iceberg_rest_service::v1::namespace::Service<State<AS, CS, S>> for CatalogServer<C, CS, A, AS>
{
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<ListNamespacesResponse> {
        let warehouse_id = to_warehouse_id(prefix)?;
        A::check_list_namespace(
            &headers,
            &warehouse_id,
            &query.parent,
            state.v1_state.auth_state,
        )
        .await?;
        C::list_namespaces(&warehouse_id, &query, state.v1_state.catalog_state).await
    }

    /// Create a namespace, with an optional set of properties.
    /// The server might also add properties, such as `last_modified_time` etc.
    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<CreateNamespaceResponse> {
        let warehouse_id = to_warehouse_id(prefix)?;
        A::check_create_namespace(
            &headers,
            &warehouse_id,
            &request.namespace,
            state.v1_state.auth_state,
        )
        .await?;
        C::create_namespace(&warehouse_id, request, state.v1_state.catalog_state).await
    }

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<GetNamespaceResponse> {
        let warehouse_id = to_warehouse_id(parameters.prefix)?;
        A::check_load_namespace_metadata(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth_state,
        )
        .await?;
        C::get_namespace_metadata(
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.catalog_state,
        )
        .await
    }

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<()> {
        let warehouse_id = to_warehouse_id(parameters.prefix)?;
        A::check_namespace_exists(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth_state,
        )
        .await?;
        if C::namespace_exists(
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.catalog_state,
        )
        .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("Namespace {:#?} not found.", parameters.namespace))
                .r#type("NoSuchNamespaceException".to_string())
                .build()
                .into())
        }
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<()> {
        let warehouse_id = to_warehouse_id(parameters.prefix)?;
        A::check_drop_namespace(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth_state,
        )
        .await?;
        C::drop_namespace(
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.catalog_state,
        )
        .await
    }

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<State<AS, CS, S>>,
        headers: HeaderMap,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        let warehouse_id = to_warehouse_id(parameters.prefix)?;
        A::check_update_namespace_properties(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth_state,
        )
        .await?;
        C::update_namespace_properties(
            &warehouse_id,
            &parameters.namespace,
            request,
            state.v1_state.catalog_state,
        )
        .await
    }
}
