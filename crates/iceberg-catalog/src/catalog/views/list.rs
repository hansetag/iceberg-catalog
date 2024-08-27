use crate::api::iceberg::v1::{NamespaceParameters, PaginationQuery};
use crate::api::ApiContext;
use crate::api::Result;
use crate::catalog::namespace::validate_namespace_ident;
use crate::catalog::require_warehouse_id;
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::{Catalog, SecretStore, State};
use iceberg_ext::catalog::rest::ListTablesResponse;

pub(crate) async fn list_views<C: Catalog, A: AuthZHandler, S: SecretStore>(
    parameters: NamespaceParameters,
    pagination_query: PaginationQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListTablesResponse> {
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters;
    let warehouse_id = require_warehouse_id(prefix)?;
    validate_namespace_ident(&namespace)?;

    // ------------------- AUTHZ -------------------
    A::check_list_views(
        &request_metadata,
        warehouse_id,
        &namespace,
        state.v1_state.auth,
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------

    let views = C::list_views(
        warehouse_id,
        &namespace,
        false,
        state.v1_state.catalog.clone(),
        pagination_query,
    )
    .await?;

    Ok(ListTablesResponse {
        next_page_token: None,
        identifiers: views.into_iter().map(|t| t.1).collect(),
    })
}
