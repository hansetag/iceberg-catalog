use crate::CONFIG;
use http::HeaderMap;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_rest_service::v1::{
    ApiContext, CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesQuery, ListNamespacesResponse, NamespaceParameters, Prefix, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};

use super::{require_warehouse_id, CatalogServer};
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, NamespaceIdentExt, State, Transaction as _,
};

pub const UNSUPPORTED_NAMESPACE_PROPERTIES: &[&str] = &["location"];
// If this is increased, we need to modify namespace creation and deletion
// to take care of the hierarchical structure.
pub const MAX_NAMESPACE_DEPTH: i32 = 1;

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    iceberg_rest_service::v1::namespace::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<ListNamespacesResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix)?;
        let ListNamespacesQuery {
            page_token: _,
            page_size: _,
            parent,
        } = &query;
        parent.as_ref().map(validate_namespace_ident).transpose()?;

        // ------------------- AUTHZ -------------------
        A::check_list_namespace(
            &headers,
            &warehouse_id,
            query.parent.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        C::list_namespaces(&warehouse_id, &query, state.v1_state.catalog).await
    }

    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<CreateNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix)?;
        let CreateNamespaceRequest {
            namespace,
            properties,
        } = &request;
        validate_namespace_ident(namespace)?;
        properties
            .as_ref()
            .map(|p| validate_namespace_properties(p.keys()))
            .transpose()?;

        if CONFIG
            .reserved_namespaces
            .contains(&namespace.as_ref()[0].to_lowercase())
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Namespace is reserved for internal use.".to_owned())
                .r#type("ReservedNamespace".to_owned())
                .build()
                .into());
        }

        // ------------------- AUTHZ -------------------
        A::check_create_namespace(
            &headers,
            &warehouse_id,
            request.namespace.parent().as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::create_namespace(&warehouse_id, request, t.transaction()).await?;
        t.commit().await?;
        Ok(r)
    }

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<GetNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        // ------------------- AUTHZ -------------------
        A::check_load_namespace_metadata(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::get_namespace_metadata(&warehouse_id, &parameters.namespace, t.transaction())
            .await?;
        t.commit().await?;
        Ok(r)
    }

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        //  ------------------- AUTHZ -------------------
        A::check_namespace_exists(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        if C::namespace_ident_to_id(&warehouse_id, &parameters.namespace, state.v1_state.catalog)
            .await?
            .is_some()
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
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        if CONFIG
            .reserved_namespaces
            .contains(&parameters.namespace.as_ref()[0].to_lowercase())
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Cannot drop namespace which is reserved for internal use.".to_owned())
                .r#type("ReservedNamespace".to_owned())
                .build()
                .into());
        }

        //  ------------------- AUTHZ -------------------
        A::check_drop_namespace(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::drop_namespace(&warehouse_id, &parameters.namespace, t.transaction()).await?;
        t.commit().await?;
        Ok(r)
    }

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<State<A, C, S>>,
        headers: HeaderMap,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;
        let UpdateNamespacePropertiesRequest { removals, updates } = &request;
        updates
            .as_ref()
            .map(|p| validate_namespace_properties(p.keys()))
            .transpose()?;
        removals
            .as_ref()
            .map(validate_namespace_properties)
            .transpose()?;

        //  ------------------- AUTHZ -------------------
        A::check_update_namespace_properties(
            &headers,
            &warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::update_namespace_properties(
            &warehouse_id,
            &parameters.namespace,
            request,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(r)
    }
}

pub(crate) fn uppercase_first_letter(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub(crate) fn validate_namespace_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if UNSUPPORTED_NAMESPACE_PROPERTIES.contains(&prop.as_str()) {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Specifying the '{prop}' property for Namespaces is not supported. '{prop}' is managed by the catalog.",
                ))
                .r#type(format!("{}PropertyNotSupported", uppercase_first_letter(prop)))
                .build()
                .into());
        } else if prop != &prop.to_lowercase() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!("The property '{prop}' is not all lowercase."))
                .r#type(format!("{}NotLowercase", uppercase_first_letter(prop)))
                .build()
                .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_namespace_ident(namespace: &NamespaceIdent) -> Result<()> {
    if namespace.len() > MAX_NAMESPACE_DEPTH as usize {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message(format!(
                "Namespace exceeds maximum depth of {MAX_NAMESPACE_DEPTH}",
            ))
            .r#type("NamespaceDepthExceeded".to_string())
            .stack(Some(vec![format!("Namespace: {:?}", namespace)]))
            .build()
            .into());
    }

    if namespace.iter().any(std::string::String::is_empty) {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Namespace parts cannot be empty".to_string())
            .r#type("EmptyNamespacePart".to_string())
            .stack(Some(vec![format!("Namespace: {:?}", namespace)]))
            .build()
            .into());
    }

    Ok(())
}
