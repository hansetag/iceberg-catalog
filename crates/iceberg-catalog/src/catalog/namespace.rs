use crate::api::iceberg::v1::{
    ApiContext, CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel, GetNamespaceResponse,
    ListNamespacesQuery, ListNamespacesResponse, NamespaceParameters, Prefix, Result,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use crate::request_metadata::RequestMetadata;
use crate::service::{GetWarehouseResponse, NamespaceIdentUuid};
use crate::CONFIG;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::configs::ConfigProperty as _;
use iceberg_ext::configs::{namespace::NamespaceProperties, Location};
use std::ops::Deref;

use super::{require_warehouse_id, CatalogServer};
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, NamespaceIdentExt, State, Transaction as _,
};

pub const UNSUPPORTED_NAMESPACE_PROPERTIES: &[&str] = &[];
// If this is increased, we need to modify namespace creation and deletion
// to take care of the hierarchical structure.
pub const MAX_NAMESPACE_DEPTH: i32 = 1;

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::namespace::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
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
            &request_metadata,
            warehouse_id,
            query.parent.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        C::list_namespaces(warehouse_id, &query, state.v1_state.catalog).await
    }

    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
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
            &request_metadata,
            warehouse_id,
            request.namespace.parent().as_ref(),
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = NamespaceIdentUuid::default();

        // Set location if not specified - validate location if specified
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
        let warehouse = C::get_warehouse(warehouse_id, t.transaction()).await?;
        drop(t);
        let mut namespace_props = NamespaceProperties::try_from_maybe_props(properties.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;

        set_namespace_location_property(&mut namespace_props, &warehouse, namespace_id)?;

        // ToDo: COntinue - add pre-defined namespace_id
        // ToDo: Use Props

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::create_namespace(warehouse_id, request, t.transaction()).await?;
        t.commit().await?;
        Ok(r)
    }

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        // ------------------- AUTHZ -------------------
        A::check_load_namespace_metadata(
            &request_metadata,
            warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::get_namespace(warehouse_id, &parameters.namespace, t.transaction()).await?;
        t.commit().await?;
        Ok(GetNamespaceResponse {
            properties: r.properties,
            namespace: r.namespace,
        })
    }

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        //  ------------------- AUTHZ -------------------
        A::check_namespace_exists(
            &request_metadata,
            warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        if C::namespace_ident_to_id(warehouse_id, &parameters.namespace, state.v1_state.catalog)
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
        request_metadata: RequestMetadata,
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
            &request_metadata,
            warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::drop_namespace(warehouse_id, &parameters.namespace, t.transaction()).await?;
        t.commit().await?;
        Ok(r)
    }

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
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
            &request_metadata,
            warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        if removals
            .as_ref()
            .is_some_and(|r| r.contains(&Location::KEY.to_string()))
        {
            return Err(ErrorModel::bad_request(
                "Namespace property `location` cannot be removed.",
                "LocationCannotBeRemoved".to_string(),
                None,
            )
            .append_detail(format!("Namespace: {:?}", parameters.namespace))
            .into());
        }
        // If location is updated, validate the new location and sanitize it (make sure there is a trailing slash)
        // ToDo Christian: Validate new location & make sure its in same base location (bucket & prefix)
        if let Some(_location) = updates.as_ref().and_then(|u| u.get(Location::KEY)) {
            let mut namespace_props = NamespaceProperties::try_from_maybe_props(updates.clone())
                .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
            let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
            let warehouse = C::get_warehouse(warehouse_id, t.transaction()).await?;
            drop(t);

            set_namespace_location_property(
                &mut namespace_props,
                &warehouse,
                C::namespace_ident_to_id(
                    warehouse_id,
                    &parameters.namespace,
                    state.v1_state.catalog.clone(),
                )
                .await?
                .unwrap(),
            )?;
        }

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::update_namespace_properties(
            warehouse_id,
            &parameters.namespace,
            request,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(r)
    }
}

fn set_namespace_location_property(
    namespace_props: &mut NamespaceProperties,
    warehouse: &GetWarehouseResponse,
    namespace_id: NamespaceIdentUuid,
) -> Result<()> {
    let mut location = namespace_props.location();

    // NS locations should always have a trailing slash
    location.as_mut().map(Location::with_trailing_slash);

    // For customer specified location, we need to check if we can write to the location.
    // If no location is specified, we use our default location.
    let location = if let Some(_location) = location {
        // ToDo Christian: Validate location
        todo!()
    } else {
        warehouse
            .storage_profile
            .default_namespace_location(namespace_id)?
    };

    namespace_props.insert(&location);
    Ok(())
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
            .stack(vec![format!("Namespace: {namespace:?}")])
            .build()
            .into());
    }

    if namespace.deref().iter().any(|s| s.contains('.')) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot contain '.'".to_string(),
            "NamespacePartContainsDot".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    if namespace.iter().any(String::is_empty) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot be empty".to_string(),
            "NamespacePartEmpty".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    Ok(())
}
