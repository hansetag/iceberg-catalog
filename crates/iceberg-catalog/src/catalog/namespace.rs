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
use iceberg_ext::configs::namespace::NamespaceProperties;
use iceberg_ext::configs::{ConfigProperty as _, Location};
use std::collections::HashMap;
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
            .map(|p| validate_namespace_properties_keys(p.keys()))
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

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let warehouse = C::get_warehouse(warehouse_id, t.transaction()).await?;

        let mut namespace_props = NamespaceProperties::try_from_maybe_props(properties.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        set_namespace_location_property(&mut namespace_props, &warehouse, namespace_id)?;

        let mut request = request;
        request.properties = Some(namespace_props.into());

        let r = C::create_namespace(warehouse_id, namespace_id, request, t.transaction()).await?;
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
        let UpdateNamespacePropertiesRequest { removals, updates } = request;
        updates
            .as_ref()
            .map(|p| validate_namespace_properties_keys(p.keys()))
            .transpose()?;
        removals
            .as_ref()
            .map(validate_namespace_properties_keys)
            .transpose()?;

        namespace_location_may_not_changed(&updates, &removals)?;
        let updates = NamespaceProperties::try_from_maybe_props(updates.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        //  ------------------- AUTHZ -------------------
        A::check_update_namespace_properties(
            &request_metadata,
            warehouse_id,
            &parameters.namespace,
            state.v1_state.auth,
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let previous_properties =
            C::get_namespace(warehouse_id, &parameters.namespace, t.transaction()).await?;
        let (new_properties, r) =
            update_namespace_properties(previous_properties.properties, updates, removals);
        C::update_namespace_properties(
            warehouse_id,
            &parameters.namespace,
            new_properties,
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

pub(crate) fn validate_namespace_properties_keys<'a, I>(properties: I) -> Result<()>
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

fn set_namespace_location_property(
    namespace_props: &mut NamespaceProperties,
    warehouse: &GetWarehouseResponse,
    namespace_id: NamespaceIdentUuid,
) -> Result<()> {
    let mut location = namespace_props.get_location();

    // NS locations should always have a trailing slash
    location.as_mut().map(Location::with_trailing_slash);

    // For customer specified location, we need to check if we can write to the location.
    // If no location is specified, we use our default location.
    let location = if let Some(location) = location {
        if warehouse.storage_profile.is_allowed_location(&location) {
            location
        } else {
            return Err(ErrorModel::bad_request(
                "Namespace location is not a valid sublocation of the storage profile",
                "NamespaceLocationForbidden",
                None,
            )
            .into());
        }
    } else {
        warehouse
            .storage_profile
            .default_namespace_location(namespace_id)?
    };

    namespace_props.insert(&location);
    Ok(())
}

fn update_namespace_properties(
    previous_properties: Option<HashMap<String, String>>,
    updates: NamespaceProperties,
    removals: Option<Vec<String>>,
) -> (HashMap<String, String>, UpdateNamespacePropertiesResponse) {
    let mut properties = previous_properties.unwrap_or_default();

    let mut changes_updated = vec![];
    let mut changes_removed = vec![];
    let mut changes_missing = vec![];

    for key in removals.unwrap_or_default() {
        if properties.remove(&key).is_some() {
            changes_removed.push(key.clone());
        } else {
            changes_missing.push(key.clone());
        }
    }

    for (key, value) in updates {
        // Push to updated if the value for the key is different.
        // Also push on insert

        if properties.insert(key.clone(), value.clone()) != Some(value) {
            changes_updated.push(key);
        }
    }

    (
        properties,
        UpdateNamespacePropertiesResponse {
            updated: changes_updated,
            removed: changes_removed,
            missing: if changes_missing.is_empty() {
                None
            } else {
                Some(changes_missing)
            },
        },
    )
}

fn namespace_location_may_not_changed(
    updates: &Option<HashMap<String, String>>,
    removals: &Option<Vec<String>>,
) -> Result<()> {
    if removals
        .as_ref()
        .is_some_and(|r| r.contains(&Location::KEY.to_string()))
    {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be removed.",
            "LocationCannotBeRemoved".to_string(),
            None,
        )
        .into());
    }

    if let Some(location) = updates.as_ref().and_then(|u| u.get(Location::KEY)) {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be updated.",
            "LocationCannotBeUpdated".to_string(),
            None,
        )
        .append_detail(format!("Location: {location:?}"))
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_update_ns_properties() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
            ("key5".to_string(), "value5".to_string()),
        ]);

        let updates = NamespaceProperties::from_props_unchecked(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value12".to_string()),
        ]);

        let removals = Some(vec!["key3".to_string(), "key4".to_string()]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert_eq!(result.updated, vec!["key2".to_string()]);
        assert_eq!(result.removed, vec!["key3".to_string()]);
        assert_eq!(result.missing, Some(vec!["key4".to_string()]));
        assert_eq!(
            new_props,
            HashMap::from_iter(vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value12".to_string()),
                ("key5".to_string(), "value5".to_string()),
            ])
        );
    }

    #[test]
    fn test_update_ns_properties_empty_removal() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![]);
        let updates = NamespaceProperties::from_props_unchecked(vec![]);
        let removals = Some(vec![]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert!(result.updated.is_empty());
        assert!(result.removed.is_empty());
        assert!(result.missing.is_none());
        assert!(new_props.is_empty());
    }
}
