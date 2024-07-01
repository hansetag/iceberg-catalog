use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, ViewParameters,
};
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::ViewUpdate;

use super::tables::{
    require_no_location_specified, validate_table_or_view_ident, validate_table_properties,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State};

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    crate::api::iceberg::v1::views::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all view identifiers underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        _query: PaginationQuery,
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
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------

        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: vec![],
        })
    }

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let view = TableIdent::new(namespace.clone(), request.name.clone());

        validate_table_or_view_ident(&view)?;
        require_no_location_specified(&request.location)?;

        if request.location.is_some() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Specifying a View `location` is not supported. Location is managed by the Catalog.".to_string())
                .r#type("LocationNotSupported".to_string())
                .build()
                .into());
        }

        validate_view_properties(request.properties.keys())?;

        if request.view_version.representations().is_empty() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("View must have at least one query.".to_string())
                .r#type("EmptyView".to_string())
                .build()
                .into());
        }

        // ------------------- AUTHZ -------------------
        A::check_create_view(
            &request_metadata,
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_IMPLEMENTED.into())
            .message("Creating views is not supported".to_string())
            .r#type("CreateViewNotSupported".to_string())
            .build()
            .into());
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        // ToDo: Remove workaround when hierarchical namespaces are supported.
        // It is important for now to throw a 404 if a table cannot be found,
        // because spark might check if `table`.`branch` exists, which should return 404.
        // Only then will it treat it as a branch.
        // 404 is returned by the logic in the remainder of this function. Here, we only
        // need to make sure that we don't fail prematurely on longer namespaces.
        match validate_table_or_view_ident(&view) {
            Ok(()) => {}
            Err(e) => {
                if e.error.r#type != *"NamespaceDepthExceeded" {
                    return Err(e);
                }
            }
        }

        // ------------------- AUTHZ -------------------
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        A::check_load_view(
            &request_metadata,
            &warehouse_id,
            Some(&view.namespace),
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("LoadViewNotSupported".to_string())
            .build()
            .into());
    }

    /// Commit updates to a view
    async fn commit_view(
        parameters: ViewParameters,
        mut request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;

        if let Some(identifier) = &request.identifier {
            if identifier != &parameters.view {
                // When querying a branch, spark sends something like:
                // namespace: (<my>, <namespace>, <table_name>)
                // table_name: branch_<branch_name>
                let ns_parts = parameters.view.namespace.clone().inner();
                let table_name_candidate = if ns_parts.len() >= 2 {
                    NamespaceIdent::from_vec(
                        ns_parts.iter().take(ns_parts.len() - 1).cloned().collect(),
                    )
                    .ok()
                    .map(|n| TableIdent::new(n, ns_parts.last().cloned().unwrap_or_default()))
                } else {
                    None
                };

                if table_name_candidate != Some(identifier.clone()) {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message(
                            "Table identifier in path does not match the one in the request body"
                                .to_string(),
                        )
                        .r#type("TableIdentifierMismatch".to_string())
                        .build()
                        .into());
                }
            }
        }

        if request.identifier.is_none() {
            request.identifier = Some(parameters.view.clone());
        }
        if let Some(ref mut identifier) = request.identifier {
            validate_table_or_view_ident(identifier)?;
        }

        let CommitViewRequest {
            identifier,
            requirements: _,
            updates,
        } = &request;

        validate_view_updates_updates(updates)?;

        identifier
            .as_ref()
            .map(validate_table_or_view_ident)
            .transpose()?;

        if let Some(identifier) = identifier {
            if identifier != &parameters.view {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message(
                        "View identifier in path does not match the one in the request body"
                            .to_string(),
                    )
                    .r#type("ViewIdentifierMismatch".to_string())
                    .build()
                    .into());
            }
        }

        // ------------------- AUTHZ -------------------
        let table_id = C::view_ident_to_id(
            &warehouse_id,
            &parameters.view,
            state.v1_state.catalog.clone(),
        )
        .await
        // We can't fail before AuthZ.
        .ok()
        .flatten();

        A::check_commit_view(
            &request_metadata,
            &warehouse_id,
            table_id.as_ref(),
            Some(&parameters.view.namespace),
            state.v1_state.auth,
        )
        .await?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("CommitViewNotSupported".to_string())
            .build()
            .into());
    }

    /// Drop a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;

        // ------------------- AUTHZ -------------------
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        A::check_drop_view(
            &request_metadata,
            &warehouse_id,
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("DropViewNotSupported".to_string())
            .build()
            .into());
    }

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;
        // TODO: authz
        let view_id = C::view_ident_to_id(&warehouse_id, &view, state.v1_state.catalog.clone())
            .await
            .ok()
            .flatten();

        A::check_view_exists(
            &request_metadata,
            &warehouse_id,
            Some(&view.namespace),
            view_id.as_ref(),
            state.v1_state.auth,
        )
        .await?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("ViewExistsNotSupported".to_string())
            .build()
            .into());
    }

    /// Rename a view
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;

        let RenameTableRequest {
            source,
            destination,
        } = request;
        validate_table_or_view_ident(&source)?;
        validate_table_or_view_ident(&destination)?;

        // ------------------- AUTHZ -------------------
        let source_id = C::view_ident_to_id(&warehouse_id, &source, state.v1_state.catalog.clone())
            .await
            // We can't fail before AuthZ.
            .ok()
            .flatten();

        // We need to be allowed to delete the old table and create the new one
        let rename_check = A::check_rename_view(
            &request_metadata,
            &warehouse_id,
            source_id.as_ref(),
            state.v1_state.auth.clone(),
        );
        let create_check = A::check_create_view(
            &request_metadata,
            &warehouse_id,
            &destination.namespace,
            state.v1_state.auth,
        );
        futures::try_join!(rename_check, create_check)?;

        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Views are not implemented".to_string())
            .r#type("RenameViewNotSupported".to_string())
            .build()
            .into());
    }
}

fn validate_view_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    validate_table_properties(properties)
}

fn validate_view_updates_updates(updates: &Vec<ViewUpdate>) -> Result<()> {
    for update in updates {
        match update {
            ViewUpdate::SetProperties(updates) => {
                validate_view_properties(updates.updates.keys())?;
            }
            ViewUpdate::RemoveProperties(removals) => {
                validate_view_properties(removals.removals.iter())?;
            }
            _ => {}
        }
    }
    Ok(())
}
