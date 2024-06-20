use crate::api::iceberg::v1::tables::Service;
use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, TableParameters, ViewParameters,
};
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use std::vec;

use super::tables::{
    maybe_body_to_json, require_no_location_specified, validate_lowercase_property,
    validate_table_or_view_ident,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, GetWarehouseResponse, State, Transaction,
};

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
        A::check_list_tables(
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
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;

        // TODO: this correct?
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
        A::check_create_table(
            &request_metadata,
            &warehouse_id,
            &namespace,
            state.v1_state.auth,
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------

        let namespace_id =
            C::namespace_ident_to_id(&warehouse_id, &namespace, state.v1_state.catalog.clone())
                .await?
                .ok_or(
                    ErrorModel::builder()
                        .code(StatusCode::NOT_FOUND.into())
                        .message("Namespace does not exist".to_string())
                        .r#type("NamespaceNotFound".to_string())
                        .build(),
                )?;

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let GetWarehouseResponse {
            id: _,
            name: _,
            project_id: _,
            storage_profile,
            storage_secret_id,
            status,
        } = C::get_warehouse(&warehouse_id, transaction.transaction()).await?;
        crate::catalog::tables::require_active_warehouse(status)?;

        if C::table_ident_to_id(&warehouse_id, &table, true, transaction)
            .await?
            .is_some()
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(format!(
                    "Table '{}' already exists in Namespace '{}'",
                    table.name,
                    table.namespace.encode_in_url()
                ))
                .r#type("TableAlreadyExists".to_string())
                .build()
                .into());
        }

        if Self::view_exists(
            ViewParameters {
                prefix,
                view: table.clone(),
            },
            state.clone(),
            request_metadata.clone(),
        ) {
            return Err(ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(format!(
                    "View '{}' already exists in Namespace '{}'",
                    table.name,
                    table.namespace.encode_in_url()
                ))
                .r#type("ViewAlreadyExists".to_string())
                .build()
                .into());
        };

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
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view: _ } = parameters;
        require_warehouse_id(prefix)?;

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
        _request: CommitViewRequest,
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        // ------------------- VALIDATIONS -------------------
        require_warehouse_id(parameters.prefix.clone())?;

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
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;

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
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let ViewParameters { prefix, view } = parameters;
        require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&view)?;

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
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let _warehouse_id = require_warehouse_id(prefix.clone())?;
        let _body = maybe_body_to_json(&request);
        let RenameTableRequest {
            source,
            destination,
        } = request;
        validate_table_or_view_ident(&source)?;
        validate_table_or_view_ident(&destination)?;

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
    for prop in properties {
        if prop != &prop.to_lowercase() {
            validate_lowercase_property(prop)?;
        }
    }
    Ok(())
}
