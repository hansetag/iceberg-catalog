use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ErrorModel, ListTablesResponse,
    LoadViewResult, NamespaceParameters, PaginationQuery, Prefix, RenameTableRequest, Result,
    TableIdent, ViewParameters,
};
use crate::catalog::io::write_metadata_file;
use crate::implementations::postgres::tabular::view::create_view;
use crate::implementations::postgres::tabular::TabularIdentUuid;
use crate::request_metadata::RequestMetadata;
use http::StatusCode;
use std::vec;

use super::tables::{
    maybe_body_to_json, require_no_location_specified, validate_lowercase_property,
    validate_table_or_view_ident,
};
use super::{namespace::validate_namespace_ident, require_warehouse_id, CatalogServer};
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, GetWarehouseResponse, State, TableIdentUuid,
    Transaction,
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
        data_access: DataAccess,
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
            state.v1_state.auth.clone(),
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

        let table_id: TabularIdentUuid = TabularIdentUuid::View(uuid::Uuid::now_v7());

        let view_location = storage_profile.tabular_location(&namespace_id, &table_id);
        let mut request = request;
        let metadata_location = storage_profile.metadata_location(&view_location, &table_id);
        request.location.as_mut().map(|loc| *loc = view_location);
        let request = request;

        let metadata = C::create_view(
            &warehouse_id,
            &namespace_id,
            &table_id,
            &table,
            request,
            &metadata_location,
            transaction.transaction(),
        )
        .await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = &storage_secret_id {
            Some(
                S::get_secret_by_id(secret_id, state.v1_state.secrets)
                    .await?
                    .secret,
            )
        } else {
            None
        };

        let file_io = storage_profile.file_io(storage_secret.as_ref())?;
        write_metadata_file(metadata_location.as_str(), &metadata, &file_io).await?;
        tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

        // Generate the storage profile. This requires the storage secret
        // because the table config might contain vended-credentials based
        // on the `data_access` parameter.
        // ToDo: There is a small inefficiency here: If storage credentials
        // are not required because of i.e. remote-signing and if this
        // is a stage-create, we still fetch the secret.
        let config = storage_profile
            .generate_table_config(
                &warehouse_id,
                &namespace_id,
                &TableIdentUuid::from(*table_id),
                &data_access,
                storage_secret.as_ref(),
            )
            .await?;

        return Ok(LoadViewResult {
            metadata_location,
            metadata,
            config: Some(config),
        });
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
