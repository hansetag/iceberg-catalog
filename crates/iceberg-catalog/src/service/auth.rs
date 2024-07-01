use std::collections::HashSet;

use super::{ProjectIdent, TableIdentUuid, WarehouseIdent};
use crate::api::iceberg::v1::{NamespaceIdent, Result};
use crate::request_metadata::RequestMetadata;

#[derive(Debug, Clone)]
pub struct UserWarehouse {
    pub project_id: Option<ProjectIdent>,
    pub warehouse_id: Option<WarehouseIdent>,
}

#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait AuthZHandler
where
    Self: Sized + Send + Sync + Clone + 'static,
{
    type State: Clone + Send + Sync + 'static;

    async fn check_list_namespace(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        parent: Option<&NamespaceIdent>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_create_namespace(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        parent: Option<&NamespaceIdent>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_load_namespace_metadata(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    /// Check if the user is allowed to check if a namespace exists,
    /// not check if the namespace exists.
    async fn check_namespace_exists(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_drop_namespace(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_update_namespace_properties(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_create_table(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_list_tables(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    /// Check if the user is allowed to load a table.
    ///
    /// `table` is an optional argument because we might not be able
    /// to obtain a table-id from the table_name a user specifies.
    /// In most cases, unless the user has high permissions on a
    /// namespace, you would probably want to return 401.
    ///
    /// Arguments:
    /// - `warehouse_id`: The warehouse the table is in.
    /// - `namespace`: The namespace the table is in. (Direct parent)
    ///
    async fn check_load_table(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: Option<&NamespaceIdent>,
        table: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;

    /// This should check if the user is allowed to rename the table.
    /// For rename to work, also "check_create_table" must pass
    /// for the destination namespace.
    async fn check_rename_table(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        source: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_table_exists(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: Option<&NamespaceIdent>,
        table: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_drop_table(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        table: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_commit_table(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        table: Option<&TableIdentUuid>,
        namespace: Option<&NamespaceIdent>,
        state: Self::State,
    ) -> Result<()>;

    // ---------------- Management API ----------------
    async fn check_create_warehouse(
        metadata: &RequestMetadata,
        project_id: &ProjectIdent,
        state: Self::State,
    ) -> Result<()>;

    // Return an error if the user is not authorized.
    // Return Ok(None) if the user is authorized to list all existing projects.
    // Return Ok(Some(projects)) if the user is authorized to list only the
    // specified projects.
    async fn check_list_projects(
        metadata: &RequestMetadata,
        state: Self::State,
    ) -> Result<Option<HashSet<ProjectIdent>>>;

    async fn check_list_warehouse_in_project(
        metadata: &RequestMetadata,
        project_id: &ProjectIdent,
        state: Self::State,
    ) -> Result<Option<HashSet<WarehouseIdent>>>;

    async fn check_delete_warehouse(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_get_warehouse(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_rename_warehouse(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_deactivate_warehouse(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_activate_warehouse(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_update_storage(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_create_view(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
        state: Self::State,
    ) -> Result<()>;

    async fn check_drop_view(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        table: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;

    async fn check_load_view(
        metadata: &RequestMetadata,
        warehouse_id: &WarehouseIdent,
        namespace: Option<&NamespaceIdent>,
        view: Option<&TableIdentUuid>,
        state: Self::State,
    ) -> Result<()>;
    async fn check_commit_view(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Option<&NamespaceIdent>,
        _: Self::State,
    ) -> Result<()>;
    async fn check_rename_view(
        _: &RequestMetadata,
        _: &WarehouseIdent,
        _: Option<&TableIdentUuid>,
        _: Self::State,
    ) -> Result<()>;
    async fn check_list_views(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: &NamespaceIdent,
        _state: Self::State,
    ) -> Result<()>;
    async fn check_view_exists(
        _metadata: &RequestMetadata,
        _warehouse_id: &WarehouseIdent,
        _namespace: Option<&NamespaceIdent>,
        _view: Option<&TableIdentUuid>,
        _state: Self::State,
    ) -> Result<()>;
}

/// Interface to provide Auth-related functions to the config gateway.
/// This is separated from the AuthHandler as different functions
/// are required while fetching the config. The config server might be
/// external to the rest of the catalog.
// We use the same associated type as AuthHandler to avoid requiring
// an additional state to pass as part of the APIContext.
// A dummy AuthHandler implementation is enough to implement this trait.
// This still feels less clunky than using a generic state type.
#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait AuthConfigHandler<A: AuthZHandler>
where
    Self: Sized + Send + Sync + Clone + 'static,
{
    /// Extract information from the user credentials. Return an error if
    /// the user is not authenticated or if an expected extraction
    /// of information (e.g. project or warehouse) failed.
    /// If information is correctly not available, return None for the
    /// respective field. In this case project / warehouse must be passed
    /// as arguments to the config endpoint.
    /// If a warehouse_id is returned, a project_id must also be returned.
    ///
    /// If a project_id or warehouse_id is returned, this function must also check the
    /// `list_warehouse_in_project` permission for a project_id and the
    /// `get_config_for_warehouse` permission for a warehouse_id.
    async fn get_and_validate_user_warehouse(
        state: A::State,
        metadata: &RequestMetadata,
    ) -> Result<UserWarehouse>;

    /// Enrich / Exchange the token that is used for all further requests
    /// to the specified warehouse. Typically, this is used to enrich the
    /// token with the warehouse-id, so that the get_token function can
    /// extract it.
    /// If this AuthNHadler does not support enriching the token, or
    /// if no change to the original token is required, return Ok(None).
    async fn exchange_token_for_warehouse(
        state: A::State,
        previous_request_metadata: &RequestMetadata,
        project_id: &ProjectIdent,
        warehouse_id: &WarehouseIdent,
    ) -> Result<Option<String>>;

    // // Used for all endpoints
    // fn get_warehouse(state: T, headers: &HeaderMap) -> Result<WarehouseIdent>;

    /// Check if the user is allowed to list all warehouses in a project.
    async fn check_list_warehouse_in_project(
        state: A::State,
        project_id: &ProjectIdent,
        metadata: &RequestMetadata,
    ) -> Result<()>;

    /// Check if the user is allowed to get the config for a warehouse.
    async fn check_user_get_config_for_warehouse(
        state: A::State,
        warehouse_id: &WarehouseIdent,
        metadata: &RequestMetadata,
    ) -> Result<()>;
}
