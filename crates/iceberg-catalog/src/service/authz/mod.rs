use std::collections::HashSet;

use super::health::HealthExt;
use super::{NamespaceIdentUuid, ProjectIdent, TableIdentUuid, WarehouseIdent};
use crate::api::iceberg::v1::Result;
use crate::request_metadata::RequestMetadata;

mod implementations {
    pub(super) mod allow_all;
}

use iceberg_ext::catalog::rest::ErrorModel;
pub use implementations::allow_all::AllowAllAuthorizer;

// #[derive(Debug, Clone)]
// pub struct UserWarehouse {
//     pub project_id: Option<ProjectIdent>,
//     pub warehouse_id: Option<WarehouseIdent>,
// }

#[derive(Debug, Clone)]
pub enum ServerAction {
    /// Can create items inside the server (can create Warehouses).
    CanCreate,
    /// List projects on this server. Returned projects
    /// are filtered by the user's permissions (`CanShowInList`)
    CanListAllProjects,
}

#[derive(Debug, Clone)]
pub enum ProjectAction {
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanShowInList,
}

#[derive(Debug, Clone)]
pub enum WarehouseAction {
    CanCreateNamespace,
    /// Can delete this warehouse permanently.
    CanDelete,
    CanUpdateStorage,
    CanUpdateStorageCredential,
    CanGetMetadata,
    CanGetConfig,
    CanListNamespaces,
    /// Base permission to use any endpoint prefixed with `/api/v1/warehouse/{warehouse_id}`.
    /// This is used to pre-check endpoints for which the actual object id must be looked up.
    CanUse,
    CanShowInList,
    CanDeactivate,
    CanActivate,
    CanRename,
    CanListDeletedTabulars,
}

#[derive(Debug, Clone)]
pub enum NamespaceAction {
    CanCreateTable,
    CanCreateView,
    CanCreateNamespace,
    CanDelete,
    CanUpdateProperties,
    CanGetMetadata,
    CanListTables,
    CanListViews,
    CanListNamespaces,
}

#[derive(Debug, Clone)]
pub enum TableAction {
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanShowInList,
}

#[derive(Debug, Clone)]
pub enum ViewAction {
    CanDrop,
    CanGetMetadata,
    CanCommit,
    CanShowInList,
    CanRename,
}

pub trait TableUuid {
    fn table_uuid(&self) -> TableIdentUuid;
}

impl TableUuid for TableIdentUuid {
    fn table_uuid(&self) -> TableIdentUuid {
        *self
    }
}

#[derive(Debug, Clone)]
pub enum ListProjectsResponse {
    /// List of projects that the user is allowed to see.
    Projects(HashSet<ProjectIdent>),
    /// The user is allowed to see all projects.
    All,
}

#[async_trait::async_trait]
/// Interface to provide AuthZ functions to the catalog.
///
/// Error codes for AuthZ are directly forwarded to the user. This means:
/// * If an object does not exist, 404 may only be returned if the user is allowed to know this, otherwise FORBIDDEN must be returned.
/// * If an object exists, but the user is not allowed to know this, 401 must be returned.
/// * Some methods take Option<Ident> as an argument. If the ident is None, the object does not exist. Return 404 only if the user is allowed to know this.
pub trait Authorizer
where
    Self: Send + Sync + Clone + 'static + HealthExt,
{
    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: ServerAction,
    ) -> Result<bool>;

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: ProjectAction,
    ) -> Result<bool>;

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: WarehouseAction,
    ) -> Result<bool>;

    /// Return the namespace_id if the action is allowed, otherwise return None.
    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        action: NamespaceAction,
    ) -> Result<bool>;

    /// Return the table_id if the action is allowed, otherwise return None.
    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        table_id: TableIdentUuid,
        action: TableAction,
    ) -> Result<bool>;

    /// Return the view_id if the action is allowed, otherwise return None.
    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        view_id: TableIdentUuid,
        action: ViewAction,
    ) -> Result<bool>;

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        action: ServerAction,
    ) -> Result<()> {
        if self.is_allowed_server_action(metadata, action).await? {
            Ok(())
        } else {
            Err(ErrorModel::forbidden("Forbidden", "ServerActionForbidden", None).into())
        }
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: ProjectAction,
    ) -> Result<()> {
        if self
            .is_allowed_project_action(metadata, project_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden("Forbidden", "ProjectActionForbidden", None).into())
        }
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: WarehouseAction,
    ) -> Result<()> {
        if self
            .is_allowed_warehouse_action(metadata, warehouse_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden("Forbidden", "WarehouseActionForbidden", None).into())
        }
    }

    async fn require_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        // Outer error: Internal error that failed to fetch the namespace.
        // Ok(None): Namespace does not exist.
        // Ok(Some(namespace_id)): Namespace exists.
        namespace_id: Result<Option<NamespaceIdentUuid>>,
        action: NamespaceAction,
    ) -> Result<NamespaceIdentUuid> {
        // It is important to throw the same error if the namespace does not exist (None) or if the action is not allowed,
        // to avoid leaking information about the existence of the namespace.
        // let err = ErrorModel::forbidden("Forbidden", "NamespaceActionForbidden", None).into();
        let msg = "Forbidden";
        let typ = "NamespaceActionForbidden";

        match namespace_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(namespace_id)) => {
                if self
                    .is_allowed_namespace_action(metadata, warehouse_id, namespace_id, action)
                    .await?
                {
                    Ok(namespace_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_table_action<T: TableUuid + Send>(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        table_id: Result<Option<T>>,
        action: TableAction,
    ) -> Result<T> {
        let msg = "Forbidden";
        let typ = "TableActionForbidden";

        match table_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(table_id)) => {
                if self
                    .is_allowed_table_action(metadata, warehouse_id, table_id.table_uuid(), action)
                    .await?
                {
                    Ok(table_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_view_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        view_id: Result<Option<TableIdentUuid>>,
        action: ViewAction,
    ) -> Result<TableIdentUuid> {
        let msg = "Forbidden";
        let typ = "ViewActionForbidden";

        match view_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(view_id)) => {
                if self
                    .is_allowed_view_action(metadata, warehouse_id, view_id, action)
                    .await?
                {
                    Ok(view_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse>;
}

// /// Interface to provide Auth-related functions to the config gateway.
// /// This is separated from the AuthHandler as different functions
// /// are required while fetching the config. The config server might be
// /// external to the rest of the catalog.
// // We use the same associated type as AuthHandler to avoid requiring
// // an additional state to pass as part of the APIContext.
// // A dummy AuthHandler implementation is enough to implement this trait.
// // This still feels less clunky than using a generic state type.
// #[async_trait::async_trait]
// pub trait AuthConfigHandler
// where
//     Self: Send + Sync + Clone + 'static + HealthExt,
// {
//     /// Extract information from the user credentials. Return an error if
//     /// the user is not authenticated or if an expected extraction
//     /// of information (e.g. project or warehouse) failed.
//     /// If information is correctly not available, return None for the
//     /// respective field. In this case project / warehouse must be passed
//     /// as arguments to the config endpoint.
//     /// If a warehouse_id is returned, a project_id must also be returned.
//     ///
//     /// If a project_id or warehouse_id is returned, this function must also check the
//     /// `list_warehouse_in_project` permission for a project_id and the
//     /// `get_config_for_warehouse` permission for a warehouse_id.
//     async fn get_and_validate_user_warehouse(
//         &self,
//         metadata: &RequestMetadata,
//     ) -> Result<UserWarehouse>;

//     /// Enrich / Exchange the token that is used for all further requests
//     /// to the specified warehouse. Typically, this is used to enrich the
//     /// token with the warehouse-id, so that the get_token function can
//     /// extract it.
//     /// If this AuthNHadler does not support enriching the token, or
//     /// if no change to the original token is required, return Ok(None).
//     async fn exchange_token_for_warehouse(
//         state: A::State,
//         previous_request_metadata: &RequestMetadata,
//         project_id: &ProjectIdent,
//         warehouse_id: WarehouseIdent,
//     ) -> Result<Option<String>>;

//     // // Used for all endpoints
//     // fn get_warehouse(state: T, headers: &HeaderMap) -> Result<WarehouseIdent>;

//     /// Check if the user is allowed to list all warehouses in a project.
//     async fn is_allowed_list_warehouse_in_project(
//         state: A::State,
//         project_id: &ProjectIdent,
//         metadata: &RequestMetadata,
//     ) -> Result<bool>;

//     /// Check if the user is allowed to get the config for a warehouse.
//     async fn is_allowed_user_get_config_for_warehouse(
//         state: A::State,
//         warehouse_id: WarehouseIdent,
//         metadata: &RequestMetadata,
//     ) -> Result<bool>;
// }
