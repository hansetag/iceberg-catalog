use super::{NamespaceIdentUuid, ProjectIdent, TableIdentUuid, WarehouseIdent};
use crate::api::iceberg::v1::Result;
use crate::request_metadata::RequestMetadata;
use std::collections::HashSet;

pub mod implementations {
    pub(super) mod allow_all;

    #[cfg(feature = "openfga")]
    pub mod openfga;
}

use iceberg_ext::catalog::rest::ErrorModel;
pub use implementations::allow_all::AllowAllAuthorizer;

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub enum ServerAction {
    /// Can create items inside the server (can create Warehouses).
    CanCreate,
    /// List projects on this server. Returned projects
    /// are filtered by the user's permissions (`CanShowInList`)
    CanListAllProjects,
}

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub enum ProjectAction {
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanShowInList,
}

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
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

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
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

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub enum TableAction {
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanShowInList,
}

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
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
pub trait Authorizer
where
    Self: Send + Sync + Clone + 'static,
{
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse>;

    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &ServerAction,
    ) -> Result<bool>;

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: &ProjectAction,
    ) -> Result<bool>;

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &WarehouseAction,
    ) -> Result<bool>;

    /// Return the namespace_id if the action is allowed, otherwise return None.
    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        action: &NamespaceAction,
    ) -> Result<bool>;

    /// Return the table_id if the action is allowed, otherwise return None.
    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        table_id: TableIdentUuid,
        action: &TableAction,
    ) -> Result<bool>;

    /// Return the view_id if the action is allowed, otherwise return None.
    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        view_id: TableIdentUuid,
        action: &ViewAction,
    ) -> Result<bool>;

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &ServerAction,
    ) -> Result<()> {
        if self.is_allowed_server_action(metadata, action).await? {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on server"),
                "ServerActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: &ProjectAction,
    ) -> Result<()> {
        if self
            .is_allowed_project_action(metadata, project_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on project {project_id}"),
                "ProjectActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &WarehouseAction,
    ) -> Result<()> {
        if self
            .is_allowed_warehouse_action(metadata, warehouse_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on warehouse {warehouse_id}"),
                "WarehouseActionForbidden",
                None,
            )
            .into())
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
        action: &NamespaceAction,
    ) -> Result<NamespaceIdentUuid> {
        // It is important to throw the same error if the namespace does not exist (None) or if the action is not allowed,
        // to avoid leaking information about the existence of the namespace.
        let msg = format!("Namespace action {action} forbidden");
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
        action: &TableAction,
    ) -> Result<T> {
        let msg = format!("Table action {action} forbidden");
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
        action: &ViewAction,
    ) -> Result<TableIdentUuid> {
        let msg = format!("View action {action} forbidden");
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_action() {
        assert_eq!(
            NamespaceAction::CanCreateTable.to_string(),
            "can_create_table"
        );
    }
}
