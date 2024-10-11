use async_trait::async_trait;

use crate::api::iceberg::v1::Result;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{
    Authorizer, ListProjectsResponse, NamespaceAction, NamespaceParent, ProjectAction, RoleAction,
    ServerAction, TableAction, UserAction, ViewAction, WarehouseAction,
};
use crate::service::health::{Health, HealthExt};
use crate::service::{
    NamespaceIdentUuid, ProjectIdent, RoleId, TableIdentUuid, UserId, ViewIdentUuid, WarehouseIdent,
};

#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl HealthExt for AllowAllAuthorizer {
    async fn health(&self) -> Vec<Health> {
        vec![]
    }
    async fn update_health(&self) {
        // Do nothing
    }
}

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn list_projects(&self, _metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
        Ok(ListProjectsResponse::All)
    }

    async fn can_search_users(&self, _metadata: &RequestMetadata) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_user_action(
        &self,
        _metadata: &RequestMetadata,
        _user_id: &UserId,
        _action: &UserAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_role_action(
        &self,
        _metadata: &RequestMetadata,
        _role_id: RoleId,
        _action: &RoleAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_server_action(
        &self,
        _metadata: &RequestMetadata,
        _action: &ServerAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_project_action(
        &self,
        _metadata: &RequestMetadata,
        _project_id: ProjectIdent,
        _action: &ProjectAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_warehouse_action(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        _action: &WarehouseAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_namespace_action(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        _namespace_id: NamespaceIdentUuid,
        _action: &NamespaceAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_table_action(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        _table_id: TableIdentUuid,
        _action: &TableAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn is_allowed_view_action(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        _view_id: TableIdentUuid,
        _action: &ViewAction,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()> {
        Ok(())
    }

    async fn create_role(
        &self,
        _metadata: &RequestMetadata,
        _role_id: RoleId,
        _parent_project_id: ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_role(&self, _metadata: &RequestMetadata, _role_id: RoleId) -> Result<()> {
        Ok(())
    }

    async fn create_project(
        &self,
        _metadata: &RequestMetadata,
        _project_id: ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        _project_id: ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_warehouse(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
        _parent_project_id: ProjectIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        _warehouse_id: WarehouseIdent,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_namespace(
        &self,
        _metadata: &RequestMetadata,
        _namespace_id: NamespaceIdentUuid,
        _parent: NamespaceParent,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        _namespace_id: NamespaceIdentUuid,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_table(
        &self,
        _metadata: &RequestMetadata,
        _table_id: TableIdentUuid,
        _parent: NamespaceIdentUuid,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_table(
        &self,
        _metadata: &RequestMetadata,
        _table_id: TableIdentUuid,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        _view_id: ViewIdentUuid,
        _parent: NamespaceIdentUuid,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_view(
        &self,
        _metadata: &RequestMetadata,
        _view_id: ViewIdentUuid,
    ) -> Result<()> {
        Ok(())
    }
}
