use async_trait::async_trait;

use crate::api::iceberg::v1::Result;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{
    Authorizer, ListProjectsResponse, NamespaceAction, ProjectAction, ServerAction, TableAction,
    ViewAction, WarehouseAction,
};
use crate::service::health::{Health, HealthExt};
use crate::service::{NamespaceIdentUuid, ProjectIdent, TableIdentUuid, WarehouseIdent};

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
}
