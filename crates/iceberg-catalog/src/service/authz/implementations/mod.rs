use crate::service::authz::{NamespaceParent, RoleAction, UserAction};
use crate::service::{RoleId, UserId, ViewIdentUuid};
use crate::{
    request_metadata::RequestMetadata,
    service::{
        authz::{
            ErrorModel, ListProjectsResponse, NamespaceAction, ProjectAction, Result, ServerAction,
            TableAction, ViewAction, WarehouseAction,
        },
        health::{Health, HealthExt},
        NamespaceIdentUuid, TableIdentUuid,
    },
    AuthZBackend, ProjectIdent, WarehouseIdent, CONFIG,
};

pub(super) mod allow_all;

pub mod openfga;

/// Get the default authorizer from the configuration
///
/// # Errors
/// Default model is not obtainable, i.e. if the model is not found in openfga
// Return error model here to convert it into anyhow in bin. IcebergErrorResponse does
// not implement StdError
pub async fn get_default_authorizer_from_config() -> std::result::Result<Authorizers, ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(allow_all::AllowAllAuthorizer.into()),
        AuthZBackend::OpenFGA => Ok(openfga::new_authorizer_from_config().await?),
    }
}

/// Migrate the default authorizer to a new model version.
///
/// # Errors
/// Migration fails - for details check the documentation of the configured
/// Authorizer implementation
pub async fn migrate_default_authorizer() -> std::result::Result<(), ErrorModel> {
    match &CONFIG.authz_backend {
        AuthZBackend::AllowAll => Ok(()),
        AuthZBackend::OpenFGA => {
            let client = openfga::new_client_from_config().await?;
            let store_name = None;
            match client {
                openfga::Clients::Unauthenticated(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
                openfga::Clients::Bearer(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
                openfga::Clients::ClientCredentials(mut client) => {
                    Ok(openfga::migrate(&mut client, store_name).await?)
                }
            }
        }
    }
}

#[derive(Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub enum FgaType {
    User,
    Role,
    Server,
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    ModelVersion,
    AuthModelId,
}

#[derive(Debug, Clone)]
pub enum Authorizers {
    AllowAll(allow_all::AllowAllAuthorizer),
    OpenFGAUnauthorized(openfga::UnauthenticatedOpenFGAAuthorizer),
    OpenFGABearer(openfga::BearerOpenFGAAuthorizer),
    OpenFGAClientCreds(openfga::ClientCredentialsOpenFGAAuthorizer),
}

impl From<allow_all::AllowAllAuthorizer> for Authorizers {
    fn from(authorizer: allow_all::AllowAllAuthorizer) -> Self {
        Self::AllowAll(authorizer)
    }
}

impl From<openfga::UnauthenticatedOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::UnauthenticatedOpenFGAAuthorizer) -> Self {
        Self::OpenFGAUnauthorized(authorizer)
    }
}

impl From<openfga::BearerOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::BearerOpenFGAAuthorizer) -> Self {
        Self::OpenFGABearer(authorizer)
    }
}

impl From<openfga::ClientCredentialsOpenFGAAuthorizer> for Authorizers {
    fn from(authorizer: openfga::ClientCredentialsOpenFGAAuthorizer) -> Self {
        Self::OpenFGAClientCreds(authorizer)
    }
}

#[async_trait::async_trait]
impl super::Authorizer for Authorizers {
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse> {
        match self {
            Self::AllowAll(authorizer) => authorizer.list_projects(metadata).await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.list_projects(metadata).await,
            Self::OpenFGABearer(authorizer) => authorizer.list_projects(metadata).await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.list_projects(metadata).await,
        }
    }

    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => authorizer.can_search_users(metadata).await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.can_search_users(metadata).await,
            Self::OpenFGABearer(authorizer) => authorizer.can_search_users(metadata).await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.can_search_users(metadata).await,
        }
    }

    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: &RoleAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_role_action(metadata, role_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_role_action(metadata, role_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_role_action(metadata, role_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_role_action(metadata, role_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: &UserAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_user_action(metadata, user_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_user_action(metadata, user_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_user_action(metadata, user_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_user_action(metadata, user_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &ServerAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer.is_allowed_server_action(metadata, action).await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.is_allowed_server_action(metadata, action).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.is_allowed_server_action(metadata, action).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.is_allowed_server_action(metadata, action).await
            }
        }
    }

    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
        action: &ProjectAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_project_action(metadata, project_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_project_action(metadata, project_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_project_action(metadata, project_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_project_action(metadata, project_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &WarehouseAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_warehouse_action(metadata, warehouse_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_warehouse_action(metadata, warehouse_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_warehouse_action(metadata, warehouse_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_warehouse_action(metadata, warehouse_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_namespace_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        namespace_id: NamespaceIdentUuid,
        action: &NamespaceAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_namespace_action(metadata, warehouse_id, namespace_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_namespace_action(metadata, warehouse_id, namespace_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_namespace_action(metadata, warehouse_id, namespace_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_namespace_action(metadata, warehouse_id, namespace_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_table_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        table_id: TableIdentUuid,
        action: &TableAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_table_action(metadata, warehouse_id, table_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_table_action(metadata, warehouse_id, table_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_table_action(metadata, warehouse_id, table_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_table_action(metadata, warehouse_id, table_id, action)
                    .await
            }
        }
    }

    async fn is_allowed_view_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        view_id: TableIdentUuid,
        action: &ViewAction,
    ) -> Result<bool> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .is_allowed_view_action(metadata, warehouse_id, view_id, action)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .is_allowed_view_action(metadata, warehouse_id, view_id, action)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .is_allowed_view_action(metadata, warehouse_id, view_id, action)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .is_allowed_view_action(metadata, warehouse_id, view_id, action)
                    .await
            }
        }
    }

    async fn create_project(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.create_project(metadata, project_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.create_project(metadata, project_id).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.create_project(metadata, project_id).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.create_project(metadata, project_id).await
            }
        }
    }

    async fn delete_project(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectIdent,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.delete_project(metadata, project_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.delete_project(metadata, project_id).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.delete_project(metadata, project_id).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.delete_project(metadata, project_id).await
            }
        }
    }

    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        parent_project_id: ProjectIdent,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .create_warehouse(metadata, warehouse_id, parent_project_id)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .create_warehouse(metadata, warehouse_id, parent_project_id)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .create_warehouse(metadata, warehouse_id, parent_project_id)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .create_warehouse(metadata, warehouse_id, parent_project_id)
                    .await
            }
        }
    }

    async fn delete_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.delete_warehouse(metadata, warehouse_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.delete_warehouse(metadata, warehouse_id).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.delete_warehouse(metadata, warehouse_id).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.delete_warehouse(metadata, warehouse_id).await
            }
        }
    }

    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        parent: NamespaceParent,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => {
                authorizer
                    .create_namespace(metadata, namespace_id, parent)
                    .await
            }
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer
                    .create_namespace(metadata, namespace_id, parent)
                    .await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer
                    .create_namespace(metadata, namespace_id, parent)
                    .await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer
                    .create_namespace(metadata, namespace_id, parent)
                    .await
            }
        }
    }

    async fn delete_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.delete_namespace(metadata, namespace_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.delete_namespace(metadata, namespace_id).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.delete_namespace(metadata, namespace_id).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.delete_namespace(metadata, namespace_id).await
            }
        }
    }

    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.create_table(metadata, table_id, parent).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.create_table(metadata, table_id, parent).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.create_table(metadata, table_id, parent).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.create_table(metadata, table_id, parent).await
            }
        }
    }

    async fn delete_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.delete_table(metadata, table_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.delete_table(metadata, table_id).await
            }
            Self::OpenFGABearer(authorizer) => authorizer.delete_table(metadata, table_id).await,
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.delete_table(metadata, table_id).await
            }
        }
    }

    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.create_view(metadata, view_id, parent).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.create_view(metadata, view_id, parent).await
            }
            Self::OpenFGABearer(authorizer) => {
                authorizer.create_view(metadata, view_id, parent).await
            }
            Self::OpenFGAClientCreds(authorizer) => {
                authorizer.create_view(metadata, view_id, parent).await
            }
        }
    }

    async fn delete_view(&self, metadata: &RequestMetadata, view_id: ViewIdentUuid) -> Result<()> {
        match self {
            Self::AllowAll(authorizer) => authorizer.delete_view(metadata, view_id).await,
            Self::OpenFGAUnauthorized(authorizer) => {
                authorizer.delete_view(metadata, view_id).await
            }
            Self::OpenFGABearer(authorizer) => authorizer.delete_view(metadata, view_id).await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.delete_view(metadata, view_id).await,
        }
    }
}

#[async_trait::async_trait]
impl HealthExt for Authorizers {
    async fn health(&self) -> Vec<Health> {
        match self {
            Self::AllowAll(authorizer) => authorizer.health().await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.health().await,
            Self::OpenFGABearer(authorizer) => authorizer.health().await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.health().await,
        }
    }

    async fn update_health(&self) {
        match self {
            Self::AllowAll(authorizer) => authorizer.update_health().await,
            Self::OpenFGAUnauthorized(authorizer) => authorizer.update_health().await,
            Self::OpenFGABearer(authorizer) => authorizer.update_health().await,
            Self::OpenFGAClientCreds(authorizer) => authorizer.update_health().await,
        }
    }
}
