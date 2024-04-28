use crate::service::{
    storage::{StorageCredential, StorageProfile},
    ProjectIdent,
};
use utoipa::ToSchema;

#[derive(Debug, Clone, ToSchema)]
pub struct CreateWarehouseRequest {
    pub warehouse_name: String,
    pub project_id: ProjectIdent,
    pub storage_profile: StorageProfile,
    pub secret: StorageCredential,
}
