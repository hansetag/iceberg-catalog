use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use utoipa::ToSchema;

use super::{
    entities::{OpenFgaEntity, ParseOpenFgaEntity},
    OpenFGAError, OpenFGAResult,
};
use crate::service::authz::{
    CatalogNamespaceAction, CatalogRoleAction, CatalogTableAction, CatalogViewAction,
};
use crate::service::{
    authz::{
        implementations::FgaType, CatalogProjectAction, CatalogServerAction, CatalogWarehouseAction,
    },
    RoleId, UserId,
};

pub(super) trait Assignment: Sized {
    type Relation: ReducedRelation;
    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[schema(rename_all = "kebab-case")]
pub(super) enum UserOrRole {
    #[schema(value_type = uuid::Uuid)]
    User(UserId),
    #[schema(value_type = uuid::Uuid)]
    Role(RoleId),
}

impl ParseOpenFgaEntity for UserOrRole {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        match r#type {
            FgaType::User => Ok(UserOrRole::User(UserId::parse_from_openfga(id)?)),
            FgaType::Role => Ok(UserOrRole::Role(RoleId::parse_from_openfga(id)?)),
            _ => Err(OpenFGAError::UnexpectedEntity {
                r#type: vec![FgaType::User],
                value: id.to_string(),
            }),
        }
    }
}

impl OpenFgaEntity for UserOrRole {
    fn to_openfga(&self) -> String {
        match self {
            UserOrRole::User(user) => format!("{}:{user}", FgaType::User),
            UserOrRole::Role(role) => format!("{}:{role}", FgaType::Role),
        }
    }

    fn openfga_type(&self) -> FgaType {
        match self {
            UserOrRole::User(_) => FgaType::User,
            UserOrRole::Role(_) => FgaType::Role,
        }
    }
}

pub(super) trait OpenFgaRelation: std::fmt::Display + Clone + Sized + Copy {}
pub(super) trait ReducedRelation: Clone + Sized + Copy + IntoEnumIterator {
    type OpenFgaRelation: OpenFgaRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation;
}

/// Role Relations in the `OpenFGA` schema
#[derive(Debug, Copy, Clone, strum_macros::Display, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum RoleRelation {
    // -- Hierarchical relations --
    Project,
    // -- Direct relations --
    Assignee,
    Ownership,
    // -- Actions --
    CanAssume,
    CanAddAssignee,
    CanRemoveAssignee,
    CanDelete,
    CanUpdate,
    CanRead,
    CanReadAssignments,
}

impl OpenFgaRelation for RoleRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=RoleRelation)]
pub(super) enum APIRoleRelation {
    Assignee,
    Ownership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum RoleAssignment {
    Assignee(UserOrRole),
    Ownership(UserOrRole),
}

impl Assignment for RoleAssignment {
    type Relation = APIRoleRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIRoleRelation::Assignee => {
                UserOrRole::parse_from_openfga(user).map(RoleAssignment::Assignee)
            }
            APIRoleRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(RoleAssignment::Ownership)
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[schema(as=RoleAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIRoleAction {
    Assume,
    AddAssignee,
    RemoveAssignee,
    Delete,
    Update,
    Read,
    ReadAssignments,
}

impl ReducedRelation for APIRoleRelation {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIRoleRelation::Assignee => RoleRelation::Assignee,
            APIRoleRelation::Ownership => RoleRelation::Ownership,
        }
    }
}

impl ReducedRelation for APIRoleAction {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIRoleAction::Assume => RoleRelation::CanAssume,
            APIRoleAction::AddAssignee => RoleRelation::CanAddAssignee,
            APIRoleAction::RemoveAssignee => RoleRelation::CanRemoveAssignee,
            APIRoleAction::Delete => RoleRelation::CanDelete,
            APIRoleAction::Update => RoleRelation::CanUpdate,
            APIRoleAction::Read => RoleRelation::CanRead,
            APIRoleAction::ReadAssignments => RoleRelation::CanReadAssignments,
        }
    }
}

impl ReducedRelation for CatalogRoleAction {
    type OpenFgaRelation = RoleRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogRoleAction::CanDelete => RoleRelation::CanDelete,
            CatalogRoleAction::CanUpdate => RoleRelation::CanUpdate,
            CatalogRoleAction::CanRead => RoleRelation::CanRead,
        }
    }
}

/// Server Relations in the `OpenFGA` schema
#[derive(Copy, Debug, Clone, strum_macros::Display, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ServerRelation {
    // -- Hierarchical relations --
    Project,
    // -- Direct relations --
    GlobalAdmin,
    // -- Actions --
    CanCreateProject,
    CanListAllProjects,
    CanListUsers,
    CanProvisionUsers,
    CanUpdateUsers,
    CanDeleteUsers,
    CanReadAssignments,
    CanGrantGlobalAdmin,
}

impl OpenFgaRelation for ServerRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ServerRelation)]
pub(super) enum APIServerRelation {
    GlobalAdmin,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ServerAssignment {
    GlobalAdmin(UserOrRole),
}

impl Assignment for ServerAssignment {
    type Relation = APIServerRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIServerRelation::GlobalAdmin => {
                UserOrRole::parse_from_openfga(user).map(ServerAssignment::GlobalAdmin)
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[schema(as=ServerAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIServerAction {
    /// Can create items inside the server (can create Warehouses).
    CreateProject,
    /// Can update all users on this server.
    UpdateUsers,
    /// Can delete users on this server apart from myself.
    DeleteUsers,
    /// Can List all users on this server.
    ListUsers,
    /// Can grant global Admin
    GrantGlobalAdmin,
    /// Can provision user
    ProvisionUsers,
    /// Can read assignments
    ReadAssignments,
}

impl ReducedRelation for APIServerRelation {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIServerRelation::GlobalAdmin => ServerRelation::GlobalAdmin,
        }
    }
}

impl ReducedRelation for CatalogServerAction {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogServerAction::CanCreateProject => ServerRelation::CanCreateProject,
            CatalogServerAction::CanUpdateUsers => ServerRelation::CanUpdateUsers,
            CatalogServerAction::CanDeleteUsers => ServerRelation::CanDeleteUsers,
            CatalogServerAction::CanListUsers => ServerRelation::CanListAllProjects,
            CatalogServerAction::CanProvisionUsers => ServerRelation::CanProvisionUsers,
        }
    }
}

impl ReducedRelation for APIServerAction {
    type OpenFgaRelation = ServerRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIServerAction::CreateProject => ServerRelation::CanCreateProject,
            APIServerAction::UpdateUsers => ServerRelation::CanUpdateUsers,
            APIServerAction::DeleteUsers => ServerRelation::CanDeleteUsers,
            APIServerAction::ListUsers => ServerRelation::CanListUsers,
            APIServerAction::ProvisionUsers => ServerRelation::CanProvisionUsers,
            APIServerAction::ReadAssignments => ServerRelation::CanReadAssignments,
            APIServerAction::GrantGlobalAdmin => ServerRelation::CanGrantGlobalAdmin,
        }
    }
}

#[derive(Copy, Debug, Clone, strum_macros::Display, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ProjectRelation {
    // -- Hierarchical relations --
    Warehouse,
    Server,
    // -- Direct relations --
    ProjectAdmin,
    SecurityAdmin,
    WarehouseAdmin,
    RoleCreator,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanIncludeInList,
    CanCreateRole,
    CanListRoles,
    CanSearchRoles,
    CanReadAssignments,
    CanGrantRoleCreator,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantProjectAdmin,
    CanGrantSecurityAdmin,
    CanGrantWarehouseAdmin,
}

impl OpenFgaRelation for ProjectRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ProjectRelation)]
pub(super) enum APIProjectRelation {
    ProjectAdmin,
    SecurityAdmin,
    WarehouseAdmin,
    RoleCreator,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ProjectAssignment {
    ProjectAdmin(UserOrRole),
    SecurityAdmin(UserOrRole),
    WarehouseAdmin(UserOrRole),
    RoleCreator(UserOrRole),
    Describe {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Select {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Create {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Modify {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
}

impl Assignment for ProjectAssignment {
    type Relation = APIProjectRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIProjectRelation::ProjectAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::ProjectAdmin)
            }
            APIProjectRelation::SecurityAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::SecurityAdmin)
            }
            APIProjectRelation::WarehouseAdmin => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::WarehouseAdmin)
            }
            APIProjectRelation::RoleCreator => {
                UserOrRole::parse_from_openfga(user).map(ProjectAssignment::RoleCreator)
            }
            APIProjectRelation::Describe => {
                RoleId::parse_from_openfga(user).map(|role| ProjectAssignment::Describe { role })
            }
            APIProjectRelation::Select => {
                RoleId::parse_from_openfga(user).map(|role| ProjectAssignment::Select { role })
            }
            APIProjectRelation::Create => {
                RoleId::parse_from_openfga(user).map(|role| ProjectAssignment::Create { role })
            }
            APIProjectRelation::Modify => {
                RoleId::parse_from_openfga(user).map(|role| ProjectAssignment::Modify { role })
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ProjectAction)]
pub(super) enum APIProjectAction {
    CreateWarehouse,
    Delete,
    Rename,
    ListWarehouses,
    CreateRole,
    ListRoles,
    SearchRoles,
    ReadAssignments,
    GrantRoleCreator,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantProjectAdmin,
    GrantSecurityAdmin,
    GrantWarehouseAdmin,
}

impl ReducedRelation for APIProjectRelation {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIProjectRelation::ProjectAdmin => ProjectRelation::ProjectAdmin,
            APIProjectRelation::SecurityAdmin => ProjectRelation::SecurityAdmin,
            APIProjectRelation::WarehouseAdmin => ProjectRelation::WarehouseAdmin,
            APIProjectRelation::RoleCreator => ProjectRelation::RoleCreator,
            APIProjectRelation::Describe => ProjectRelation::Describe,
            APIProjectRelation::Select => ProjectRelation::Select,
            APIProjectRelation::Create => ProjectRelation::Create,
            APIProjectRelation::Modify => ProjectRelation::Modify,
        }
    }
}

impl ReducedRelation for APIProjectAction {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIProjectAction::CreateWarehouse => ProjectRelation::CanCreateWarehouse,
            APIProjectAction::Delete => ProjectRelation::CanDelete,
            APIProjectAction::Rename => ProjectRelation::CanRename,
            APIProjectAction::ListWarehouses => ProjectRelation::CanListWarehouses,
            APIProjectAction::CreateRole => ProjectRelation::CanCreateRole,
            APIProjectAction::ListRoles => ProjectRelation::CanListRoles,
            APIProjectAction::SearchRoles => ProjectRelation::CanSearchRoles,
            APIProjectAction::ReadAssignments => ProjectRelation::CanReadAssignments,
            APIProjectAction::GrantRoleCreator => ProjectRelation::CanGrantRoleCreator,
            APIProjectAction::GrantCreate => ProjectRelation::CanGrantCreate,
            APIProjectAction::GrantDescribe => ProjectRelation::CanGrantDescribe,
            APIProjectAction::GrantModify => ProjectRelation::CanGrantModify,
            APIProjectAction::GrantSelect => ProjectRelation::CanGrantSelect,
            APIProjectAction::GrantProjectAdmin => ProjectRelation::CanGrantProjectAdmin,
            APIProjectAction::GrantSecurityAdmin => ProjectRelation::CanGrantSecurityAdmin,
            APIProjectAction::GrantWarehouseAdmin => ProjectRelation::CanGrantWarehouseAdmin,
        }
    }
}

impl ReducedRelation for CatalogProjectAction {
    type OpenFgaRelation = ProjectRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogProjectAction::CanCreateWarehouse => ProjectRelation::CanCreateWarehouse,
            CatalogProjectAction::CanDelete => ProjectRelation::CanDelete,
            CatalogProjectAction::CanRename => ProjectRelation::CanRename,
            CatalogProjectAction::CanGetMetadata => ProjectRelation::CanGetMetadata,
            CatalogProjectAction::CanListWarehouses => ProjectRelation::CanListWarehouses,
            CatalogProjectAction::CanIncludeInList => ProjectRelation::CanIncludeInList,
            CatalogProjectAction::CanCreateRole => ProjectRelation::CanCreateRole,
            CatalogProjectAction::CanListRoles => ProjectRelation::CanListRoles,
            CatalogProjectAction::CanSearchRoles => ProjectRelation::CanSearchRoles,
        }
    }
}

#[derive(Copy, Debug, Clone, strum_macros::Display, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub(super) enum WarehouseRelation {
    // -- Hierarchical relations --
    Project,
    Namespace,
    // -- Managed relations --
    _ManagedAccess,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateNamespace,
    CanDelete,
    CanUpdateStorage,
    CanUpdateStorageCredential,
    CanGetMetadata,
    CanGetConfig,
    CanListNamespaces,
    CanUse,
    CanIncludeInList,
    CanDeactivate,
    CanActivate,
    CanRename,
    CanListDeletedTabulars,
    CanReadAssignments,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanChangeOwnership,
}

impl OpenFgaRelation for WarehouseRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=WarehouseRelation)]
pub(super) enum APIWarehouseRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum WarehouseAssignment {
    Ownership(UserOrRole),
    PassGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    ManageGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Describe {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Select {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Create {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Modify {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
}

impl Assignment for WarehouseAssignment {
    type Relation = APIWarehouseRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIWarehouseRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(WarehouseAssignment::Ownership)
            }
            APIWarehouseRelation::PassGrants => RoleId::parse_from_openfga(user)
                .map(|role| WarehouseAssignment::PassGrants { role }),
            APIWarehouseRelation::ManageGrants => RoleId::parse_from_openfga(user)
                .map(|role| WarehouseAssignment::ManageGrants { role }),
            APIWarehouseRelation::Describe => {
                RoleId::parse_from_openfga(user).map(|role| WarehouseAssignment::Describe { role })
            }
            APIWarehouseRelation::Select => {
                RoleId::parse_from_openfga(user).map(|role| WarehouseAssignment::Select { role })
            }
            APIWarehouseRelation::Create => {
                RoleId::parse_from_openfga(user).map(|role| WarehouseAssignment::Create { role })
            }
            APIWarehouseRelation::Modify => {
                RoleId::parse_from_openfga(user).map(|role| WarehouseAssignment::Modify { role })
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[strum(serialize_all = "snake_case")]
#[schema(as=WarehouseAction)]
pub(super) enum APIWarehouseAction {
    CreateNamespace,
    Delete,
    ModifyStorage,
    ModifyStorageCredential,
    GetConfig,
    GetMetadata,
    ListNamespaces,
    IncludeInList,
    Deactivate,
    Activate,
    Rename,
    ListDeletedTabulars,
    ReadAssignments,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantPassGrants,
    GrantManageGrants,
    ChangeOwnership,
}

impl ReducedRelation for APIWarehouseRelation {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIWarehouseRelation::Ownership => WarehouseRelation::Ownership,
            APIWarehouseRelation::PassGrants => WarehouseRelation::PassGrants,
            APIWarehouseRelation::ManageGrants => WarehouseRelation::ManageGrants,
            APIWarehouseRelation::Describe => WarehouseRelation::Describe,
            APIWarehouseRelation::Select => WarehouseRelation::Select,
            APIWarehouseRelation::Create => WarehouseRelation::Create,
            APIWarehouseRelation::Modify => WarehouseRelation::Modify,
        }
    }
}

impl ReducedRelation for APIWarehouseAction {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIWarehouseAction::CreateNamespace => WarehouseRelation::CanCreateNamespace,
            APIWarehouseAction::Delete => WarehouseRelation::CanDelete,
            APIWarehouseAction::ModifyStorage => WarehouseRelation::CanUpdateStorage,
            APIWarehouseAction::ModifyStorageCredential => {
                WarehouseRelation::CanUpdateStorageCredential
            }
            APIWarehouseAction::GetMetadata => WarehouseRelation::CanGetMetadata,
            APIWarehouseAction::GetConfig => WarehouseRelation::CanGetConfig,
            APIWarehouseAction::ListNamespaces => WarehouseRelation::CanListNamespaces,
            APIWarehouseAction::IncludeInList => WarehouseRelation::CanIncludeInList,
            APIWarehouseAction::Deactivate => WarehouseRelation::CanDeactivate,
            APIWarehouseAction::Activate => WarehouseRelation::CanActivate,
            APIWarehouseAction::Rename => WarehouseRelation::CanRename,
            APIWarehouseAction::ListDeletedTabulars => WarehouseRelation::CanListDeletedTabulars,
            APIWarehouseAction::ReadAssignments => WarehouseRelation::CanReadAssignments,
            APIWarehouseAction::GrantCreate => WarehouseRelation::CanGrantCreate,
            APIWarehouseAction::GrantDescribe => WarehouseRelation::CanGrantDescribe,
            APIWarehouseAction::GrantModify => WarehouseRelation::CanGrantModify,
            APIWarehouseAction::GrantSelect => WarehouseRelation::CanGrantSelect,
            APIWarehouseAction::GrantPassGrants => WarehouseRelation::CanGrantPassGrants,
            APIWarehouseAction::GrantManageGrants => WarehouseRelation::CanGrantManageGrants,
            APIWarehouseAction::ChangeOwnership => WarehouseRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogWarehouseAction {
    type OpenFgaRelation = WarehouseRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogWarehouseAction::CanCreateNamespace => WarehouseRelation::CanCreateNamespace,
            CatalogWarehouseAction::CanDelete => WarehouseRelation::CanDelete,
            CatalogWarehouseAction::CanUpdateStorage => WarehouseRelation::CanUpdateStorage,
            CatalogWarehouseAction::CanUpdateStorageCredential => {
                WarehouseRelation::CanUpdateStorageCredential
            }
            CatalogWarehouseAction::CanGetMetadata => WarehouseRelation::CanGetMetadata,
            CatalogWarehouseAction::CanGetConfig => WarehouseRelation::CanGetConfig,
            CatalogWarehouseAction::CanListNamespaces => WarehouseRelation::CanListNamespaces,
            CatalogWarehouseAction::CanUse => WarehouseRelation::CanUse,
            CatalogWarehouseAction::CanIncludeInList => WarehouseRelation::CanIncludeInList,
            CatalogWarehouseAction::CanDeactivate => WarehouseRelation::CanDeactivate,
            CatalogWarehouseAction::CanActivate => WarehouseRelation::CanActivate,
            CatalogWarehouseAction::CanRename => WarehouseRelation::CanRename,
            CatalogWarehouseAction::CanListDeletedTabulars => {
                WarehouseRelation::CanListDeletedTabulars
            }
        }
    }
}

#[derive(Debug, Copy, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum NamespaceRelation {
    // -- Hierarchical relations --
    Parent,
    Child,
    // -- Managed relations --
    _ManagedAccess,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
    // -- Actions --
    CanCreateTable,
    CanCreateView,
    CanCreateNamespace,
    CanDelete,
    CanUpdateProperties,
    CanGetMetadata,
    CanListTables,
    CanListViews,
    CanListNamespaces,
    _CanIncludeInList,
    CanReadAssignments,
    CanGrantCreate,
    CanGrantDescribe,
    CanGrantModify,
    CanGrantSelect,
    CanGrantPassGrants,
    CanGrantManageGrants,
}

impl OpenFgaRelation for NamespaceRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=NamespaceRelation)]
pub(super) enum APINamespaceRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Create,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum NamespaceAssignment {
    Ownership(UserOrRole),
    PassGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    ManageGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Describe {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Select {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Create {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Modify {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
}

impl Assignment for NamespaceAssignment {
    type Relation = APINamespaceRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APINamespaceRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(NamespaceAssignment::Ownership)
            }
            APINamespaceRelation::PassGrants => RoleId::parse_from_openfga(user)
                .map(|role| NamespaceAssignment::PassGrants { role }),
            APINamespaceRelation::ManageGrants => RoleId::parse_from_openfga(user)
                .map(|role| NamespaceAssignment::ManageGrants { role }),
            APINamespaceRelation::Describe => {
                RoleId::parse_from_openfga(user).map(|role| NamespaceAssignment::Describe { role })
            }
            APINamespaceRelation::Select => {
                RoleId::parse_from_openfga(user).map(|role| NamespaceAssignment::Select { role })
            }
            APINamespaceRelation::Create => {
                RoleId::parse_from_openfga(user).map(|role| NamespaceAssignment::Create { role })
            }
            APINamespaceRelation::Modify => {
                RoleId::parse_from_openfga(user).map(|role| NamespaceAssignment::Modify { role })
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[schema(as=NamespaceAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APINamespaceAction {
    CreateTable,
    CreateView,
    CreateNamespace,
    Delete,
    UpdateProperties,
    GetMetadata,
    ReadAssignments,
    GrantCreate,
    GrantDescribe,
    GrantModify,
    GrantSelect,
    GrantPassGrants,
    GrantManageGrants,
}

impl ReducedRelation for APINamespaceRelation {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APINamespaceRelation::Ownership => NamespaceRelation::Ownership,
            APINamespaceRelation::PassGrants => NamespaceRelation::PassGrants,
            APINamespaceRelation::ManageGrants => NamespaceRelation::ManageGrants,
            APINamespaceRelation::Describe => NamespaceRelation::Describe,
            APINamespaceRelation::Select => NamespaceRelation::Select,
            APINamespaceRelation::Create => NamespaceRelation::Create,
            APINamespaceRelation::Modify => NamespaceRelation::Modify,
        }
    }
}

impl ReducedRelation for APINamespaceAction {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APINamespaceAction::CreateTable => NamespaceRelation::CanCreateTable,
            APINamespaceAction::CreateView => NamespaceRelation::CanCreateView,
            APINamespaceAction::CreateNamespace => NamespaceRelation::CanCreateNamespace,
            APINamespaceAction::Delete => NamespaceRelation::CanDelete,
            APINamespaceAction::UpdateProperties => NamespaceRelation::CanUpdateProperties,
            APINamespaceAction::GetMetadata => NamespaceRelation::CanGetMetadata,
            APINamespaceAction::ReadAssignments => NamespaceRelation::CanReadAssignments,
            APINamespaceAction::GrantCreate => NamespaceRelation::CanGrantCreate,
            APINamespaceAction::GrantDescribe => NamespaceRelation::CanGrantDescribe,
            APINamespaceAction::GrantModify => NamespaceRelation::CanGrantModify,
            APINamespaceAction::GrantSelect => NamespaceRelation::CanGrantSelect,
            APINamespaceAction::GrantPassGrants => NamespaceRelation::CanGrantPassGrants,
            APINamespaceAction::GrantManageGrants => NamespaceRelation::CanGrantManageGrants,
        }
    }
}

impl ReducedRelation for CatalogNamespaceAction {
    type OpenFgaRelation = NamespaceRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogNamespaceAction::CanCreateTable => NamespaceRelation::CanCreateTable,
            CatalogNamespaceAction::CanCreateView => NamespaceRelation::CanCreateView,
            CatalogNamespaceAction::CanCreateNamespace => NamespaceRelation::CanCreateNamespace,
            CatalogNamespaceAction::CanDelete => NamespaceRelation::CanDelete,
            CatalogNamespaceAction::CanUpdateProperties => NamespaceRelation::CanUpdateProperties,
            CatalogNamespaceAction::CanGetMetadata => NamespaceRelation::CanGetMetadata,
            CatalogNamespaceAction::CanListTables => NamespaceRelation::CanListTables,
            CatalogNamespaceAction::CanListViews => NamespaceRelation::CanListViews,
            CatalogNamespaceAction::CanListNamespaces => NamespaceRelation::CanListNamespaces,
        }
    }
}

#[derive(Debug, Copy, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum TableRelation {
    // -- Hierarchical relations --
    Parent,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Modify,
    // -- Actions --
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanIncludeInList,
    CanReadAssignments,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanGrantDescribe,
    CanGrantSelect,
    CanGrantModify,
    CanChangeOwnership,
}

impl OpenFgaRelation for TableRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=TableRelation)]
pub(super) enum APITableRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Select,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum TableAssignment {
    Ownership(UserOrRole),
    PassGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    ManageGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Describe {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Select {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Modify {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
}

impl Assignment for TableAssignment {
    type Relation = APITableRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APITableRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(TableAssignment::Ownership)
            }
            APITableRelation::PassGrants => {
                RoleId::parse_from_openfga(user).map(|role| TableAssignment::PassGrants { role })
            }
            APITableRelation::ManageGrants => {
                RoleId::parse_from_openfga(user).map(|role| TableAssignment::ManageGrants { role })
            }
            APITableRelation::Describe => {
                RoleId::parse_from_openfga(user).map(|role| TableAssignment::Describe { role })
            }
            APITableRelation::Select => {
                RoleId::parse_from_openfga(user).map(|role| TableAssignment::Select { role })
            }
            APITableRelation::Modify => {
                RoleId::parse_from_openfga(user).map(|role| TableAssignment::Modify { role })
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[schema(as=TableAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APITableAction {
    Drop,
    WriteData,
    ReadData,
    GetMetadata,
    Commit,
    Rename,
    ReadAssignments,
    GrantPassGrants,
    GrantManageGrants,
    GrantDescribe,
    GrantSelect,
    GrantModify,
    ChangeOwnership,
}

impl ReducedRelation for APITableRelation {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APITableRelation::Ownership => TableRelation::Ownership,
            APITableRelation::PassGrants => TableRelation::PassGrants,
            APITableRelation::ManageGrants => TableRelation::ManageGrants,
            APITableRelation::Describe => TableRelation::Describe,
            APITableRelation::Select => TableRelation::Select,
            APITableRelation::Modify => TableRelation::Modify,
        }
    }
}

impl ReducedRelation for APITableAction {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APITableAction::Drop => TableRelation::CanDrop,
            APITableAction::WriteData => TableRelation::CanWriteData,
            APITableAction::ReadData => TableRelation::CanReadData,
            APITableAction::GetMetadata => TableRelation::CanGetMetadata,
            APITableAction::Commit => TableRelation::CanCommit,
            APITableAction::Rename => TableRelation::CanRename,
            APITableAction::ReadAssignments => TableRelation::CanReadAssignments,
            APITableAction::GrantPassGrants => TableRelation::CanGrantPassGrants,
            APITableAction::GrantManageGrants => TableRelation::CanGrantManageGrants,
            APITableAction::GrantDescribe => TableRelation::CanGrantDescribe,
            APITableAction::GrantSelect => TableRelation::CanGrantSelect,
            APITableAction::GrantModify => TableRelation::CanGrantModify,
            APITableAction::ChangeOwnership => TableRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogTableAction {
    type OpenFgaRelation = TableRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogTableAction::CanDrop => TableRelation::CanDrop,
            CatalogTableAction::CanWriteData => TableRelation::CanWriteData,
            CatalogTableAction::CanReadData => TableRelation::CanReadData,
            CatalogTableAction::CanGetMetadata => TableRelation::CanGetMetadata,
            CatalogTableAction::CanCommit => TableRelation::CanCommit,
            CatalogTableAction::CanRename => TableRelation::CanRename,
            CatalogTableAction::CanIncludeInList => TableRelation::CanIncludeInList,
        }
    }
}

#[derive(Debug, Copy, Clone, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ViewRelation {
    // -- Hierarchical relations --
    Parent,
    // -- Direct relations --
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Modify,
    // -- Actions --
    CanDrop,
    CanCommit,
    CanGetMetadata,
    CanRename,
    CanIncludeInList,
    CanReadAssignments,
    CanGrantPassGrants,
    CanGrantManageGrants,
    CanGrantDescribe,
    CanGrantModify,
    CanChangeOwnership,
}

impl OpenFgaRelation for ViewRelation {}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, ToSchema, EnumIter)]
#[serde(rename_all = "snake_case")]
#[schema(as=ViewRelation)]
pub(super) enum APIViewRelation {
    Ownership,
    PassGrants,
    ManageGrants,
    Describe,
    Modify,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ViewAssignment {
    Ownership(UserOrRole),
    PassGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    ManageGrants {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Describe {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
    Modify {
        #[schema(value_type = uuid::Uuid)]
        role: RoleId,
    },
}

impl Assignment for ViewAssignment {
    type Relation = APIViewRelation;

    fn try_from_user(user: &str, relation: &Self::Relation) -> OpenFGAResult<Self> {
        match relation {
            APIViewRelation::Ownership => {
                UserOrRole::parse_from_openfga(user).map(ViewAssignment::Ownership)
            }
            APIViewRelation::PassGrants => {
                RoleId::parse_from_openfga(user).map(|role| ViewAssignment::PassGrants { role })
            }
            APIViewRelation::ManageGrants => {
                RoleId::parse_from_openfga(user).map(|role| ViewAssignment::ManageGrants { role })
            }
            APIViewRelation::Describe => {
                RoleId::parse_from_openfga(user).map(|role| ViewAssignment::Describe { role })
            }
            APIViewRelation::Modify => {
                RoleId::parse_from_openfga(user).map(|role| ViewAssignment::Modify { role })
            }
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Serialize, ToSchema, EnumIter)]
#[schema(as=APIViewAction)]
#[serde(rename_all = "snake_case")]
pub(super) enum APIViewAction {
    Drop,
    Commit,
    GetMetadata,
    Rename,
    ReadAssignments,
    GrantPassGrants,
    GrantManageGrants,
    GrantDescribe,
    GrantModify,
    ChangeOwnership,
}

impl ReducedRelation for APIViewRelation {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIViewRelation::Ownership => ViewRelation::Ownership,
            APIViewRelation::PassGrants => ViewRelation::PassGrants,
            APIViewRelation::ManageGrants => ViewRelation::ManageGrants,
            APIViewRelation::Describe => ViewRelation::Describe,
            APIViewRelation::Modify => ViewRelation::Modify,
        }
    }
}

impl ReducedRelation for APIViewAction {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            APIViewAction::Drop => ViewRelation::CanDrop,
            APIViewAction::Commit => ViewRelation::CanCommit,
            APIViewAction::GetMetadata => ViewRelation::CanGetMetadata,
            APIViewAction::Rename => ViewRelation::CanRename,
            APIViewAction::ReadAssignments => ViewRelation::CanReadAssignments,
            APIViewAction::GrantPassGrants => ViewRelation::CanGrantPassGrants,
            APIViewAction::GrantManageGrants => ViewRelation::CanGrantManageGrants,
            APIViewAction::GrantDescribe => ViewRelation::CanGrantDescribe,
            APIViewAction::GrantModify => ViewRelation::CanGrantModify,
            APIViewAction::ChangeOwnership => ViewRelation::CanChangeOwnership,
        }
    }
}

impl ReducedRelation for CatalogViewAction {
    type OpenFgaRelation = ViewRelation;

    fn to_openfga(&self) -> Self::OpenFgaRelation {
        match self {
            CatalogViewAction::CanDrop => ViewRelation::CanDrop,
            CatalogViewAction::CanCommit => ViewRelation::CanCommit,
            CatalogViewAction::CanGetMetadata => ViewRelation::CanGetMetadata,
            CatalogViewAction::CanRename => ViewRelation::CanRename,
            CatalogViewAction::CanIncludeInList => ViewRelation::CanIncludeInList,
        }
    }
}
