use crate::service::authz::implementations::FgaType;
use crate::service::token_verification::Actor;
use crate::service::{NamespaceIdentUuid, RoleId, TableIdentUuid, ViewIdentUuid};
use crate::{ProjectIdent, WarehouseIdent};

pub(super) trait OpenFgaEntity {
    fn to_openfga(&self) -> crate::api::Result<String>;

    fn openfga_type(&self) -> FgaType;
}

impl OpenFgaEntity for RoleId {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("role:{self}"))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Role
    }
}

impl OpenFgaEntity for Actor {
    fn to_openfga(&self) -> crate::api::Result<String> {
        let fga_type = self.openfga_type().to_string();
        match self {
            Actor::Anonymous => Ok(format!("{fga_type}:*").to_string()),
            Actor::Principal(principal) => Ok(format!("{fga_type}:{principal}")),
            Actor::Role {
                principal: _,
                assumed_role,
            } => Ok(format!("{fga_type}:{assumed_role}#assignee")),
        }
    }

    fn openfga_type(&self) -> FgaType {
        match self {
            Actor::Anonymous | Actor::Principal(_) => FgaType::User,
            Actor::Role { .. } => FgaType::Role,
        }
    }
}

impl OpenFgaEntity for ProjectIdent {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("{}:{self}", self.openfga_type()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Project
    }
}

impl OpenFgaEntity for WarehouseIdent {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("{}:{self}", self.openfga_type()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Warehouse
    }
}

impl OpenFgaEntity for TableIdentUuid {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("{}:{self}", self.openfga_type()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Table
    }
}

impl OpenFgaEntity for NamespaceIdentUuid {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("{}:{self}", self.openfga_type()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Namespace
    }
}

impl OpenFgaEntity for ViewIdentUuid {
    fn to_openfga(&self) -> crate::api::Result<String> {
        Ok(format!("{}:{self}", self.openfga_type()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::View
    }
}
