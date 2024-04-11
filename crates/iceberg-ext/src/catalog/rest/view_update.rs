use crate::spec;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ViewUpdate {
    AssignUuid(AssignUuidUpdate),
    UpgradeFormatVersion(UpgradeFormatVersionUpdate),
    AddSchema(AddSchemaUpdate),
    SetLocation(SetLocationUpdate),
    SetProperties(SetPropertiesUpdate),
    RemoveProperties(RemovePropertiesUpdate),
    AddViewVersion(AddViewVersionUpdate),
    SetCurrentViewVersion(SetCurrentViewVersionUpdate),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpgradeFormatVersionUpdate {
    #[serde(rename = "format-version")]
    pub format_version: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetPropertiesUpdate {
    #[serde(rename = "updates")]
    pub updates: std::collections::HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetLocationUpdate {
    #[serde(rename = "location")]
    pub location: String,
}

/// AssignUuidUpdate : Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssignUuidUpdate {
    #[serde(rename = "uuid")]
    pub uuid: uuid::Uuid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AddSchemaUpdate {
    #[serde(rename = "schema")]
    pub schema: spec::Schema,
    /// The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.
    #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
    pub last_column_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AddViewVersionUpdate {
    #[serde(rename = "view-version")]
    pub view_version: spec::ViewVersion,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemovePropertiesUpdate {
    #[serde(rename = "removals")]
    pub removals: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetCurrentViewVersionUpdate {
    /// The view version id to set as current, or -1 to set last added view version id
    #[serde(rename = "view-version-id")]
    pub view_version_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_view_uuid_update() {
        let json = serde_json::json!({
            "action": "assign-uuid",
            "uuid": "550e8400-e29b-41d4-a716-446655440000"
        });

        let update: ViewUpdate = serde_json::from_value(json.clone()).unwrap();
        match update.clone() {
            ViewUpdate::AssignUuid(u) => {
                assert_eq!(
                    u.uuid,
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
                );
            }
            _ => panic!("Expected BaseUpdate::AssignUuidUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }

    #[test]
    fn test_upgrade_format_version_update() {
        let json = serde_json::json!({
            "action": "upgrade-format-version",
            "format-version": 2
        });

        let update: ViewUpdate = serde_json::from_value(json.clone()).unwrap();
        match update.clone() {
            ViewUpdate::UpgradeFormatVersion(u) => {
                assert_eq!(u.format_version, 2);
            }
            _ => panic!("Expected BaseUpdate::UpgradeFormatVersionUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }
}
