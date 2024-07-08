pub use iceberg::ViewUpdate;

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
            ViewUpdate::AssignUuid { uuid } => {
                assert_eq!(
                    uuid,
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
            ViewUpdate::UpgradeFormatVersion { format_version } => {
                assert_eq!(format_version, 2);
            }
            _ => panic!("Expected BaseUpdate::UpgradeFormatVersionUpdate"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&update).unwrap(), json);
    }
}
