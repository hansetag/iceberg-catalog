use crate::catalog::rest::ErrorModel;
use iceberg::spec::ViewMetadata;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRequirement {
    AssertViewUuid(AssertViewUuid),
}

pub trait ViewRequirementExt {
    fn assert(&self, view: &ViewMetadata) -> Result<(), ErrorModel>;
}

impl ViewRequirementExt for ViewRequirement {
    fn assert(&self, view: &ViewMetadata) -> Result<(), ErrorModel> {
        match self {
            ViewRequirement::AssertViewUuid(x) => {
                if view.uuid() != x.uuid {
                    return Err(ErrorModel::builder()
                        .code(409)
                        .message("View UUID does not match")
                        .r#type("ContractViolation".to_string())
                        .build()
                        .into());
                }
            }
        }
        Ok(())
    }
}

/// The view UUID must match the requirement's `uuid`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AssertViewUuid {
    pub uuid: uuid::Uuid,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assert_view_uuid() {
        let j = serde_json::json!({
            "type": "assert-view-uuid",
            "uuid": "550e8400-e29b-41d4-a716-446655440000"
        });

        let r: ViewRequirement = serde_json::from_value(j.clone()).unwrap();
        match r.clone() {
            ViewRequirement::AssertViewUuid(x) => {
                assert_eq!(
                    x.uuid,
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
                );
            }
        }
        assert_eq!(serde_json::to_value(&r).unwrap(), j);
    }
}
