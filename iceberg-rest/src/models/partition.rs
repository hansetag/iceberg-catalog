use crate::models::*;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct PartitionField {
    #[serde(rename = "field-id", skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "name")]
    pub name: String,
    #[validate(nested)]
    #[serde(rename = "transform")]
    pub transform: Transform,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id", skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    #[validate(nested)]
    #[serde(rename = "fields")]
    pub fields: Vec<PartitionField>,
}
