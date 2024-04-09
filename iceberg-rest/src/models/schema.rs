use crate::models::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct Schema {
    #[serde(flatten)]
    pub r#type: StructType,

    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub identifier_field_ids: Option<Vec<i32>>,
}
