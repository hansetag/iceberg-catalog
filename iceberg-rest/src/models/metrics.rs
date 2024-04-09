use crate::models;
use std::collections::HashMap;

use validator::Validate;

pub type Metrics = HashMap<String, MetricResult>;

#[derive(Clone, Hash, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CounterResult {
    #[serde(rename = "unit")]
    pub unit: String,
    #[serde(rename = "value")]
    pub value: i64,
}

#[derive(Clone, Hash, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimerResult {
    #[serde(rename = "time-unit")]
    pub time_unit: String,
    #[serde(rename = "count")]
    pub count: i64,
    #[serde(rename = "total-duration")]
    pub total_duration: i64,
}

#[derive(Clone, Hash, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricResult {
    #[validate(nested)]
    #[serde(flatten)]
    pub counter: Option<CounterResult>,
    #[validate(nested)]
    #[serde(flatten)]
    pub timer: Option<TimerResult>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanReport {
    #[serde(rename = "table-name")]
    pub table_name: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[validate(nested)]
    #[serde(rename = "filter")]
    pub filter: models::Expression,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "projected-field-ids")]
    pub projected_field_ids: Vec<i32>,
    #[serde(rename = "projected-field-names")]
    pub projected_field_names: Vec<String>,
    #[validate(nested)]
    #[serde(rename = "metrics")]
    pub metrics: Metrics,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitReport {
    #[serde(rename = "table-name")]
    pub table_name: String,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "operation")]
    pub operation: String,
    #[validate(nested)]
    #[serde(rename = "metrics")]
    pub metrics: Metrics,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_counter_result() {
        let j = serde_json::json!(
            {
                "unit": "unit",
                "value": 42
            }
        );

        let c: CounterResult = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(c.unit, "unit");
        assert_eq!(c.value, 42);
        assert_eq!(serde_json::to_value(c).unwrap(), j);
    }
}
