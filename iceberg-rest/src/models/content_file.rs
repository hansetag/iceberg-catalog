// use std::fs::File;

// use crate::models;
// use validator::Validate;

// // This does not serialize according to the openapi spec due
// // to PrimitiveTypeValue not beeing deserializable on its own.
// // Java impl: https://github.com/apache/iceberg/blob/b3261d07fc687d5df1f0f8309bc5a0f6022434cf/core/src/main/java/org/apache/iceberg/ContentFileParser.java#L34

// #[derive(Clone, PartialOrd, Debug, PartialEq, Serialize, Deserialize)]
// #[serde(tag = "type", rename_all = "kebab-case")]
// pub enum PrimitiveTypeValue {
//     BooleanTypeValue(bool),
//     IntegerTypeValue(i32),
//     LongTypeValue(i64),
//     FloatTypeValue(f32),
//     DoubleTypeValue(f64),
//     /// Decimal type values are serialized as strings. Decimals with a positive scale serialize as numeric plain text, while decimals with a negative scale use scientific notation and the exponent will be equal to the negated scale. For instance, a decimal with a positive scale is '123.4500', with zero scale is '2', and with a negative scale is '2E+20'
//     DecimalTypeValue(String),
//     StringTypeValue(String),
//     /// UUID type values are serialized as a 36-character lowercase string in standard UUID format as specified by RFC-4122
//     UuidTypeValue(uuid::Uuid),
//     /// Date type values follow the 'YYYY-MM-DD' ISO-8601 standard date format
//     DateTypeValue(String),
//     /// Time type values follow the 'HH:MM:SS.ssssss' ISO-8601 format with microsecond precision
//     TimeTypeValue(String),
//     /// Timestamp type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss' ISO-8601 format with microsecond precision
//     TimestampTypeValue(String),
//     /// TimestampTz type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss+00:00' ISO-8601 format with microsecond precision, and a timezone offset (+00:00 for UTC)
//     TimestampTzTypeValue(String),
//     /// Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss' ISO-8601 format with nanosecond precision
//     TimestampNanoTypeValue(String),
//     /// Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss+00:00' ISO-8601 format with nanosecond precision, and a timezone offset (+00:00 for UTC)
//     TimestampTzNanoTypeValue(String),
//     /// Fixed length type values are stored and serialized as an uppercase hexadecimal string preserving the fixed length
//     FixedTypeValue(String),
//     /// Binary type values are stored and serialized as an uppercase hexadecimal string
//     BinaryTypeValue(String),
// }

// #[derive(Clone, PartialOrd, Hash, Validate, Debug, PartialEq, Serialize, Deserialize)]
// pub struct CountMap {
//     /// List of integer column ids for each corresponding value
//     #[serde(rename = "keys", skip_serializing_if = "Option::is_none")]
//     pub keys: Option<Vec<i32>>,
//     /// List of Long values, matched to 'keys' by index
//     #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
//     pub values: Option<Vec<i64>>,
// }

// #[derive(Clone, PartialOrd, PartialEq, Validate, Debug, Serialize, Deserialize)]
// pub struct ValueMap {
//     /// List of integer column ids for each corresponding value
//     #[serde(rename = "keys", skip_serializing_if = "Option::is_none")]
//     pub keys: Option<Vec<i32>>,
//     /// List of primitive type values, matched to 'keys' by index
//     #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
//     pub values: Option<Vec<models::PrimitiveTypeValue>>,
// }

// #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
// pub enum FileFormat {
//     #[serde(rename = "avro")]
//     Avro,
//     #[serde(rename = "orc")]
//     Orc,
//     #[serde(rename = "parquet")]
//     Parquet,
// }

// impl Validate for FileFormat {
//     fn validate(&self) -> Result<(), validator::ValidationErrors> {
//         Ok(())
//     }
// }

// #[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
// pub struct ContentFile {
//     #[validate(nested)]
//     #[serde(flatten)]
//     content: Content,

//     #[serde(rename = "file-path")]
//     file_path: String,
//     #[validate(nested)]
//     #[serde(rename = "file-format")]
//     file_format: models::FileFormat,
//     #[serde(rename = "spec-id")]
//     spec_id: i32,
//     /// A list of partition field values ordered based on the fields of the partition spec specified by the `spec-id`
//     #[serde(rename = "partition", skip_serializing_if = "Option::is_none")]
//     partition: Option<Vec<models::PrimitiveTypeValue>>,
//     /// Total file size in bytes
//     #[serde(rename = "file-size-in-bytes")]
//     file_size_in_bytes: i64,
//     /// Number of records in the file
//     #[serde(rename = "record-count")]
//     record_count: i64,
//     /// Encryption key metadata blob
//     #[serde(rename = "key-metadata", skip_serializing_if = "Option::is_none")]
//     key_metadata: Option<String>,
//     /// List of splittable offsets
//     #[serde(rename = "split-offsets", skip_serializing_if = "Option::is_none")]
//     split_offsets: Option<Vec<i64>>,
//     #[serde(rename = "sort-order-id", skip_serializing_if = "Option::is_none")]
//     sort_order_id: Option<i32>,
// }

// #[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
// #[serde(tag = "content")]
// pub enum Content {
//     #[serde(rename = "data")]
//     DataFile {
//         #[serde(rename = "column-sizes", skip_serializing_if = "Option::is_none")]
//         column_sizes: Option<models::CountMap>,
//         #[serde(rename = "value-counts", skip_serializing_if = "Option::is_none")]
//         value_counts: Option<models::CountMap>,
//         #[serde(rename = "null-value-counts", skip_serializing_if = "Option::is_none")]
//         null_value_counts: Option<models::CountMap>,
//         #[serde(rename = "nan-value-counts", skip_serializing_if = "Option::is_none")]
//         nan_value_counts: Option<models::CountMap>,
//         #[serde(rename = "lower-bounds", skip_serializing_if = "Option::is_none")]
//         lower_bounds: Option<models::ValueMap>,
//         #[serde(rename = "upper-bounds", skip_serializing_if = "Option::is_none")]
//         upper_bounds: Option<models::ValueMap>,
//     },
//     #[serde(rename = "equality-deletes")]
//     EqualityDeleteFile {
//         #[serde(rename = "equality-ids", skip_serializing_if = "Option::is_none")]
//         equality_ids: Option<Vec<i32>>,
//     },
//     #[serde(rename = "position-deletes")]
//     PositionDeleteFile {},
// }

// impl Validate for Content {
//     fn validate(&self) -> Result<(), validator::ValidationErrors> {
//         match self {
//             Content::DataFile {
//                 column_sizes,
//                 value_counts,
//                 null_value_counts,
//                 nan_value_counts,
//                 lower_bounds,
//                 upper_bounds,
//             } => {
//                 if let Some(column_sizes) = column_sizes {
//                     column_sizes.validate()?;
//                 }
//                 if let Some(value_counts) = value_counts {
//                     value_counts.validate()?;
//                 }
//                 if let Some(null_value_counts) = null_value_counts {
//                     null_value_counts.validate()?;
//                 }
//                 if let Some(nan_value_counts) = nan_value_counts {
//                     nan_value_counts.validate()?;
//                 }
//                 if let Some(lower_bounds) = lower_bounds {
//                     lower_bounds.validate()?;
//                 }
//                 if let Some(upper_bounds) = upper_bounds {
//                     upper_bounds.validate()?;
//                 }
//             }
//             Content::EqualityDeleteFile { equality_ids } => {
//                 if let Some(_equality_ids) = equality_ids {}
//             }
//             Content::PositionDeleteFile {} => {}
//         }
//         Ok(())
//     }
// }
