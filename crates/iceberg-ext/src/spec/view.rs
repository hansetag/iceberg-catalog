use self::_serde::*;
use crate::catalog::*;
use crate::spec::*;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

// ToDo: Improve deserialization error messages

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, TypedBuilder)]
#[serde(from = "ViewVersionSerde", into = "ViewVersionSerde")]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct ViewVersion {
    /// ID for the version
    pub version_id: i64,
    /// ID of the schema for the view version, -1 for latest version
    pub schema_id: i64,
    /// Timestamp when the version was created (ms from epoch)
    pub timestamp_ms: i64,
    /// A string to string map of summary metadata about the version
    pub summary: std::collections::HashMap<String, String>,
    /// A list of representations for the view definition
    pub representations: Vec<ViewRepresentation>,
    /// Catalog name to use when a reference in the SELECT does not contain a catalog
    pub default_catalog: Option<String>,
    /// Namespace to use when a reference in the SELECT is a single identifier
    pub default_namespace: NamespaceIdent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ViewRepresentation {
    #[serde(rename = "sql")]
    SqlViewRepresentation(SqlViewRepresentation),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SqlViewRepresentation {
    #[serde(rename = "sql")]
    pub sql: String,
    #[serde(rename = "dialect")]
    pub dialect: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(from = "ViewMetadataSerde", into = "ViewMetadataSerde")]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct ViewMetadata {
    /// A UUID that identifies the view, generated when the view is created.
    /// Implementations must throw an exception if a view's UUID does not
    /// match the expected UUID after refreshing metadata
    pub view_uuid: uuid::Uuid,
    /// An integer version number for the view format; must be 1
    pub format_version: i32,
    /// The view's base location; used to create metadata file locations
    pub location: String,
    /// ID of the current version of the view (version-id)
    pub current_version_id: i64,
    /// A list of known versions of the view [1]
    pub versions: Vec<ViewVersion>,
    /// A list of version log entries with the timestamp and version-id for every change to current-version-id
    pub version_log: Vec<ViewHistoryEntry>,
    /// A list of known schemas
    pub schemas: Vec<Schema>,
    /// A string to string map of view properties [2]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct ViewHistoryEntry {
    pub version_id: i64,
    pub timestamp_ms: i64,
}

pub(super) mod _serde {
    use super::*;

    // Neccesary until https://github.com/serde-rs/serde/issues/745 is resolved
    #[derive(Debug, PartialEq)]
    pub(super) struct FormatVersion<const V: i32>;

    impl<const V: i32> Serialize for FormatVersion<V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_i32(V)
        }
    }

    impl<'de, const V: i32> Deserialize<'de> for FormatVersion<V> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let value = i32::deserialize(deserializer)?;
            if value == V {
                Ok(FormatVersion::<V>)
            } else {
                Err(serde::de::Error::custom(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Unexpected version: {}", value),
                )))
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(untagged)]
    pub(super) enum ViewVersionSerde {
        V1(ViewVersionV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view for serialization/deserialization
    pub(super) struct ViewVersionV1 {
        /// ID for the version
        pub(super) version_id: i64,
        /// ID of the schema for the view version, -1 for latest version
        pub(super) schema_id: i64,
        /// Timestamp when the version was created (ms from epoch)
        pub(super) timestamp_ms: i64,
        /// A string to string map of summary metadata about the version
        pub(super) summary: std::collections::HashMap<String, String>,
        /// A list of representations for the view definition
        pub(super) representations: Vec<ViewRepresentation>,
        /// Catalog name to use when a reference in the SELECT does not contain a catalog
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) default_catalog: Option<String>,
        /// Namespace to use when a reference in the SELECT is a single identifier
        pub(super) default_namespace: NamespaceIdent,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataSerde {
        // #[serde(rename = "1")] // cannot be an integer currently...
        // https://github.com/serde-rs/serde/issues/745
        V1 {
            #[serde(rename = "format-version")]
            format_version: FormatVersion<1>,
            #[serde(flatten)]
            metadata: ViewMetadataV1,
        },
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view for serialization/deserialization
    pub(super) struct ViewMetadataV1 {
        pub(super) view_uuid: uuid::Uuid,
        pub(super) location: String,
        pub(super) current_version_id: i64,
        pub(super) versions: Vec<ViewVersion>,
        pub(super) version_log: Vec<ViewHistoryEntry>,
        pub(super) schemas: Vec<Schema>,
        pub(super) properties: Option<std::collections::HashMap<String, String>>,
    }

    impl From<ViewVersionV1> for ViewVersion {
        fn from(v1: ViewVersionV1) -> Self {
            ViewVersion {
                version_id: 1,
                schema_id: v1.schema_id,
                timestamp_ms: v1.timestamp_ms,
                summary: v1.summary,
                representations: v1.representations,
                default_catalog: v1.default_catalog,
                default_namespace: v1.default_namespace,
            }
        }
    }

    impl From<ViewVersion> for ViewVersionV1 {
        fn from(v: ViewVersion) -> Self {
            ViewVersionV1 {
                version_id: v.version_id,
                schema_id: v.schema_id,
                timestamp_ms: v.timestamp_ms,
                summary: v.summary,
                representations: v.representations,
                default_catalog: v.default_catalog,
                default_namespace: v.default_namespace,
            }
        }
    }

    impl From<ViewVersionSerde> for ViewVersion {
        fn from(v: ViewVersionSerde) -> Self {
            match v {
                ViewVersionSerde::V1(v1) => v1.into(),
            }
        }
    }

    impl From<ViewVersion> for ViewVersionSerde {
        fn from(v: ViewVersion) -> Self {
            ViewVersionSerde::V1(v.into())
        }
    }

    impl From<ViewMetadataV1> for ViewMetadata {
        fn from(v1: ViewMetadataV1) -> Self {
            ViewMetadata {
                view_uuid: v1.view_uuid,
                format_version: 1,
                location: v1.location,
                current_version_id: v1.current_version_id,
                versions: v1.versions,
                version_log: v1.version_log,
                schemas: v1.schemas,
                properties: v1.properties,
            }
        }
    }

    impl From<ViewMetadata> for ViewMetadataV1 {
        fn from(v: ViewMetadata) -> Self {
            ViewMetadataV1 {
                view_uuid: v.view_uuid,
                location: v.location,
                current_version_id: v.current_version_id,
                versions: v.versions,
                version_log: v.version_log,
                schemas: v.schemas,
                properties: v.properties,
            }
        }
    }

    impl From<ViewMetadataSerde> for ViewMetadata {
        fn from(v: ViewMetadataSerde) -> Self {
            match v {
                ViewMetadataSerde::V1 {
                    format_version: _,
                    metadata,
                } => metadata.into(),
            }
        }
    }

    impl From<ViewMetadata> for ViewMetadataSerde {
        fn from(v: ViewMetadata) -> Self {
            ViewMetadataSerde::V1 {
                format_version: FormatVersion::<1>,
                metadata: v.into(),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_view_version() {
        let j = serde_json::json!({
          "version-id" : 1,
          "timestamp-ms" : 1573518431292i64,
          "schema-id" : 1,
          "default-catalog" : "prod",
          "default-namespace" : [ "default" ],
          "summary" : {
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
          },
          "representations" : [ {
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
          } ]
        });

        let r: ViewVersion = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(&r).unwrap(), j);
    }

    #[test]
    fn test_schema_serialization() {
        let j = serde_json::json!({
          "schema-id": 1,
          "type" : "struct",
          "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
          }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
          } ]
        });

        let r: Schema = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(&r).unwrap(), j);
    }

    #[test]
    fn test_view_metadata() {
        let j = serde_json::json!({
          "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
          "format-version" : 1,
          "location" : "s3://bucket/warehouse/default.db/event_agg",
          "current-version-id" : 1,
          "properties" : {
            "comment" : "Daily event counts"
          },
          "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292i64,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
              "engine-name" : "Spark",
              "engineVersion" : "3.3.2"
            },
            "representations" : [ {
              "type" : "sql",
              "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
              "dialect" : "spark"
            } ]
          } ],
          "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
              "id" : 1,
              "name" : "event_count",
              "required" : false,
              "type" : "int",
              "doc" : "Count of events"
            }, {
              "id" : 2,
              "name" : "event_date",
              "required" : false,
              "type" : "date"
            } ]
          } ],
          "version-log" : [ {
            "timestamp-ms" : 1573518431292i64,
            "version-id" : 1
          } ]
        });

        let r: ViewMetadata = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(&r).unwrap(), j);
    }
}
