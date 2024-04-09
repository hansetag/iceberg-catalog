use validator::Validate;

pub const MAX_INT: i32 = 2147483647;
pub const MIN_INT: i32 = -2147483648;
pub const MAX_LONG: i64 = 9223372036854775807;
pub const MIN_LONG: i64 = -9223372036854775808;
pub const MAX_FLOAT: f32 = 3.4028235e38;
pub const MIN_FLOAT: f32 = -3.4028235e38;

#[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Type {
    PrimitiveType(PrimitiveType),
    StructType(StructType),
    ListType(ListType),
    MapType(MapType),
}

impl Type {
    pub fn is_primitive(&self) -> bool {
        matches!(self, Self::PrimitiveType(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, Self::StructType(_))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    Boolean,
    String,
    Int,
    Long,
    Float,
    Double,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Binary,
    #[serde(deserialize_with = "deserialize_fixed")]
    #[serde(serialize_with = "serialize_fixed")]
    #[serde(untagged)]
    Fixed {
        size: i32,
    },
    #[serde(deserialize_with = "deserialize_decimal")]
    #[serde(serialize_with = "serialize_decimal")]
    #[serde(untagged)]
    Decimal {
        precision: i32,
        scale: i32,
    },
}

impl validator::Validate for Type {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::PrimitiveType(_t) => Ok(()), // ToDo: Values?
            Self::StructType(t) => t.validate(),
            Self::ListType(t) => t.validate(),
            Self::MapType(t) => t.validate(),
        }
    }
}

#[derive(Clone, Default, Hash, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(tag = "type", rename = "struct")]
pub struct StructType {
    #[validate(nested)]
    #[serde(rename = "fields")]
    pub fields: Vec<StructField>,
}

impl StructType {
    pub fn new(fields: Vec<StructField>) -> StructType {
        StructType { fields }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize, Validate)]
#[serde(tag = "type", rename = "list")]
pub struct ListType {
    #[serde(rename = "element-id")]
    pub element_id: i32,
    #[validate(nested)]
    #[serde(rename = "element")]
    pub element: Box<Type>,
    #[serde(rename = "element-required")]
    pub element_required: bool,
}

impl ListType {
    pub fn new(element_id: i32, element: Type, element_required: bool) -> ListType {
        ListType {
            element_id,
            element: Box::new(element),
            element_required,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize, Validate)]
#[serde(tag = "type", rename = "map")]
pub struct MapType {
    #[serde(rename = "key-id")]
    pub key_id: i32,
    #[validate(nested)]
    #[serde(rename = "key")]
    pub key: Box<Type>,
    #[serde(rename = "value-id")]
    pub value_id: i32,
    #[validate(nested)]
    #[serde(rename = "value")]
    pub value: Box<Type>,
    #[serde(rename = "value-required")]
    pub value_required: bool,
}

impl MapType {
    pub fn new(
        key_id: i32,
        key: Type,
        value_id: i32,
        value: Type,
        value_required: bool,
    ) -> MapType {
        MapType {
            key_id,
            key: Box::new(key),
            value_id,
            value: Box::new(value),
            value_required,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize, Validate)]
pub struct StructField {
    #[serde(rename = "id")]
    pub id: i32,
    #[serde(rename = "name")]
    pub name: String,
    #[validate(nested)]
    #[serde(rename = "type")]
    pub r#type: Type,
    #[serde(rename = "required")]
    pub required: bool,
    #[serde(rename = "doc", skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl From<MapType> for Type {
    fn from(t: MapType) -> Self {
        Type::MapType(t)
    }
}

impl From<ListType> for Type {
    fn from(t: ListType) -> Self {
        Type::ListType(t)
    }
}

impl From<StructType> for Type {
    fn from(t: StructType) -> Self {
        Type::StructType(t)
    }
}

impl From<PrimitiveType> for Type {
    fn from(t: PrimitiveType) -> Self {
        Type::PrimitiveType(t)
    }
}

// Fixed representation: fixed[SIZE]
pub fn deserialize_fixed<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    use serde::de::Visitor;

    struct FixedVisitor;

    impl<'de> Visitor<'de> for FixedVisitor {
        type Value = i32;

        fn expecting<'a>(&'a self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("fixed[SIZE]")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            let size = value
                .strip_prefix("fixed[")
                .and_then(|s| s.strip_suffix("]"))
                .ok_or_else(|| E::custom("Invalid fixed type"))?
                .parse()
                .map_err(|_| E::custom("Invalid fixed type"))?;
            Ok(size)
        }
    }

    deserializer.deserialize_str(FixedVisitor)
}

pub fn serialize_fixed<S>(size: &i32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("fixed[{}]", size))
}

// Decimal representation: decimal[Precision, Scale]
pub fn deserialize_decimal<'de, D>(deserializer: D) -> Result<(i32, i32), D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    use serde::de::Visitor;

    struct DecimalVisitor;

    impl<'de> Visitor<'de> for DecimalVisitor {
        type Value = (i32, i32);

        fn expecting<'a>(&'a self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("decimal[Precision, Scale]")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            use itertools::Itertools;

            let (precision, scale) = value
                .strip_prefix("decimal[")
                .and_then(|s| s.strip_suffix("]"))
                .ok_or_else(|| E::custom("Invalid decimal type"))?
                .split(',')
                .collect_tuple()
                .ok_or_else(|| E::custom("Expected one comma for decimal type"))?;
            let precision = precision
                .parse()
                .map_err(|_| E::custom("Invalid precision for decimal type"))?;
            let scale = scale
                .parse()
                .map_err(|_| E::custom("Invalid scale for decimal type"))?;
            Ok((precision, scale))
        }
    }

    deserializer.deserialize_str(DecimalVisitor)
}

pub fn serialize_decimal<S>(precision: &i32, scale: &i32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("decimal[{},{}]", precision, scale))
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_types() {
        let t: Type = PrimitiveType::String.into();
        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(s, serde_json::json!("string"));
        assert_eq!(t, serde_json::from_value(s.clone()).unwrap());

        let t: Type = PrimitiveType::Int.into();
        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(s, serde_json::json!("int"));
        assert_eq!(t, serde_json::from_value(s.clone()).unwrap());

        let t: Type = PrimitiveType::Decimal {
            precision: 1,
            scale: 3,
        }
        .into();
        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(s, serde_json::json!("decimal[1,3]"));
        assert_eq!(t, serde_json::from_value(s.clone()).unwrap());

        let t: Type = PrimitiveType::Fixed { size: 10 }.into();
        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(s, serde_json::json!("fixed[10]"));
        assert_eq!(t, serde_json::from_value(s.clone()).unwrap());
    }

    #[test]
    fn test_struct_type() {
        let t: Type = StructType::new(vec![StructField {
            id: 1,
            name: "foo".to_string(),
            r#type: Type::PrimitiveType(PrimitiveType::String).into(),
            required: true,
            doc: None,
        }])
        .into();

        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(
            s,
            serde_json::json!(
                {
                    "type": "struct",
                    "fields": [
                        {
                            "id": 1,
                            "name": "foo",
                            "type": "string",
                            "required": true
                        }
                    ]
                }
            )
        );
    }

    #[test]
    fn test_list_type() {
        let t: Type = ListType::new(1, Type::PrimitiveType(PrimitiveType::String), true).into();

        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(
            s,
            serde_json::json!(
                {
                    "type": "list",
                    "element-id": 1,
                    "element": "string",
                    "element-required": true
                }
            )
        );
    }

    #[test]
    fn test_map_type() {
        let t: Type = MapType::new(
            1,
            Type::PrimitiveType(PrimitiveType::String),
            2,
            Type::PrimitiveType(PrimitiveType::String),
            true,
        )
        .into();

        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(
            s,
            serde_json::json!(
                {
                    "type": "map",
                    "key-id": 1,
                    "key": "string",
                    "value-id": 2,
                    "value": "string",
                    "value-required": true
                }
            )
        );
    }

    #[test]
    fn test_type() {
        let t: Type = Type::PrimitiveType(PrimitiveType::String);

        let s = serde_json::to_value(&t).unwrap();
        assert_eq!(s, serde_json::json!("string"));
    }
}
