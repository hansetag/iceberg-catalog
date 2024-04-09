use validator::Validate;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl ToString for SortDirection {
    fn to_string(&self) -> String {
        match self {
            Self::Asc => String::from("asc"),
            Self::Desc => String::from("desc"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    #[validate(nested)]
    #[serde(rename = "fields")]
    pub fields: Vec<SortField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct SortField {
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "transform")]
    pub transform: String,
    #[serde(rename = "direction")]
    pub direction: SortDirection,
    #[serde(rename = "null-order")]
    pub null_order: NullOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    First,
    #[serde(rename = "nulls-last")]
    Last,
}

impl ToString for NullOrder {
    fn to_string(&self) -> String {
        match self {
            Self::First => String::from("nulls-first"),
            Self::Last => String::from("nulls-last"),
        }
    }
}
