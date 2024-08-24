pub mod namespace;
pub mod table;

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse '{typ}' from '{value}'")]
pub struct ParseError {
    value: String,
    typ: String,
}

/// `ParseFromStr` is a trait that needs to be implemented for the associated type of `ConfigValue`.
///
/// In most cases, it can be a `FromStr` implementation, in other cases, like `bool` we implement it
/// to handle more bool-ish variants such as "1", "t", "true", "f", "false", etc.
pub trait ParseFromStr {
    /// # Errors
    /// Returns a `ParseError` if the value cannot be parsed.
    fn parse_value(value: &str) -> Result<Self, ParseError>
    where
        Self: Sized;
}

impl ParseFromStr for String {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        Ok(value.to_string())
    }
}

impl ParseFromStr for bool {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        match value {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
            value => Err(ParseError {
                value: value.to_string(),
                typ: "bool".to_string(),
            }),
        }
    }
}

impl ParseFromStr for url::Url {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        value.parse().map_err(|_| ParseError {
            value: value.to_string(),
            typ: "Url".to_string(),
        })
    }
}
