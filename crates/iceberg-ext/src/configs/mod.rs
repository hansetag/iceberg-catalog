use heck::ToUpperCamelCase;

mod location;
pub mod namespace;
pub mod table;

pub use custom::CustomConfig;
pub use location::Location;

pub trait NotCustomProp {}

#[allow(clippy::module_name_repetitions)]
pub trait ConfigProperty {
    const KEY: &'static str;
    type Type: ToString + ParseFromStr;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value(&self) -> &Self::Type;

    fn into_value(self) -> Self::Type;

    fn value_to_string(&self) -> String {
        self.value().to_string()
    }

    /// Parse the value from a string.
    ///
    /// # Errors
    /// Returns a `ParseError` if the value is incompatible with the type.
    fn parse_value(value: &str) -> Result<Self::Type, ConfigParseError>
    where
        Self::Type: ParseFromStr,
    {
        ParseFromStr::parse_value(value).map_err(|e| e.for_key(Self::KEY))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse value '{value}' to '{typ}'.")]
pub struct ParseError {
    value: String,
    typ: String,
}

impl ParseError {
    #[must_use]
    pub fn for_key(self, key: &str) -> ConfigParseError {
        ConfigParseError {
            value: self.value,
            typ: self.typ,
            key: key.to_string(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse config '{key}' with value '{value}' to '{typ}'")]
pub struct ConfigParseError {
    value: String,
    typ: String,
    key: String,
}

impl ConfigParseError {
    #[must_use]
    pub fn err_type(&self) -> String {
        format!("Config{}ParseError", self.key.to_upper_camel_case())
    }
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

mod custom {
    use super::ConfigProperty;

    #[derive(Debug, PartialEq, Clone)]
    #[allow(clippy::module_name_repetitions)]
    pub struct CustomConfig {
        pub key: String,
        pub value: String,
    }

    impl ConfigProperty for CustomConfig {
        const KEY: &'static str = "custom";
        type Type = String;

        fn key(&self) -> &str {
            self.key.as_str()
        }

        fn value(&self) -> &Self::Type {
            &self.value
        }

        fn into_value(self) -> Self::Type {
            self.value
        }
    }
}
