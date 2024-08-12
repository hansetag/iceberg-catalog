use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Default)]
pub struct TableConfig {
    pub(crate) props: HashMap<String, String>,
}

pub trait NotCustomProp {}

impl TableConfig {
    pub fn insert<S>(&mut self, pair: &S)
    where
        S: ConfigValue,
    {
        self.props
            .insert(pair.key().to_string(), pair.value_to_string());
    }

    #[must_use]
    pub fn get_prop<C>(&self) -> Option<C::Type>
    where
        C: ConfigValue + NotCustomProp,
    {
        self.props
            .get(C::KEY)
            .and_then(|v| ParseFromStr::parse_value(v.as_str()).ok())
    }

    #[must_use]
    pub fn get_custom_prop(&self, key: &str) -> Option<String> {
        self.props.get(key).cloned()
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to parse '{typ}' from '{value}'")]
pub struct ParseError {
    value: String,
    typ: String,
}

#[allow(clippy::implicit_hasher)]
impl From<TableConfig> for HashMap<String, String> {
    fn from(config: TableConfig) -> Self {
        config.props
    }
}

macro_rules! impl_config_value {
    ($struct_name:ident, $typ:ident, $key:expr) => {
        #[derive(Debug, PartialEq, Clone)]
        pub struct $struct_name(pub $typ);

        impl ConfigValue for $struct_name {
            const KEY: &'static str = $key;
            type Type = $typ;

            fn value_to_string(&self) -> String {
                self.0.to_string()
            }
        }

        impl NotCustomProp for $struct_name {}
    };
}

pub mod s3 {
    use super::{ConfigValue, NotCustomProp};
    // TODO: add veil support to the macro
    impl_config_value!(Region, String, "s3.region");
    impl_config_value!(Endpoint, String, "s3.endpoint");
    impl_config_value!(PathStyleAccess, bool, "s3.path-style-access");
    impl_config_value!(AccessKeyId, String, "s3.access-key-id");
    impl_config_value!(SecretAccessKey, String, "s3.secret-access-key");
    impl_config_value!(SessionToken, String, "s3.session-token");
    impl_config_value!(RemoteSigningEnabled, bool, "s3.remote-signing-enabled");
    impl_config_value!(Signer, String, "s3.signer");
}

pub mod client {
    use super::{ConfigValue, NotCustomProp};
    impl_config_value!(Region, String, "client.region");
}

pub mod custom {

    #[derive(Debug, PartialEq, Clone)]
    pub struct Pair {
        pub key: String,
        pub value: String,
    }

    impl super::ConfigValue for Pair {
        const KEY: &'static str = "custom";
        type Type = String;

        fn key(&self) -> &str {
            self.key.as_str()
        }

        fn value_to_string(&self) -> String {
            self.value.clone()
        }
    }
}

pub trait ConfigValue {
    const KEY: &'static str;
    type Type: ToString + ParseFromStr;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value_to_string(&self) -> String;
}

/// `ParseFromStr` is a trait that needs to be implemented for the associated type of `ConfigValue`.
///
/// In most cases, it can be a `FromStr` implementation, in other cases, like `bool` we implement it
/// to handle more bool-ish variants such as "1", "t", "true", "f", "false", etc.
pub trait ParseFromStr {
    type Err;

    /// # Errors
    /// Returns a `ParseError` if the value cannot be parsed.
    fn parse_value(value: &str) -> Result<Self, Self::Err>
    where
        Self: Sized;
}

impl ParseFromStr for String {
    type Err = ParseError;

    fn parse_value(value: &str) -> Result<Self, Self::Err> {
        Ok(value.to_string())
    }
}

impl ParseFromStr for bool {
    type Err = ParseError;

    fn parse_value(value: &str) -> Result<Self, Self::Err> {
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
