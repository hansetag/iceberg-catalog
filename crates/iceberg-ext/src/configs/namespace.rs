use url::Url;

use super::{ParseError, ParseFromStr};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct NamespaceProperties {
    pub(crate) props: HashMap<String, String>,
}

pub trait NotCustomProp {}

impl NamespaceProperties {
    pub fn insert<S>(&mut self, pair: &S)
    where
        S: NamespaceProperty,
    {
        self.props
            .insert(pair.key().to_string(), pair.value_to_string());
    }

    #[must_use]
    pub fn get_prop_opt<C>(&self) -> Option<C::Type>
    where
        C: NamespaceProperty + NotCustomProp,
    {
        self.props
            .get(C::KEY)
            .and_then(|v| ParseFromStr::parse_value(v.as_str()).ok())
    }

    #[must_use]
    pub fn get_prop_fallible<C>(&self) -> Option<Result<C::Type, ParseError>>
    where
        C: NamespaceProperty + NotCustomProp,
    {
        self.props
            .get(C::KEY)
            .map(|v| ParseFromStr::parse_value(v.as_str()))
    }

    #[must_use]
    pub fn get_custom_prop(&self, key: &str) -> Option<String> {
        self.props.get(key).cloned()
    }

    /// Try to create a `NamespaceProperties` from a list of key-value pairs.
    ///
    /// # Errors
    /// Returns an error if a known key has an incompatible value.
    pub fn try_from_props(
        props: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ParseError> {
        let mut config = NamespaceProperties::default();
        for (key, value) in props {
            validate(&key, &value)?;
            config.props.insert(key, value);
        }
        Ok(config)
    }

    pub fn from_props_unchecked(props: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut config = NamespaceProperties::default();
        for (key, value) in props {
            config.props.insert(key, value);
        }
        config
    }
}

#[allow(clippy::implicit_hasher)]
impl From<NamespaceProperties> for HashMap<String, String> {
    fn from(config: NamespaceProperties) -> Self {
        config.props
    }
}

macro_rules! impl_config_value {
    ($struct_name:ident, $typ:ident, $key:expr) => {
        #[derive(Debug, PartialEq, Clone)]
        pub struct $struct_name(pub $typ);

        impl NamespaceProperty for $struct_name {
            const KEY: &'static str = $key;
            type Type = $typ;

            fn value_to_string(&self) -> String {
                self.0.to_string()
            }

            fn parse_value(value: &str) -> Result<Self::Type, ParseError>
            where
                Self::Type: ParseFromStr,
            {
                Self::Type::parse_value(value)
            }
        }

        impl NotCustomProp for $struct_name {}

        paste::paste! {
            impl NamespaceProperties {
                #[must_use]
                pub fn [<$struct_name:snake>](&self) -> Option<$typ> {
                    self.get_prop_opt::<$struct_name>()
                }
            }
        }
    };
}
macro_rules! impl_config_values {
    ($($struct_name:ident, $typ:ident, $key:expr);+ $(;)?) => {
        $(
            impl_config_value!($struct_name, $typ, $key);
        )+

        pub(crate) fn validate(
            key: &str,
            value: &str,
        ) -> Result<(), ParseError> {
            Ok(match key {
                $(
                    $struct_name::KEY => {
                        _ = $struct_name::parse_value(value)?;
                    }
                )+
                _ => {},
            })
        }
    };
}

impl_config_values!(Location, Url, "location");

pub mod custom {
    use super::{NamespaceProperty, ParseError, ParseFromStr};

    #[derive(Debug, PartialEq, Clone)]
    pub struct Pair {
        pub key: String,
        pub value: String,
    }

    impl NamespaceProperty for Pair {
        const KEY: &'static str = "custom";
        type Type = String;

        fn key(&self) -> &str {
            self.key.as_str()
        }

        fn value_to_string(&self) -> String {
            self.value.clone()
        }

        fn parse_value(value: &str) -> Result<Self::Type, ParseError>
        where
            Self::Type: ParseFromStr,
        {
            Ok(value.to_string())
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait NamespaceProperty {
    const KEY: &'static str;
    type Type: ToString + ParseFromStr;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value_to_string(&self) -> String;

    /// Parse the value from a string.
    ///
    /// # Errors
    /// Returns a `ParseError` if the value is incompatible with the type.
    fn parse_value(value: &str) -> Result<Self::Type, ParseError>
    where
        Self::Type: ParseFromStr;
}
