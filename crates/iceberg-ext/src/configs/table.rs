use super::{ParseError, ParseFromStr};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Default)]
#[allow(clippy::module_name_repetitions)]
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
    pub fn get_prop_opt<C>(&self) -> Option<C::Type>
    where
        C: ConfigValue + NotCustomProp,
    {
        self.props
            .get(C::KEY)
            .and_then(|v| ParseFromStr::parse_value(v.as_str()).ok())
    }

    #[must_use]
    pub fn get_prop_fallible<C>(&self) -> Option<Result<C::Type, ParseError>>
    where
        C: ConfigValue + NotCustomProp,
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
        let mut config = TableConfig::default();
        for (key, value) in props {
            if key.starts_with("s3") {
                s3::validate(&key, &value)?;
                config.props.insert(key, value);
            } else if key.starts_with("client") {
                client::validate(&key, &value)?;
                config.props.insert(key, value);
            } else {
                let pair = custom::Pair {
                    key: key.clone(),
                    value,
                };
                config.insert(&pair);
            }
        }
        Ok(config)
    }

    pub fn from_props_unchecked(props: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut table_config = TableConfig::default();
        for (key, value) in props {
            table_config.props.insert(key, value);
        }
        table_config
    }
}

#[allow(clippy::implicit_hasher)]
impl From<TableConfig> for HashMap<String, String> {
    fn from(config: TableConfig) -> Self {
        config.props
    }
}

macro_rules! impl_config_value {
    ($struct_name:ident, $typ:ident, $key:expr, $accessor:expr) => {
        #[derive(Debug, PartialEq, Clone)]
        pub struct $struct_name(pub $typ);

        impl ConfigValue for $struct_name {
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
            impl TableConfig {
                #[must_use]
                pub fn [<$accessor:snake>](&self) -> Option<$typ> {
                    self.get_prop_opt::<$struct_name>()
                }
            }
        }
    };
}
macro_rules! impl_config_values {
    ($($struct_name:ident, $typ:ident, $key:expr, $accessor:expr);+ $(;)?) => {
        $(
            impl_config_value!($struct_name, $typ, $key, $accessor);
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

pub mod s3 {
    use super::{ConfigValue, NotCustomProp, ParseError, ParseFromStr, TableConfig};
    use url::Url;

    impl_config_values!(
        Region, String, "s3.region", "s3_region";
        Endpoint, Url, "s3.endpoint", "s3_endpoint";
        PathStyleAccess, bool, "s3.path-style-access", "s3_path_style_access";
        AccessKeyId, String, "s3.access-key-id", "s3_access_key_id";
        SecretAccessKey, String, "s3.secret-access-key", "s3_secret_access_key";
        SessionToken, String, "s3.session-token", "s3_session_token";
        RemoteSigningEnabled, bool, "s3.remote-signing-enabled", "s3_remote_signing_enabled";
        Signer, String, "s3.signer", "s3_signer";
    );
}

pub mod client {
    use super::{ConfigValue, NotCustomProp, ParseError, ParseFromStr, TableConfig};
    impl_config_values!(Region, String, "client.region", "client_region");
}

pub mod custom {
    use super::{ConfigValue, ParseError, ParseFromStr};

    #[derive(Debug, PartialEq, Clone)]
    pub struct Pair {
        pub key: String,
        pub value: String,
    }

    impl ConfigValue for Pair {
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

pub trait ConfigValue {
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
