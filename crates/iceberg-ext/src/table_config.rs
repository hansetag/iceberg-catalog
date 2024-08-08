use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, PartialEq, Default)]
pub struct TableConfig {
    pub(crate) props: HashMap<String, String>,
}

impl TableConfig {
    pub fn insert(&mut self, pair: &impl ConfigValue) {
        self.props
            .insert(pair.key().to_string(), pair.value_to_string());
    }

    #[must_use]
    pub fn get_prop<C>(&self, key: &str) -> Option<C::TYPE>
    where
        C: ConfigValue,
    {
        self.props.get(key).and_then(|v| v.parse().ok())
    }

    #[must_use]
    pub fn get_string_prop(&self, key: &str) -> Option<String> {
        self.get_prop::<custom::Pair>(key)
    }
}

#[allow(clippy::implicit_hasher)]
impl From<TableConfig> for HashMap<String, String> {
    fn from(config: TableConfig) -> Self {
        config.props
    }
}

pub trait ConfigValue {
    const KEY: &'static str;
    type TYPE: ToString + FromStr;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value_to_string(&self) -> String;
}

macro_rules! impl_config_value {
    ($struct_name:ident, $typ:ident, $key:expr) => {
        #[derive(Debug, PartialEq, Clone)]
        pub struct $struct_name(pub $typ);

        impl ConfigValue for $struct_name {
            const KEY: &'static str = $key;
            type TYPE = $typ;
            fn value_to_string(&self) -> String {
                self.0.to_string()
            }
        }
    };
}

pub mod s3 {
    use super::ConfigValue;
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
    use super::ConfigValue;
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
        type TYPE = String;

        fn key(&self) -> &str {
            self.key.as_str()
        }

        fn value_to_string(&self) -> String {
            self.value.clone()
        }
    }
}
