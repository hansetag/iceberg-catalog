use super::{ConfigParseError, ConfigProperty, Location, NotCustomProp, ParseFromStr};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct NamespaceProperties {
    props: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
pub trait NamespaceProperty: ConfigProperty {}

impl NamespaceProperty for Location {}

impl NamespaceProperties {
    pub fn insert<S>(&mut self, pair: &S) -> Option<S::Type>
    where
        S: NamespaceProperty,
    {
        let prev = self
            .props
            .insert(pair.key().to_string(), pair.value_to_string());
        prev.and_then(|v| S::parse_value(v.as_str()).ok())
    }

    pub fn remove<S>(&mut self) -> Option<S::Type>
    where
        S: NamespaceProperty,
    {
        self.props
            .remove(S::KEY)
            .and_then(|v| S::parse_value(v.as_str()).ok())
    }

    pub fn remove_untyped(&mut self, key: &str) -> Option<String> {
        self.props.remove(key)
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
    pub fn get_prop_fallible<C>(&self) -> Option<Result<C::Type, ConfigParseError>>
    where
        C: NamespaceProperty + NotCustomProp,
    {
        self.props
            .get(C::KEY)
            .map(|v| ParseFromStr::parse_value(v.as_str()))
            .map(|r| r.map_err(|e| e.for_key(C::KEY)))
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
    ) -> Result<Self, ConfigParseError> {
        let mut config = NamespaceProperties::default();
        for (key, value) in props {
            validate(&key, &value)?;
            config.props.insert(key, value);
        }
        Ok(config)
    }

    /// Try to create a `NamespaceProperties` from an Option of list of key-value pairs.
    ///
    /// # Errors
    /// Returns an error if a known key has an incompatible value.
    pub fn try_from_maybe_props(
        props: Option<impl IntoIterator<Item = (String, String)>>,
    ) -> Result<Self, ConfigParseError> {
        match props {
            Some(props) => Self::try_from_props(props),
            None => Ok(Self::default()),
        }
    }

    pub fn from_props_unchecked(props: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut config = NamespaceProperties::default();
        for (key, value) in props {
            config.props.insert(key, value);
        }
        config
    }

    #[must_use]
    pub fn get_location(&self) -> Option<Location> {
        self.get_prop_opt::<Location>()
    }
}

impl IntoIterator for NamespaceProperties {
    type Item = (String, String);
    type IntoIter = std::collections::hash_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.props.into_iter()
    }
}

#[allow(clippy::implicit_hasher)]
impl From<NamespaceProperties> for HashMap<String, String> {
    fn from(config: NamespaceProperties) -> Self {
        config.props
    }
}

pub(crate) fn validate(key: &str, value: &str) -> Result<(), ConfigParseError> {
    match key {
        Location::KEY => <Location as ConfigProperty>::parse_value(value).map(|_| ()),
        _ => Ok(()),
    }
}
