use super::{ConfigParseError, ConfigProperty, NotCustomProp, ParseError, ParseFromStr};

#[derive(Debug, PartialEq, Clone)]
pub struct Location(url::Url);

impl NotCustomProp for Location {}

impl Location {
    #[must_use]
    pub fn url(&self) -> &url::Url {
        &self.0
    }

    #[must_use]
    pub fn into_url(self) -> url::Url {
        self.0
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn with_trailing_slash(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop_if_empty().push("");
        };
        self
    }

    pub fn without_trailing_slash(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop_if_empty();
        };
        self
    }

    /// Follows the same logic as `url::MutPathSegments::extend`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn extend<I>(&mut self, segments: I) -> &mut Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.extend(segments);
        };
        self
    }

    /// Follows the same logic as `url::MutPathSegments::push`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn push(&mut self, segment: &str) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.push(segment);
        };
        self
    }

    /// Follows the same logic as `url::MutPathSegments::pop`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn pop(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop();
        };
        self
    }
}

impl ConfigProperty for Location {
    const KEY: &'static str = "location";
    type Type = Self;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value(&self) -> &Self::Type {
        self
    }

    fn into_value(self) -> Self::Type {
        self
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ParseFromStr for Location {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        let location = url::Url::parse(value).map_err(|_e| ParseError {
            value: value.to_string(),
            typ: "Url".to_string(),
        })?;

        if location.cannot_be_a_base() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url with base".to_string(),
            });
        }

        if location.fragment().is_some() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url without fragment".to_string(),
            });
        }

        if location.query().is_some() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url without query".to_string(),
            });
        }

        Ok(Location(location))
    }
}

impl std::str::FromStr for Location {
    type Err = ConfigParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        <Location as ConfigProperty>::parse_value(value)
    }
}
