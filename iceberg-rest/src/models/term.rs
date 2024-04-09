use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Term {
    Reference(ReferenceTerm),
    TransformTerm(TransformTerm),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// Example: "column-name"
pub struct ReferenceTerm(pub String);

impl From<ReferenceTerm> for Term {
    fn from(t: ReferenceTerm) -> Self {
        Self::Reference(t)
    }
}

impl Validate for ReferenceTerm {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// Examples: identity, year, month, day, hour, bucket[256], truncate[16]
pub struct Transform(pub String);

impl Validate for Transform {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

impl Validate for Term {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::Reference(t) => t.validate(),
            Self::TransformTerm(t) => t.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(tag = "type", rename = "transform")]
pub struct TransformTerm {
    #[validate(nested)]
    #[serde(rename = "transform")]
    pub transform: Transform,
    #[validate(nested)]
    #[serde(rename = "term")]
    pub term: ReferenceTerm,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_reference() {
        let json = serde_json::json!("column-name");
        let term: Term = serde_json::from_value(json.clone()).unwrap();

        match term.clone() {
            Term::Reference(r) => {
                assert_eq!(r.0, "column-name");
            }
            _ => panic!("Expected Term::Reference"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&term).unwrap(), json);
    }

    #[test]
    fn test_term_transform_term() {
        let json = serde_json::json!({
            "type": "transform",
            "transform": "identity",
            "term": "column-name"
        });
        let term: Term = serde_json::from_value(json.clone()).unwrap();

        match term.clone() {
            Term::TransformTerm(t) => {
                assert_eq!(t.transform, Transform("identity".into()));
                assert_eq!(t.term.0, "column-name");
            }
            _ => panic!("Expected Term::TransformTerm"),
        }

        // Roundtrip
        assert_eq!(serde_json::to_value(&term).unwrap(), json);
    }
}
