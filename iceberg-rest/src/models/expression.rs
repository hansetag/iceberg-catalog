use crate::models::*;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpressionType {
    Eq,
    And,
    Or,
    Not,
    In,
    NotIn,
    Lt,
    LtEq,
    Gt,
    GtEq,
    NotEq,
    StartsWith,
    NotStartsWith,
    IsNull,
    NotNull,
    IsNan,
    NotNan,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Expression {
    And(And),
    Or(Or),
    Not(Not),
    In(In),
    NotIn(NotIn),
    Eq(Eq),
    Lt(Lt),
    LtEq(LtEq),
    Gt(Gt),
    GtEq(GtEq),
    NotEq(NotEq),
    StartsWith(StartsWith),
    NotStartsWith(NotStartsWith),
    IsNull(IsNull),
    NotNull(NotNull),
    IsNan(IsNan),
    NotNan(NotNan),
}

impl Validate for Expression {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::And(e) => e.validate(),
            Self::Or(e) => e.validate(),
            Self::Not(e) => e.validate(),
            Self::In(e) => e.validate(),
            Self::NotIn(e) => e.validate(),
            Self::Eq(e) => e.validate(),
            Self::Lt(e) => e.validate(),
            Self::LtEq(e) => e.validate(),
            Self::Gt(e) => e.validate(),
            Self::GtEq(e) => e.validate(),
            Self::NotEq(e) => e.validate(),
            Self::StartsWith(e) => e.validate(),
            Self::NotStartsWith(e) => e.validate(),
            Self::IsNull(e) => e.validate(),
            Self::NotNull(e) => e.validate(),
            Self::IsNan(e) => e.validate(),
            Self::NotNan(e) => e.validate(),
        }
    }
}

pub trait AndOrExpression {
    fn left(&self) -> &Expression;

    fn right(&self) -> &Expression;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct And {
    #[validate(nested)]
    #[serde(rename = "left")]
    pub left: Box<Expression>,
    #[validate(nested)]
    #[serde(rename = "right")]
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct Or {
    #[validate(nested)]
    #[serde(rename = "left")]
    pub left: Box<Expression>,
    #[validate(nested)]
    #[serde(rename = "right")]
    pub right: Box<Expression>,
}

impl AndOrExpression for And {
    fn left(&self) -> &Expression {
        &self.left
    }

    fn right(&self) -> &Expression {
        &self.right
    }
}
impl AndOrExpression for Or {
    fn left(&self) -> &Expression {
        &self.left
    }

    fn right(&self) -> &Expression {
        &self.right
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct Not {
    #[validate(nested)]
    #[serde(rename = "child")]
    pub child: Box<Expression>,
}

pub trait UnaryExpression {
    fn term(&self) -> &Term;

    fn value(&self) -> &serde_json::Value;
}

macro_rules! unary_expression {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
        pub struct $name {
            #[validate(nested)]
            #[serde(rename = "term")]
            pub term: Term,
            #[serde(rename = "value")]
            pub value: serde_json::Value,
        }

        impl UnaryExpression for $name {
            fn term(&self) -> &Term {
                &self.term
            }

            fn value(&self) -> &serde_json::Value {
                &self.value
            }
        }
    };
}

unary_expression!(IsNull);
unary_expression!(NotNull);
unary_expression!(IsNan);
unary_expression!(NotNan);

pub trait SetExpression {
    fn term(&self) -> &Term;

    fn values(&self) -> &Vec<serde_json::Value>;
}

macro_rules! set_expression {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
        pub struct $name {
            #[validate(nested)]
            #[serde(rename = "term")]
            pub term: Term,
            #[serde(rename = "values")]
            pub values: Vec<serde_json::Value>,
        }

        impl SetExpression for $name {
            fn term(&self) -> &Term {
                &self.term
            }

            fn values(&self) -> &Vec<serde_json::Value> {
                &self.values
            }
        }
    };
    () => {};
}

set_expression!(In);
set_expression!(NotIn);

pub trait LiteralExpression {
    fn term(&self) -> &Term;

    fn value(&self) -> &serde_json::Value;
}

macro_rules! literal_expression {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
        pub struct $name {
            #[validate(nested)]
            #[serde(rename = "term")]
            pub term: Term,
            #[serde(rename = "value")]
            pub value: serde_json::Value,
        }

        impl LiteralExpression for $name {
            fn term(&self) -> &Term {
                &self.term
            }

            fn value(&self) -> &serde_json::Value {
                &self.value
            }
        }
    };
}

literal_expression!(Lt);
literal_expression!(LtEq);
literal_expression!(Gt);
literal_expression!(GtEq);
literal_expression!(Eq);
literal_expression!(NotEq);
literal_expression!(StartsWith);
literal_expression!(NotStartsWith);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_and_or() {
        let json = serde_json::json!({
            "type": "and",
            "left": {
                "type": "eq",
                "term": "foo",
                "value": "bar"
            },
            "right": {
                "type": "eq",
                "term": "baz",
                "value": "qux"
            }
        });
        let expr: Expression = serde_json::from_value(json.clone()).unwrap();
        assert!(expr.validate().is_ok());

        match expr.clone() {
            Expression::And(e) => match *e.left {
                Expression::Eq(e) => {
                    assert_eq!(e.term, Term::Reference(ReferenceTerm("foo".into())));
                    assert_eq!(e.value, serde_json::Value::String("bar".into()));
                }
                _ => panic!("Expected Expression::Eq"),
            },
            _ => panic!("Expected Expression::And"),
        }

        // Roundtrip
        let json2 = serde_json::to_value(&expr).unwrap();
        assert_eq!(json, json2);
    }
}
