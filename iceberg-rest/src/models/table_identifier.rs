use super::Namespace;
use validator::Validate;

#[derive(Clone, Hash, Eq, Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct TableIdentifier {
    /// Reference to one or more levels of a namespace
    #[validate(nested)]
    #[serde(rename = "namespace")]
    pub namespace: Namespace,
    #[serde(rename = "name")]
    pub name: String,
}

impl TableIdentifier {
    pub fn new(namespace: Namespace, name: String) -> TableIdentifier {
        TableIdentifier { namespace, name }
    }
}
