use crate::validations::*;
use validator::Validate;

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,
    /// Configured string to string map of properties for the namespace
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl CreateNamespaceRequest {
    pub fn new(namespace: Vec<String>) -> CreateNamespaceRequest {
        CreateNamespaceRequest {
            namespace,
            properties: None,
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,
    /// Properties stored on the namespace, if supported by the server.
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl CreateNamespaceResponse {
    pub fn new(namespace: Vec<String>) -> CreateNamespaceResponse {
        CreateNamespaceResponse {
            namespace,
            properties: None,
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateNamespacePropertiesRequest {
    #[validate(custom(function = "validate_unique_vec"))]
    #[serde(rename = "removals", skip_serializing_if = "Option::is_none")]
    pub removals: Option<Vec<String>>,
    #[serde(rename = "updates", skip_serializing_if = "Option::is_none")]
    pub updates: Option<std::collections::HashMap<String, String>>,
}

impl UpdateNamespacePropertiesRequest {
    pub fn new() -> UpdateNamespacePropertiesRequest {
        UpdateNamespacePropertiesRequest {
            removals: None,
            updates: None,
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetNamespaceResponse {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,
    /// Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.
    #[serde(
        rename = "properties",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateNamespacePropertiesResponse {
    /// List of property keys that were added or updated
    #[serde(rename = "updated")]
    pub updated: Vec<String>,
    /// List of properties that were removed
    #[serde(rename = "removed")]
    pub removed: Vec<String>,
    /// List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.
    #[serde(rename = "missing", default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,
}

impl UpdateNamespacePropertiesResponse {
    pub fn new(updated: Vec<String>, removed: Vec<String>) -> UpdateNamespacePropertiesResponse {
        UpdateNamespacePropertiesResponse {
            updated,
            removed,
            missing: None,
        }
    }
}

#[derive(Clone, Validate, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    /// An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables). Clients may initiate the first paginated request by sending an empty query parameter `pageToken` to the server. Servers that support pagination should identify the `pageToken` parameter and return a `next-page-token` in the response if there are more results available.  After the initial request, the value of `next-page-token` from each response must be used as the `pageToken` parameter value for the next request. The server must return `null` value for the `next-page-token` in the last response. Servers that support pagination must return all results in a single response with the value of `next-page-token` set to `null` if the query parameter `pageToken` is not set in the request. Servers that do not support pagination should ignore the `pageToken` parameter and return all results in a single response. The `next-page-token` must be omitted from the response. Clients must interpret either `null` or missing response value of `next-page-token` as the end of the listing results.
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    #[validate(custom(function = "validate_unique_vec"))]
    #[serde(rename = "namespaces", skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<Vec<String>>>,
}
