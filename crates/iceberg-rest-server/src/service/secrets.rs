use iceberg_rest_service::Result;
use serde::{Deserialize, Serialize};

#[allow(clippy::module_name_repetitions)]
pub trait SecretsState: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
// Is UUID here too strict?
pub struct SecretIdent(uuid::Uuid);

impl SecretIdent {
    #[must_use]
    #[inline]
    pub fn into_uuid(&self) -> uuid::Uuid {
        self.0
    }

    #[must_use]
    #[inline]
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl From<uuid::Uuid> for SecretIdent {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SecretIdent> for uuid::Uuid {
    fn from(ident: SecretIdent) -> Self {
        ident.0
    }
}

impl std::fmt::Display for SecretIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// #[derive(Debug, Clone, PartialEq, strum_macros::Display)]
// #[strum(serialize_all = "snake_case")]
// pub enum SecretType {
//     Storage,
// }

#[derive(Debug, Clone)]
pub struct Secret<T> {
    pub secret_id: SecretIdent,
    pub secret: T,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Interface for Handling Secrets.
#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait SecretStore<T: SecretsState>
where
    Self: Sized + Send + Sync + Clone + 'static,
{
    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: for<'de> Deserialize<'de>>(
        state: T,
        secret_id: &SecretIdent,
    ) -> Result<Secret<S>>;

    /// Create a new secret
    async fn create_secret<S: Send + Sync + Serialize + std::fmt::Debug>(
        state: T,
        secret: S,
    ) -> Result<SecretIdent>;
}
