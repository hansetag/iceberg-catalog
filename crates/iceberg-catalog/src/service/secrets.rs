use crate::api::Result;
use crate::service::health::HealthExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone)]
pub struct Secret<T> {
    pub secret_id: SecretIdent,
    pub secret: T,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

// Prohibits us to store unwanted types in the storage.
pub trait SecretInStorage {}

/// Interface for Handling Secrets.
#[async_trait::async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait SecretStore
where
    Self: Clone + Send + Sync + 'static,
{
    type State: Sized + Send + Sync + Clone + 'static + HealthExt;

    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: SecretInStorage + DeserializeOwned>(
        secret_id: &SecretIdent,
        state: Self::State,
    ) -> Result<Secret<S>>;

    /// Create a new secret
    async fn create_secret<S: SecretInStorage + Send + Sync + Serialize + std::fmt::Debug>(
        secret: S,
        state: Self::State,
    ) -> Result<SecretIdent>;

    /// Delete a secret
    async fn delete_secret(secret_id: &SecretIdent, state: Self::State) -> Result<()>;
}
