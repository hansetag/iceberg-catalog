use crate::api::Result;
use crate::service::health::{Health, HealthExt};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Interface for Handling Secrets.
#[async_trait]

pub trait SecretStore
where
    Self: Send + Sync + 'static + HealthExt + Clone,
{
    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: SecretInStorage + DeserializeOwned>(
        &self,
        secret_id: &SecretIdent,
    ) -> Result<Secret<S>>;

    /// Create a new secret
    async fn create_secret<S: SecretInStorage + Send + Sync + Serialize + std::fmt::Debug>(
        &self,
        secret: S,
    ) -> Result<SecretIdent>;

    /// Delete a secret
    async fn delete_secret(&self, secret_id: &SecretIdent) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum Secrets {
    Postgres(crate::service::catalog_backends::implementations::postgres::SecretsState),
    KV2(crate::service::catalog_backends::implementations::kv2::SecretsState),
}

#[async_trait]
impl SecretStore for Secrets {
    async fn get_secret_by_id<S: SecretInStorage + DeserializeOwned>(
        &self,
        secret_id: &SecretIdent,
    ) -> crate::api::Result<Secret<S>> {
        match self {
            Self::Postgres(state) => state.get_secret_by_id(secret_id).await,
            Self::KV2(state) => state.get_secret_by_id(secret_id).await,
        }
    }

    async fn create_secret<S: SecretInStorage + Send + Sync + Serialize + std::fmt::Debug>(
        &self,
        secret: S,
    ) -> crate::api::Result<SecretIdent> {
        match self {
            Self::Postgres(state) => state.create_secret(secret).await,
            Self::KV2(state) => state.create_secret(secret).await,
        }
    }

    async fn delete_secret(&self, secret_id: &SecretIdent) -> crate::api::Result<()> {
        match self {
            Self::Postgres(state) => state.delete_secret(secret_id).await,
            Self::KV2(state) => state.delete_secret(secret_id).await,
        }
    }
}

#[async_trait]
impl HealthExt for Secrets {
    async fn health(&self) -> Vec<Health> {
        match self {
            Self::Postgres(state) => state.health().await,
            Self::KV2(state) => state.health().await,
        }
    }

    async fn update_health(&self) {
        match self {
            Self::Postgres(state) => state.update_health().await,
            Self::KV2(state) => state.update_health().await,
        }
    }
}

impl From<crate::service::catalog_backends::implementations::postgres::SecretsState> for Secrets {
    fn from(
        state: crate::service::catalog_backends::implementations::postgres::SecretsState,
    ) -> Self {
        Self::Postgres(state)
    }
}

impl From<crate::service::catalog_backends::implementations::kv2::SecretsState> for Secrets {
    fn from(state: crate::service::catalog_backends::implementations::kv2::SecretsState) -> Self {
        Self::KV2(state)
    }
}

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
