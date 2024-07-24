use crate::api::{ErrorModel, Result};
use crate::service::health::{Health, HealthExt, HealthStatus};
use crate::service::secrets::{Secret, SecretIdent, SecretStore};
use std::fmt::Formatter;

use async_trait::async_trait;

use iceberg_ext::catalog::rest::IcebergErrorResponse;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Sleep;
use uuid::Uuid;
use vaultrs::client::{Client, VaultClient};

use vaultrs_login::engines::userpass::UserpassLogin;
use vaultrs_login::LoginMethod;

#[derive(Debug, Clone)]
pub struct Server {}

#[derive(Clone)]
pub struct SecretsState {
    vault_client: Arc<RwLock<VaultClient>>,
    secret_mount: String,
    // these are actually accessed, no idea what's clippy's problem here
    #[allow(dead_code)]
    vault_user: String,
    #[allow(dead_code)]
    vault_password: String,
    pub health: Arc<RwLock<Vec<Health>>>,
}

impl std::fmt::Debug for SecretsState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsState")
            .field("vault_client", &"VaultClient")
            .field("secret_mount", &self.secret_mount)
            .field("vault_user", &self.vault_user)
            .field("vault_password", &"REDACTED")
            .field("health", &self.health)
            .finish()
    }
}

#[async_trait]
impl HealthExt for SecretsState {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }

    async fn update_health(&self) {
        let handle = self.vault_client.read().await;
        let t = vaultrs::sys::health(&*handle).await;
        match t {
            Ok(_) => {
                tracing::debug!("Vault is healthy");
                self.health
                    .write()
                    .await
                    .push(Health::now("vault", HealthStatus::Healthy));
            }
            Err(err) => {
                tracing::error!(?err, "Vault is unhealthy");
                self.health
                    .write()
                    .await
                    .push(Health::now("vault", HealthStatus::Unhealthy));
            }
        }
    }
}

impl SecretsState {
    pub async fn login_task(&self) {
        let login = UserpassLogin::new(self.vault_user.as_str(), self.vault_password.as_str());
        let client_handle = self.vault_client.clone();
        let mut sleep = Self::refresh_login(&login, client_handle.clone()).await;
        tokio::task::spawn(async move {
            loop {
                eprintln!("Refreshing token");
                sleep.await;
                sleep = Self::refresh_login(&login, client_handle.clone()).await;
                tracing::debug!("Token refreshed");
            }
        });
    }

    async fn refresh_login(
        login: &UserpassLogin,
        client_handle: Arc<RwLock<VaultClient>>,
    ) -> Sleep {
        tracing::debug!("Refreshing token");

        let log = {
            let handle = &*client_handle.read().await;
            login.login(handle, "userpass").await
        };

        tracing::debug!("Token refreshed");

        let tken = log.unwrap();
        let sleep = tokio::time::sleep(std::time::Duration::from_secs(tken.lease_duration - 10));
        let mut handle = client_handle.write().await;
        handle.set_token(tken.client_token.as_str());
        tracing::debug!("Token refreshed");
        sleep
    }
}

#[async_trait::async_trait]
impl SecretStore for Server {
    type State = SecretsState;

    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: DeserializeOwned>(
        secret_id: &SecretIdent,
        state: SecretsState,
    ) -> Result<Secret<S>> {
        // is there no atomic get for metadata and secret??
        let metadata = vaultrs::kv2::read_metadata(
            &*state.vault_client.read().await,
            state.secret_mount.as_str(),
            &format!("secret/{secret_id}"),
        )
        .await
        .map_err(|err| {
            IcebergErrorResponse::from(ErrorModel::internal(
                "secret metadata read failure",
                "SecretReadFailed",
                Some(Box::new(err)),
            ))
        })?;

        Ok(Secret {
            secret_id: *secret_id,
            secret: vaultrs::kv2::read_version::<S>(
                &*state.vault_client.read().await,
                state.secret_mount.as_str(),
                &format!("secret/{secret_id}"),
                metadata.current_version,
            )
            .await
            .map_err(|err| {
                IcebergErrorResponse::from(ErrorModel::internal(
                    "secret read failure",
                    "SecretReadFailed",
                    Some(Box::new(err)),
                ))
            })?,
            created_at: metadata.created_time.parse().unwrap(),
            updated_at: Some(metadata.updated_time.parse().unwrap()),
        })
    }

    /// Create a new secret
    async fn create_secret<S: Send + Sync + Serialize + std::fmt::Debug>(
        secret: S,
        state: SecretsState,
    ) -> Result<SecretIdent> {
        let secret_id = SecretIdent::from(Uuid::now_v7());
        vaultrs::kv2::set(
            &*state.vault_client.read().await,
            state.secret_mount.as_str(),
            &format!("secret/{}", secret_id.as_uuid()),
            &secret,
        )
        .await
        .map_err(|err| {
            ErrorModel::internal(
                "secret creation failure",
                "SecretCreationFailed",
                Some(Box::new(err)),
            )
        })?;
        Ok(secret_id)
    }

    /// Delete a secret
    async fn delete_secret(secret_id: &SecretIdent, state: SecretsState) -> Result<()> {
        Ok(vaultrs::kv2::delete_metadata(
            &*state.vault_client.read().await,
            state.secret_mount.as_str(),
            &format!("secret/{secret_id}", secret_id = secret_id.as_uuid()),
        )
        .await
        .map_err(|err| {
            ErrorModel::internal(
                "secret deletion failure",
                "SecretDeletionFailed",
                Some(Box::new(err)),
            )
        })?)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::VaultConfig;
    use crate::service::storage::{S3Credential, StorageCredential};
    use crate::CONFIG;

    use super::*;

    #[tokio::test]
    async fn test_write_read_secret() {
        let VaultConfig {
            url,
            user,
            password,
            secret_mount,
        } = CONFIG.vault.clone().unwrap();
        let state = SecretsState {
            vault_client: Arc::new(RwLock::new(
                VaultClient::new(
                    vaultrs::client::VaultClientSettingsBuilder::default()
                        .address(url)
                        .build()
                        .unwrap(),
                )
                .unwrap(),
            )),
            secret_mount,
            vault_user: user,
            vault_password: password,
            health: Arc::default(),
        };
        state.login_task().await;

        let secret: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id: "my access key".to_string(),
            aws_secret_access_key: "my secret key".to_string(),
        }
        .into();

        let secret_id = Server::create_secret(secret.clone(), state.clone())
            .await
            .unwrap();

        let read_secret = Server::get_secret_by_id::<StorageCredential>(&secret_id, state.clone())
            .await
            .unwrap();

        assert_eq!(read_secret.secret, secret);
    }

    #[tokio::test]
    async fn test_delete_secret() {
        let VaultConfig {
            url,
            user,
            password,
            secret_mount,
        } = CONFIG.vault.clone().unwrap();
        let state = SecretsState {
            vault_client: Arc::new(RwLock::new(
                VaultClient::new(
                    vaultrs::client::VaultClientSettingsBuilder::default()
                        .address(url)
                        .build()
                        .unwrap(),
                )
                .unwrap(),
            )),
            secret_mount,
            vault_user: user,
            vault_password: password,
            health: Arc::default(),
        };
        state.login_task().await;

        let secret: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id: "my access key".to_string(),
            aws_secret_access_key: "my secret key".to_string(),
        }
        .into();

        let secret_id = Server::create_secret(secret.clone(), state.clone())
            .await
            .unwrap();

        Server::delete_secret(&secret_id, state.clone())
            .await
            .unwrap();

        let read_secret =
            Server::get_secret_by_id::<StorageCredential>(&secret_id, state.clone()).await;

        assert!(read_secret.is_err());
    }
}
