use super::SecretsState;
use crate::api::{ErrorModel, Result};
use crate::service::secrets::{Secret, SecretIdent, SecretStore};
use crate::CONFIG;
use http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Server {}

#[async_trait::async_trait]
impl SecretStore for Server {
    type State = SecretsState;

    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: for<'de> Deserialize<'de>>(
        secret_id: &SecretIdent,
        state: SecretsState,
    ) -> Result<Secret<S>> {
        struct SecretRow {
            secret: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: Option<chrono::DateTime<chrono::Utc>>,
        }

        let secret: SecretRow = sqlx::query_as!(
            SecretRow,
            r#"
            SELECT 
                pgp_sym_decrypt(secret, $2, 'cipher-algo=aes256') as secret,
                created_at,
                updated_at
            FROM secret
            WHERE secret_id = $1
            "#,
            secret_id.as_uuid(),
            CONFIG.pg_encryption_key
        )
        .fetch_one(&state.read_pool())
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Secret not found".to_string())
                .r#type("SecretNotFound".to_string())
                .stack(vec![format!("secret_id: {}", secret_id), e.to_string()])
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching secret".to_string())
                .r#type("SecretFetchError".to_string())
                .stack(vec![format!("secret_id: {}", secret_id), e.to_string()])
                .build(),
        })?;

        let inner =
            serde_json::from_str(&secret.secret.unwrap_or("{}".to_string())).map_err(|_e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error parsing secret".to_string())
                    .r#type("SecretParseError".to_string())
                    // We do not add the error here as it might contain sensitive information
                    .stack(vec![format!("Secret ID: {}", secret_id)])
                    .build()
            })?;

        Ok(Secret {
            secret_id: *secret_id,
            secret: inner,
            created_at: secret.created_at,
            updated_at: secret.updated_at,
        })
    }

    /// Create a new secret
    async fn create_secret<S: Send + Sync + Serialize + std::fmt::Debug>(
        secret: S,
        state: SecretsState,
    ) -> Result<SecretIdent> {
        let secret_str = serde_json::to_string(&secret).map_err(|_e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error serializing secret".to_string())
                .r#type("SecretSerializeError".to_string())
                // Redacted by veil
                .stack(vec![format!("secret: {:?}", secret)])
                .build()
        })?;

        let secret_id = sqlx::query_scalar!(
            r#"
            INSERT INTO secret (secret)
            VALUES (pgp_sym_encrypt($1, $2, 'cipher-algo=aes256'))
            RETURNING secret_id
            "#,
            secret_str,
            CONFIG.pg_encryption_key,
        )
        .fetch_one(&state.write_pool())
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error creating secret".to_string())
                .r#type("SecretCreateError".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?;

        Ok(secret_id.into())
    }

    /// Delete a secret
    async fn delete_secret(secret_id: &SecretIdent, state: SecretsState) -> Result<()> {
        sqlx::query!(
            r#"
            DELETE FROM secret
            WHERE secret_id = $1
            "#,
            secret_id.as_uuid()
        )
        .execute(&state.write_pool())
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error deleting secret".to_string())
                .r#type("SecretDeleteError".to_string())
                .stack(vec![format!("secret_id: {}", secret_id)])
                .source(Some(Box::new(e)))
                .build()
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::service::storage::{S3Credential, StorageCredential};

    use super::*;

    #[sqlx::test]
    async fn test_write_read_secret(pool: sqlx::PgPool) {
        let state = SecretsState::from_pools(pool.clone(), pool);

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

    #[sqlx::test]
    async fn test_delete_secret(pool: sqlx::PgPool) {
        let state = SecretsState::from_pools(pool.clone(), pool);

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
