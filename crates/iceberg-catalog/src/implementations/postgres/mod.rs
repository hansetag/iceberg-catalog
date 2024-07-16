mod catalog;
pub(crate) mod dbutils;
pub(crate) mod namespace;
pub(crate) mod secrets;
pub(crate) mod tabular;
pub(crate) mod warehouse;

use crate::CONFIG;
use anyhow::anyhow;
use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;
use std::str::FromStr;

pub use secrets::Server as SecretsStore;

use self::dbutils::DBErrorHandler;
use crate::api::Result;
use crate::config::PgSslMode;

/// # Errors
/// Returns an error if the pool creation fails.
pub async fn get_reader_pool() -> anyhow::Result<sqlx::PgPool> {
    let pool = build_opts()
        .connect_with(build_connect_ops(ConnectionType::Read)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating read pool."))?;
    Ok(pool)
}

/// # Errors
/// Returns an error if the pool cannot be created.
pub async fn get_writer_pool() -> anyhow::Result<sqlx::PgPool> {
    let pool = build_opts()
        .connect_with(build_connect_ops(ConnectionType::Write)?)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating write pool."))?;
    Ok(pool)
}

/// # Errors
/// Returns an error if the migration fails.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::migrate!()
        .run(pool)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error migrating database."))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Catalog {}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct PostgresTransaction {
    transaction: sqlx::Transaction<'static, sqlx::Postgres>,
}

#[async_trait::async_trait]
impl crate::service::Transaction<CatalogState> for PostgresTransaction {
    type Transaction<'a> = &'a mut sqlx::Transaction<'static, sqlx::Postgres>;

    async fn begin_write(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_internal_error("Error starting transaction".to_string()))?;

        Ok(Self { transaction })
    }

    async fn begin_read(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state
            .read_pool
            .begin()
            .await
            .map_err(|e| e.into_internal_error("Error starting transaction".to_string()))?;

        Ok(Self { transaction })
    }

    async fn commit(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .map_err(|e| e.into_internal_error("Error committing transaction".to_string()))?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .map_err(|e| e.into_internal_error("Error rolling back transaction".to_string()))?;
        Ok(())
    }

    fn transaction(&mut self) -> Self::Transaction<'_> {
        &mut self.transaction
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CatalogState {
    #[cfg(feature = "sqlx-postgres")]
    pub read_pool: sqlx::PgPool,
    #[cfg(feature = "sqlx-postgres")]
    pub write_pool: sqlx::PgPool,
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SecretsState {
    #[cfg(feature = "sqlx-postgres")]
    pub read_pool: sqlx::PgPool,
    #[cfg(feature = "sqlx-postgres")]
    pub write_pool: sqlx::PgPool,
}

fn build_opts() -> sqlx::pool::PoolOptions<sqlx::Postgres> {
    sqlx::pool::PoolOptions::default()
        .test_before_acquire(CONFIG.pg_test_before_acquire)
        .max_lifetime(
            CONFIG
                .pg_connection_max_lifetime
                .map(core::time::Duration::from_secs),
        )
        .after_connect(|_conn, meta| {
            Box::pin(async move {
                tracing::debug!(metadata = ?meta, "pg pool established a new connection");
                Ok(())
            })
        })
        .before_acquire(|_conn, meta| {
            Box::pin(async move {
                tracing::trace!(metadata = ?meta, "acquiring connection from pg pool");
                Ok(true)
            })
        })
        .after_release(|_conn, meta| {
            Box::pin(async move {
                tracing::trace!(metadata = ?meta, "connection was released back to pg pool");
                Ok(true)
            })
        })
}

#[derive(Debug, Clone, Copy)]
enum ConnectionType {
    Read,
    Write,
}

fn build_connect_ops(typ: ConnectionType) -> anyhow::Result<PgConnectOptions> {
    let url = match typ {
        ConnectionType::Read => CONFIG.pg_database_url_read.as_deref(),
        ConnectionType::Write => CONFIG.pg_database_url_write.as_deref(),
    };

    let host = match typ {
        ConnectionType::Read => CONFIG.pg_host_r.as_deref(),
        ConnectionType::Write => CONFIG.pg_host_w.as_deref(),
    };
    let opts = if let Some(cfg) = url {
        PgConnectOptions::from_str(cfg)?
    } else {
        PgConnectOptions::new()
            .host(host.ok_or(anyhow!(
                "A connection string or postgres host must be provided."
            ))?)
            .port(CONFIG.pg_port.ok_or(anyhow!(
                "A connection string or postgres port must be provided."
            ))?)
            .username(CONFIG.pg_user.as_deref().ok_or(anyhow!(
                "A connection string or postgres user must be provided."
            ))?)
            .password(CONFIG.pg_password.as_deref().ok_or(anyhow!(
                "A connection string or postgres password must be provided."
            ))?)
            .database(CONFIG.pg_database.as_deref().ok_or(anyhow!(
                "A connection string or postgres database must be provided."
            ))?)
            .ssl_mode(CONFIG.pg_ssl_mode.unwrap_or(PgSslMode::Prefer).into())
    };
    let opts = if let Some(cert) = CONFIG.pg_ssl_root_cert.as_deref() {
        opts.ssl_root_cert(cert)
    } else {
        opts
    };
    let opts = if CONFIG.pg_enable_statement_logging {
        opts
    } else {
        opts.disable_statement_logging()
    };
    Ok(opts)
}
