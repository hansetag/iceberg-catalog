mod catalog;
pub(crate) mod dbutils;
pub(crate) mod namespace;
pub(crate) mod tabular;
pub(crate) mod warehouse;

pub(crate) mod secrets;

use crate::CONFIG;

pub use secrets::Server as SecretsStore;

use crate::api::Result;

use self::dbutils::DBErrorHandler;

/// # Errors
/// Returns an error if the pool creation fails.
pub async fn get_reader_pool() -> anyhow::Result<sqlx::PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(CONFIG.pg_read_pool_connections)
        .connect(&CONFIG.pg_database_url_read)
        .await
        .map_err(|e| anyhow::anyhow!(e).context("Error creating read pool."))?;
    Ok(pool)
}

/// # Errors
/// Returns an error if the pool cannot be created.
pub async fn get_writer_pool() -> anyhow::Result<sqlx::PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(CONFIG.pg_write_pool_connections)
        .connect(&CONFIG.pg_database_url_write)
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
            .map_err(|e| e.into_error_model("Error starting transaction".to_string()))?;

        Ok(Self { transaction })
    }

    async fn begin_read(db_state: CatalogState) -> Result<Self> {
        let transaction = db_state
            .read_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("Error starting transaction".to_string()))?;

        Ok(Self { transaction })
    }

    async fn commit(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .map_err(|e| e.into_error_model("Error committing transaction".to_string()))?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .map_err(|e| e.into_error_model("Error rolling back transaction".to_string()))?;
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
