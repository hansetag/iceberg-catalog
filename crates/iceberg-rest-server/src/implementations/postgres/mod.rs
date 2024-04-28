mod catalog;
mod config;
pub mod secrets;
use crate::CONFIG;

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
#[allow(clippy::module_name_repetitions)]
pub struct PostgresCatalog {}

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

impl crate::service::CatalogState for CatalogState {}
impl crate::service::SecretsState for SecretsState {}
