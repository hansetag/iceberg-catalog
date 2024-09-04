use crate::api::management::v1::TabularType;
use crate::api::Result;
use crate::catalog::maybe_get_secret;
use crate::service::task_queue::{SuccessOrFailure, Task, TaskQueue};
use crate::service::{Catalog, SecretStore, TableIdentUuid, Transaction};
use crate::WarehouseIdent;
use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;
use std::time::Duration;
use uuid::Uuid;

// TODO: concurrent workers
pub async fn purge_task<C: Catalog, S: SecretStore>(
    fetcher: Arc<
        dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync + 'static,
    >,
    catalog_state: C::State,
    secret_state: S,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let expiration = match fetcher.pick_new_task().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(TabularPurgeTask {
            tabular_id,
            warehouse_ident,
            tabular_type,
            task,
        }) = expiration
        else {
            continue;
        };

        let span = tracing::span!(
            tracing::Level::INFO,
            "tabular_cleanup",
            tabular_id = %tabular_id,
            warehouse_id = %warehouse_ident,
            %tabular_type,
            task_name = %task.task_name,
            task = ?task,
        );
        let _entered = span.enter();

        match tabular_type {
            TabularType::Table => {
                tracing::info!("Table");

                match handle_table::<C, S>(
                    warehouse_ident,
                    tabular_id,
                    &secret_state,
                    catalog_state.clone(),
                )
                .await
                {
                    Ok(()) => {
                        fetcher
                            .retrying_record_success_or_failure(&task, SuccessOrFailure::Success)
                            .await;
                        tracing::info!("Successfully handled tabular cleanup");
                    }
                    Err(err) => {
                        tracing::error!("Failed to expire table: {}", err.error);
                        fetcher
                            .retrying_record_failure(&task, &err.error.to_string())
                            .await;
                        continue;
                    }
                };

                let mut retry = 0;
                while let Err(e) = fetcher.record_success(task.task_id).await {
                    tracing::error!("Failed to record success: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    retry += 1;
                    if retry > 5 {
                        tracing::error!("Giving up trying to record success.");
                        break;
                    }
                }
            }
            TabularType::View => {}
        }
    }
}

// Loads the table metadata
// Gets the FileIO for the table
// Deletes data files referenced by old metadata
// Deletes the old metadata
// Deletes data files referenced by current table metadata
// Deletes current table metadata
// Drops the table from the catalog
async fn handle_table<C, S>(
    warehouse_ident: WarehouseIdent,
    tabular_id: Uuid,
    secret_state: &S,
    catalog_state: C::State,
) -> Result<()>
where
    C: Catalog,
    S: SecretStore,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start transaction: {:?}", e);
            e
        })?;

    let mut table_metadata = C::load_tables(
        warehouse_ident,
        [TableIdentUuid::from(tabular_id)],
        true,
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to load tables: {:?}", e);
        e
    })?;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    let table_metadata = table_metadata.remove(&tabular_id.into());
    let Some(table_metadata) = table_metadata else {
        tracing::error!("Table not found: '{tabular_id}'");
        return Err(ErrorModel::internal("Table not found", "TableNotFound", None).into());
    };

    let secret = maybe_get_secret(table_metadata.storage_secret_ident, secret_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get secret: {:?}", e);
            e
        })?;

    let fio = table_metadata
        .storage_profile
        .file_io(secret.as_ref())
        .map_err(|e| {
            tracing::error!("Failed to get storage profile: {:?}", e);
            e
        })?;
    tracing::debug!("Got FileIO");

    fio.remove_all(table_metadata.table_metadata.location())
        .await
        .map_err(|e| {
            ErrorModel::internal(
                "Failed to remove location.",
                "FileIOError",
                Some(Box::new(e)),
            )
        })?;

    Ok(())
}

#[derive(Debug)]
pub struct TabularPurgeTask {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug)]
pub struct TabularPurgeInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub parent_id: Option<Uuid>,
}
