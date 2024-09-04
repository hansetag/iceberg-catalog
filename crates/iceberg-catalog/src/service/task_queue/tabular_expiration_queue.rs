use crate::api::management::v1::{DeleteKind, TabularType};
use crate::api::Result;
use crate::service::task_queue::{Task, TaskQueue};
use crate::service::{Catalog, TableIdentUuid, Transaction};
use crate::WarehouseIdent;
use std::sync::Arc;

use crate::service::task_queue::tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask};
use iceberg_ext::catalog::rest::ErrorModel;
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

// TODO: concurrent workers
pub async fn tabular_expiration_task<C: Catalog>(
    fetcher: Arc<
        dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput>
            + Send
            + Sync
            + 'static,
    >,
    cleaner: Arc<
        dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync + 'static,
    >,
    catalog_state: C::State,
) {
    loop {
        let expiration = match fetcher.pick_new_task().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(expiration) = expiration else {
            continue;
        };

        let span = tracing::debug_span!(
            "tabular_expiration",
            task_name = %expiration.task.task_name,
            tabular_id = %expiration.tabular_id,
            warehouse_id = %expiration.warehouse_ident,
            tabular_type = %expiration.tabular_type,
            deletion_kind = ?expiration.deletion_kind,
            task = ?expiration.task,
        );

        instrumented_expire::<C>(
            fetcher.clone(),
            &cleaner,
            catalog_state.clone(),
            &expiration,
        )
        .instrument(span.or_current())
        .await;

        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn instrumented_expire<C: Catalog>(
    fetcher: Arc<
        dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput> + Send + Sync,
    >,
    cleaner: &Arc<dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync>,
    catalog_state: C::State,
    expiration: &TabularExpirationTask,
) {
    match handle_table::<C>(catalog_state.clone(), cleaner, expiration).await {
        Ok(()) => {
            fetcher.retrying_record_success(&expiration.task).await;
            tracing::info!("Successfully handled table expiration");
        }
        Err(e) => {
            tracing::error!("Failed to handle table expiration: {:?}", e);
            fetcher
                .retrying_record_failure(&expiration.task, &format!("{e:?}"))
                .await;
        }
    };
}

// Loads the table metadata
// Gets the FileIO for the table
// Deletes data files referenced by old metadata
// Deletes the old metadata
// Deletes data files referenced by current table metadata
// Deletes current table metadata
// Drops the table from the catalog
async fn handle_table<C>(
    catalog_state: C::State,
    delete_queue: &Arc<
        dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync + 'static,
    >,
    expiration: &TabularExpirationTask,
) -> Result<()>
where
    C: Catalog,
{
    let mut trx = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start transaction: {:?}", e);
            e
        })?;

    // We need to load the table metadata to get the location and we cannot load the table after
    // dropping it.
    let cleanup_task = maybe_prepare_purge_input::<C>(expiration, &mut trx).await?;

    C::drop_table(
        TableIdentUuid::from(expiration.tabular_id),
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to drop table: {:?}", e);
        e
    })?;

    if let Some(task) = cleanup_task {
        let id = delete_queue.enqueue(task).await?;
        tracing::debug!("Enqueued purge task: {:?}", id);
    }

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    Ok(())
}

async fn maybe_prepare_purge_input<C>(
    TabularExpirationTask {
        deletion_kind,
        tabular_id,
        warehouse_ident,
        tabular_type,
        task,
    }: &TabularExpirationTask,
    trx: &mut C::Transaction,
) -> Result<Option<TabularPurgeInput>>
where
    C: Catalog,
{
    Ok(if matches!(deletion_kind, DeleteKind::Purge) {
        let tabular_location = C::load_tables(
            *warehouse_ident,
            [TableIdentUuid::from(*tabular_id)],
            true,
            trx.transaction(),
        )
        .await
        .map_err(|e| {
            tracing::error!("Failed to load table: {:?}", e);
            e
        })?
        .remove(&TableIdentUuid::from(*tabular_id))
        .ok_or_else(|| {
            tracing::error!("Table not found");
            ErrorModel::internal("Table not found", "InternalDBError", None)
        })?
        .table_metadata
        .location;
        Some(TabularPurgeInput {
            tabular_id: *tabular_id,
            tabular_location,
            warehouse_ident: *warehouse_ident,
            tabular_type: *tabular_type,
            parent_id: Some(task.task_id),
        })
    } else {
        None
    })
}

#[derive(Debug)]
pub struct TabularExpirationTask {
    pub deletion_kind: DeleteKind,
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug)]
pub struct TabularExpirationInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub purge: bool,
}
