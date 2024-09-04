use crate::api::Result;
use crate::catalog::maybe_get_secret;
use crate::service::task_queue::{retrying_record_failure, unwrap_or_continue, Task, TaskQueue};
use crate::service::{Catalog, SecretStore, Transaction};
use crate::WarehouseIdent;
use iceberg::io::FileIO;
use std::time::Duration;
use uuid::Uuid;

// TODO: concurrent workers
pub async fn delete_queue<
    F: TaskQueue<Task = Deletion, Input = DeleteInput> + Send + Sync + 'static,
    C: Catalog,
    S: SecretStore,
>(
    fetcher: F,
    conn: C::State,
    secret_state: S,
) {
    loop {
        let span = tracing::span!(
            tracing::Level::INFO,
            "deleting",
            id = tracing::field::Empty,
            location = tracing::field::Empty,
            attempt = tracing::field::Empty,
            warehouse_id = tracing::field::Empty
        );
        let _entered = span.enter();

        // TODO: make this configurable
        tokio::time::sleep(Duration::from_secs(7)).await;

        let deletion = match fetcher.pick_new_task().await {
            Ok(deletion) => deletion,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(deletion) = deletion else {
            continue;
        };

        span.record("id", deletion.entity_id.to_string().as_str());
        span.record("location", deletion.location.as_str());
        span.record("attempt", deletion.task.attempt);
        span.record("warehouse_id", deletion.warehouse_id.to_string().as_str());
        span.record("task", format!("{:?}", deletion.task));

        tracing::info!("Got deletion");

        let warehouse = deletion.warehouse_id;

        let file_io = unwrap_or_continue!(
            get_file_io::<C, S>(&conn, secret_state.clone(), warehouse).await,
            "Failed to get file io: {:?}",
            fetcher,
            &deletion.task
        );

        unwrap_or_continue!(
            file_io.delete(&deletion.location).await,
            "Failed to delete: {:?}",
            fetcher,
            &deletion.task
        );

        tracing::info!("Deleted task: {:?}", deletion);
        let mut retry = 0;
        while let Err(e) = fetcher.record_success(deletion.task.task_id).await {
            tracing::error!("Failed to record success: {:?}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
            retry += 1;
            if retry > 5 {
                tracing::error!("Giving up trying to record success.");
                break;
            }
        }
    }
}

async fn get_file_io<C: Catalog, S: SecretStore>(
    conn: &C::State,
    secret_state: S,
    warehouse: Uuid,
) -> Result<FileIO> {
    let mut t = C::Transaction::begin_write(conn.clone()).await?;
    let warehouse = C::get_warehouse(warehouse.into(), t.transaction()).await?;
    t.commit().await?;
    let secret = maybe_get_secret(warehouse.storage_secret_id, &secret_state).await?;

    Ok(warehouse.storage_profile.file_io(secret.as_ref())?)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Deletion {
    pub entity_id: Uuid,
    pub location: String,
    pub warehouse_id: Uuid,
    pub task: Task,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteInput {
    pub entity_id: Uuid,
    pub location: String,
    pub warehouse_id: WarehouseIdent,
    pub parent_task: Option<Uuid>,
}
