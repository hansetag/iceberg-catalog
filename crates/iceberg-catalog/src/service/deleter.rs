use crate::api::management::v1::TabularType;
use crate::api::Result;
use crate::catalog::maybe_get_secret;
use crate::implementations::postgres::task_runner::{Deletion, TableExpirationTask, Task};
use crate::service::{Catalog, DropFlags, SecretStore, TableIdentUuid, Transaction};
use crate::WarehouseIdent;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::ErrorModel;
use std::time::Duration;
use uuid::Uuid;

macro_rules! unwrap_or_continue {
    ($cont:expr, $err_msg:expr, $fetcher:expr, $task:expr) => {
        match $cont {
            Ok(x) => x,
            Err(e) => {
                tracing::error!($err_msg, e);
                retrying_record_failure(&$fetcher, &$task, format!("{e:?}")).await;
                continue;
            }
        }
    };
}

async fn retrying_record_failure(fetcher: &impl TaskQueue, task: &Task, details: String) {
    let mut retry = 0;
    while let Err(e) = fetcher
        .record_failure(task.task_id, 5, details.clone())
        .await
    {
        tracing::error!("Failed to record failure: {:?}", e);
        tokio::time::sleep(Duration::from_secs(1 + retry)).await;
        retry += 1;
        if retry > 5 {
            tracing::error!("Giving up trying to record failure.");
            break;
        }
    }
}

#[async_trait]
pub trait TaskQueue: std::fmt::Debug {
    type Task: Send + Sync + 'static;
    type Input: Send + Sync + 'static;

    fn queue_name(&self) -> &'static str;
    async fn poll(&self) -> Result<Option<Self::Task>>;
    async fn record_success(&self, id: Uuid) -> Result<()>;
    async fn record_failure(&self, id: Uuid, n_retries: i32, error_details: String) -> Result<()>;
    async fn enqueue(
        &self,
        task: Self::Input,
        // TODO: taking transaction here enables us to have all-or-nothing semantics for enqueuing
        //       not sure if all possible backends support this though
    ) -> Result<Uuid>;
}

#[derive(Debug)]
pub struct DeleteInput {
    /// The `entity_id` to which the deletion belongs, e.g. a `table_id`
    pub entity_id: Uuid,
    /// The `warehouse_id` under which the entity lives
    pub warehouse_id: WarehouseIdent,
    /// The `location` to delete
    pub location: String,
    /// The `parent_task`, if any, e.g. a table expiration task which spawned a number of deletions
    pub parent_task: Option<Uuid>,
}

pub async fn tabular_expiration_task<
    F: TaskQueue<Task = TableExpirationTask> + Send + Sync + 'static,
    D: TaskQueue<Task = Deletion, Input = DeleteInput> + Send + Sync + 'static,
    C: Catalog,
    S: SecretStore,
>(
    fetcher: F,
    delete_queue: D,
    catalog_state: C::State,
    secret_state: S,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let expiration = match fetcher.poll().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch deletion: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let Some(TableExpirationTask {
            tabular_id,
            warehouse_ident,
            tabular_type,
            task,
        }) = expiration
        else {
            continue;
        };
        tracing::info!("Got expiration: tabular_id: '{tabular_id}', whident: '{warehouse_ident}', typ: '{tabular_type}', task: '{task:?}'");

        let span = tracing::span!(
            tracing::Level::INFO,
            "expiring",
            tabular_id = %tabular_id,
            task = ?task,
        );
        let _entered = span.enter();
        let mut trx = unwrap_or_continue!(
            C::Transaction::begin_write(catalog_state.clone()).await,
            "Failed to start transaction: {:?}",
            fetcher,
            &task
        );

        match tabular_type {
            TabularType::Table => {
                tracing::info!("Table");
                let mut table_metadata = unwrap_or_continue!(
                    C::load_tables(
                        warehouse_ident,
                        [TableIdentUuid::from(tabular_id)],
                        true,
                        trx.transaction(),
                    )
                    .await,
                    "Failed to load tables: {:?}",
                    fetcher,
                    &task
                );
                let table_metadata = table_metadata.remove(&tabular_id.into());
                let Some(table_metadata) = table_metadata else {
                    tracing::error!("Table not found: '{tabular_id}'",);
                    continue;
                };

                let warehouse = unwrap_or_continue!(
                    C::get_warehouse(warehouse_ident, trx.transaction()).await,
                    "Failed to get warehouse: {:?}",
                    fetcher,
                    &task
                );

                let secret = unwrap_or_continue!(
                    maybe_get_secret(warehouse.storage_secret_id, &secret_state).await,
                    "Failed to get secret: {:?}",
                    fetcher,
                    &task
                );

                let fio = unwrap_or_continue!(
                    warehouse.storage_profile.file_io(secret.as_ref()),
                    "Failed to get storage profile: {:?}",
                    fetcher,
                    &task
                );
                tracing::info!("Got fio");
                unwrap_or_continue!(
                    handle_old_metadata(
                        warehouse_ident,
                        &fio,
                        &table_metadata.table_metadata,
                        &delete_queue,
                    )
                    .await,
                    "{:?}",
                    fetcher,
                    &task
                );
                let location = unwrap_or_continue!(
                    table_metadata.metadata_location.as_deref().ok_or_else(|| {
                        ErrorModel::internal(
                            "Table metadata location not found",
                            "MetadataLocationNotFound",
                            None,
                        )
                    }),
                    "{:?}",
                    fetcher,
                    &task
                );
                unwrap_or_continue!(
                    find_files(
                        &table_metadata.table_metadata,
                        location,
                        warehouse_ident,
                        &delete_queue,
                        &fio,
                    )
                    .await,
                    "{:?}",
                    fetcher,
                    &task
                );

                unwrap_or_continue!(
                    C::drop_table(
                        tabular_id.into(),
                        DropFlags {
                            hard_delete: true,
                            // Purge doesn't matter, we're deleting the table
                            purge: true,
                        },
                        trx.transaction(),
                    )
                    .await,
                    "Failed to drop table: {:?}",
                    fetcher,
                    &task
                );

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

        unwrap_or_continue!(
            trx.commit().await,
            "Failed to commit transaction: {:?}",
            fetcher,
            &task
        );
    }
}

async fn handle_old_metadata(
    warehouse_id: WarehouseIdent,
    fio: &FileIO,
    meta: &TableMetadata,
    fetcher: &impl TaskQueue<Task = Deletion, Input = DeleteInput>,
) -> Result<()> {
    for old_meta in &meta.metadata_log {
        // old metadata doesnt exist in db hence we're going to s3
        let input = fio
            .new_input(old_meta.metadata_file.as_str())
            .map_err(|e| {
                ErrorModel::internal("Failed to create input", "FileIOError", Some(Box::new(e)))
            })?;
        let metadata = input.read().await.map_err(|e| {
            ErrorModel::internal("Failed to read input", "FileIOError", Some(Box::new(e)))
        })?;
        let meta = serde_json::from_slice::<TableMetadata>(&metadata).map_err(|e| {
            ErrorModel::internal(
                "Failed to deserialize metadata",
                "SerdeJsonError",
                Some(Box::new(e)),
            )
        })?;

        find_files::<_>(
            &meta,
            old_meta.metadata_file.as_str(),
            warehouse_id,
            fetcher,
            fio,
        )
        .await?;
    }
    Ok(())
}

async fn find_files<D: TaskQueue<Task = Deletion, Input = DeleteInput>>(
    metadata: &TableMetadata,
    metadata_location: &str,
    warehouse_ident: WarehouseIdent,
    conn: &D,
    fio: &FileIO,
) -> Result<()> {
    for file in metadata.snapshots.values() {
        let list = file.load_manifest_list(fio, metadata).await.map_err(|e| {
            ErrorModel::internal(
                "Failed to load manifest list",
                "FileIOError",
                Some(Box::new(e)),
            )
        })?;

        for fi in list.entries() {
            let man = fi.load_manifest(fio).await.map_err(|e| {
                ErrorModel::internal("Failed to load manifest", "FileIOError", Some(Box::new(e)))
            })?;
            for e in man.entries() {
                conn.enqueue(DeleteInput {
                    entity_id: metadata.table_uuid,
                    location: e.data_file().file_path().to_string(),
                    warehouse_id: warehouse_ident,
                    parent_task: None,
                })
                .await?;
            }
            tracing::info!("fi manifest_path: {}", fi.manifest_path);

            conn.enqueue(DeleteInput {
                entity_id: metadata.table_uuid,
                location: fi.manifest_path.clone(),
                warehouse_id: warehouse_ident,
                parent_task: None,
            })
            .await?;
        }
        tracing::info!("fi manifest_list: {}", file.manifest_list());

        conn.enqueue(DeleteInput {
            entity_id: metadata.table_uuid,
            location: file.manifest_list().to_string(),
            warehouse_id: warehouse_ident,
            parent_task: None,
        })
        .await?;

        // TODO: delete statistics files
    }

    conn.enqueue(DeleteInput {
        entity_id: metadata.table_uuid,
        location: metadata_location.to_string(),
        warehouse_id: warehouse_ident,
        parent_task: None,
    })
    .await?;

    Ok(())
}

pub async fn delete_task<
    F: TaskQueue<Task = Deletion> + Send + Sync + 'static,
    C: Catalog,
    S: SecretStore,
>(
    fetcher: F,
    conn: C::State,
    secret_state: S,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(7)).await;

        // TODO: does the FOR UPDATE lock last beyond the await? Probably not?
        //       maybe all of this needs to happen in a transaction.
        let deletion = match fetcher.poll().await {
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

        tracing::info!("Got deletion: {:?}", deletion);

        let span = tracing::span!(tracing::Level::INFO, "deleting", id = %deletion.task.task_id, location = %deletion.location, attempt = %deletion.task.attempt, warehouse_id = %deletion.warehouse_id);
        let _entered = span.enter();
        let warehouse = deletion.warehouse_id;

        let mut t = unwrap_or_continue!(
            C::Transaction::begin_write(conn.clone()).await,
            "Failed to start transaction: {:?}",
            fetcher,
            &deletion.task
        );

        let warehouse = unwrap_or_continue!(
            C::get_warehouse(warehouse.into(), t.transaction()).await,
            "Failed to get warehouse: {:?}",
            fetcher,
            &deletion.task
        );
        unwrap_or_continue!(
            t.commit().await,
            "Failed to commit transaction: {:?}",
            fetcher,
            &deletion.task
        );

        let secret = unwrap_or_continue!(
            maybe_get_secret(warehouse.storage_secret_id, &secret_state).await,
            "Failed to get secret: {:?}",
            fetcher,
            &deletion.task
        );

        let prof = unwrap_or_continue!(
            warehouse.storage_profile.file_io(secret.as_ref()),
            "Failed to get storage profile: {:?}",
            fetcher,
            &deletion.task
        );

        unwrap_or_continue!(
            prof.delete(&deletion.location).await,
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
