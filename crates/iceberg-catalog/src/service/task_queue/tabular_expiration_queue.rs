use crate::api::management::v1::TabularType;
use crate::api::Result;
use crate::catalog::maybe_get_secret;
use crate::service::task_queue::delete_queue::{DeleteInput, Deletion};
use crate::service::task_queue::{retrying_record_failure, unwrap_or_continue, Task, TaskQueue};
use crate::service::{
    Catalog, DropFlags, GetTableMetadataResponse, LoadTableResponse, SecretStore, TableIdentUuid,
    Transaction,
};
use crate::WarehouseIdent;
use async_trait::async_trait;
use chrono::Utc;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::FromRow;
use std::time::Duration;
use uuid::Uuid;

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

        match tabular_type {
            TabularType::Table => {
                tracing::info!("Table");

                if let Err(err) = handle_table::<C, D, S>(
                    warehouse_ident,
                    tabular_id,
                    &secret_state,
                    &delete_queue,
                    task.task_id,
                    catalog_state.clone(),
                )
                .await
                {
                    tracing::error!("Failed to expire table: {}", err.error);
                    retrying_record_failure(&fetcher, &task, err.error.to_string()).await;
                    continue;
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
async fn handle_table<C, D, S>(
    warehouse_ident: WarehouseIdent,
    tabular_id: Uuid,
    secret_state: &S,
    delete_queue: &D,
    task_id: Uuid,
    catalog_state: C::State,
) -> Result<()>
where
    C: Catalog,
    D: TaskQueue<Task = Deletion, Input = DeleteInput> + Send + Sync + 'static,
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

    let table_metadata = table_metadata.remove(&tabular_id.into());
    let Some(table_metadata) = table_metadata else {
        tracing::error!("Table not found: '{tabular_id}'");
        return Err(ErrorModel::internal("Table not found", "TableNotFound", None).into());
    };

    let warehouse = C::get_warehouse(warehouse_ident, trx.transaction())
        .await
        .map_err(|e| {
            tracing::error!("Failed to get warehouse: {:?}", e);
            e
        })?;

    let secret = maybe_get_secret(warehouse.storage_secret_id, secret_state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get secret: {:?}", e);
            e
        })?;

    let fio = warehouse
        .storage_profile
        .file_io(secret.as_ref())
        .map_err(|e| {
            tracing::error!("Failed to get storage profile: {:?}", e);
            e
        })?;
    tracing::debug!("Got FileIO");

    handle_old_metadata(
        warehouse_ident,
        &fio,
        &table_metadata.table_metadata,
        delete_queue,
        task_id,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to handle old metadata: {:?}", e);
        e
    })?;

    let location = table_metadata.metadata_location.as_deref().ok_or_else(|| {
        let err = ErrorModel::internal(
            "Table metadata location not found",
            "MetadataLocationNotFound",
            None,
        );
        tracing::error!("Table metadata location not found: {:?}", err);
        err
    })?;

    find_files(
        &table_metadata.table_metadata,
        location,
        warehouse_ident,
        delete_queue,
        &fio,
        task_id,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to find files: {:?}", e);
        e
    })?;

    C::drop_table(
        tabular_id.into(),
        DropFlags {
            hard_delete: true,
            purge: true,
        },
        trx.transaction(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to drop table: {:?}", e);
        e
    })?;

    trx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        e
    })?;

    Ok(())
}

async fn handle_old_metadata(
    warehouse_id: WarehouseIdent,
    fio: &FileIO,
    meta: &TableMetadata,
    fetcher: &impl TaskQueue<Task = Deletion, Input = DeleteInput>,
    task_id: Uuid,
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
            task_id,
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
    task_id: Uuid,
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
                    parent_task: Some(task_id),
                })
                .await?;
            }
            tracing::info!("fi manifest_path: {}", fi.manifest_path);

            conn.enqueue(DeleteInput {
                entity_id: metadata.table_uuid,
                location: fi.manifest_path.clone(),
                warehouse_id: warehouse_ident,
                parent_task: Some(task_id),
            })
            .await?;
        }
        tracing::info!("fi manifest_list: {}", file.manifest_list());

        conn.enqueue(DeleteInput {
            entity_id: metadata.table_uuid,
            location: file.manifest_list().to_string(),
            warehouse_id: warehouse_ident,
            parent_task: Some(task_id),
        })
        .await?;

        // TODO: delete statistics files
    }

    conn.enqueue(DeleteInput {
        entity_id: metadata.table_uuid,
        location: metadata_location.to_string(),
        warehouse_id: warehouse_ident,
        parent_task: Some(task_id),
    })
    .await?;

    Ok(())
}

#[derive(Debug)]
pub struct TableExpirationTask {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
    pub task: Task,
}

#[derive(Debug)]
pub struct ExpirationInput {
    pub tabular_id: Uuid,
    pub warehouse_ident: WarehouseIdent,
    pub tabular_type: TabularType,
}
