use crate::api::management::v1::TabularType;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::TabularType as DbTabularType;
use crate::implementations::postgres::DeletionKind;

use crate::implementations::postgres::ReadWrite;
use crate::service::task_queue::tabular_expiration_queue::{
    TabularExpirationInput, TabularExpirationTask,
};
use crate::service::task_queue::tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask};
use crate::service::task_queue::{Task, TaskQueue, TaskStatus};
use crate::WarehouseIdent;
use async_trait::async_trait;
use chrono::Utc;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub(crate) async fn queue_task(
    conn: &mut PgConnection,
    task_name: &str,
    parenet_task_id: Option<Uuid>,
    idempotency_key: Uuid,
    warehouse_ident: WarehouseIdent,
) -> Result<Uuid, IcebergErrorResponse> {
    let task_id = Uuid::now_v7();
    let task_handle = sqlx::query!(
        r#"INSERT INTO task(
                        task_id,
                        task_name,
                        status,
                        parent_task_id,
                        idempotency_key,
                        warehouse_id)
        VALUES ($1, $2, 'pending', $3, $4, $5)
        ON CONFLICT ON CONSTRAINT unique_idempotency_key DO NOTHING
        RETURNING task_id"#,
        task_id,
        task_name,
        parenet_task_id,
        idempotency_key,
        *warehouse_ident
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("fail".into()))?;

    if task_handle.is_none() {
        tracing::info!(
            "Task already exists with idempotency key: {}",
            idempotency_key
        );
        return Ok(task_id);
    }
    Ok(task_id)
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        WITH cte as (
            SELECT attempt >= $2 as should_fail
            FROM task
            WHERE task_id = $1
        )
        UPDATE task
        SET status = CASE WHEN (select should_fail from cte) THEN 'failed'::task_status ELSE 'pending'::task_status END,
            last_error_details = $3
        WHERE task_id = $1
        "#,
        id,
        n_retries,
        details
    )
    .execute(conn)
    .await.map_err(|e| e.into_error_model("fail".into()))?;
    Ok(())
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    name: &'static str,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let x = sqlx::query_as!(
        Task,
        r#"
    WITH updated_task AS (
        SELECT task_id
        FROM task
        WHERE status = 'pending' AND task_name = $1
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE task
    SET status = 'running', picked_up_at = $2, attempt = task.attempt + 1
    FROM updated_task
    WHERE task.task_id = updated_task.task_id
    RETURNING task.task_id, task.status as "status: TaskStatus", task.picked_up_at, task.attempt, task.parent_task_id, task.task_name
    "#,
        name,
        Utc::now()
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!(?e, "Failed to pick a task");
        e.into_error_model(format!("Failed to pick a '{name}' task")) })?;

    if let Some(task) = x.as_ref() {
        tracing::info!("Picked up task: {:?}", task);
    }

    Ok(x)
}

#[derive(Debug, Clone)]
pub struct ExpirationTaskFetcher {
    pub read_write: ReadWrite,
}

#[derive(Debug, Clone)]
pub struct TabularPurgeTaskFetcher {
    pub read_write: ReadWrite,
}

#[async_trait]
impl TaskQueue for TabularPurgeTaskFetcher {
    type Task = TabularPurgeTask;
    type Input = TabularPurgeInput;

    fn queue_name(&self) -> &'static str {
        "tabular_purges"
    }

    #[tracing::instrument(skip(self))]
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>> {
        let task = pick_task(&self.read_write.write_pool, self.queue_name()).await?;

        let Some(task) = task else {
            tracing::info!("No task found");
            return Ok(None);
        };

        let purge = sqlx::query!(
            r#"
            SELECT tabular_id, tabular_location, warehouse_id, typ as "tabular_type: DbTabularType"
            FROM tabular_purges
            WHERE task_id = $1
            "#,
            task.task_id
        )
        .fetch_one(&self.read_write.read_pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "error selecting tabular expiration");
            // TODO: should we reset task status here?
            e.into_error_model("failed to read task after picking one up".into())
        })?;

        tracing::info!("Purge task: {:?}", purge);
        Ok(Some(TabularPurgeTask {
            tabular_id: purge.tabular_id,
            tabular_location: purge.tabular_location,
            warehouse_ident: purge.warehouse_id.into(),
            tabular_type: purge.tabular_type.into(),
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.read_write.write_pool).await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        n_retries: i32,
        error_details: &str,
    ) -> crate::api::Result<()> {
        record_failure(&self.read_write.write_pool, id, n_retries, error_details).await
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue(
        &self,
        TabularPurgeInput {
            tabular_location,
            tabular_id,
            warehouse_ident,
            tabular_type,
            parent_id,
        }: TabularPurgeInput,
    ) -> crate::api::Result<Uuid> {
        let mut transaction = self
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;

        tracing::info!(
            "Queuing expiration for '{tabular_id}' of type: '{}' under warehouse: '{warehouse_ident}'",
            tabular_type.to_string(),
        );

        let idempotency_key = Uuid::new_v5(&warehouse_ident, tabular_id.as_bytes());

        let task_id = queue_task(
            &mut transaction,
            self.queue_name(),
            parent_id,
            idempotency_key,
            warehouse_ident,
        )
        .await?;

        let it = sqlx::query!(
                "INSERT INTO tabular_purges(task_id, tabular_id, warehouse_id, typ, tabular_location) VALUES ($1, $2, $3, $4, $5) RETURNING task_id",
                task_id,
                tabular_id,
                *warehouse_ident,
                match tabular_type {
                    TabularType::Table => DbTabularType::Table,
                    TabularType::View => DbTabularType::View,
                } as _,
                tabular_location,
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!(?e, "failed to insert into tabular_purges");
                e.into_error_model("fail".into())
            })?;

        match it {
            Some(row) => tracing::info!("Queued purge task: {:?}", row),
            None => {
                tracing::info!("Purge task already exists.");
            }
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("fail".into())
        })?;

        Ok(task_id)
    }
}

#[async_trait]
impl TaskQueue for ExpirationTaskFetcher {
    type Task = TabularExpirationTask;
    type Input = TabularExpirationInput;

    fn queue_name(&self) -> &'static str {
        "tabular_expiration"
    }

    #[tracing::instrument(skip(self))]
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>> {
        let task = pick_task(&self.read_write.write_pool, self.queue_name()).await?;

        let Some(task) = task else {
            tracing::info!("No task found");
            return Ok(None);
        };

        let expiration = sqlx::query!(
            r#"
            SELECT tabular_id, warehouse_id, typ as "tabular_type: DbTabularType", deletion_kind as "deletion_kind: DeletionKind"
            FROM tabular_expirations
            WHERE task_id = $1
            "#,
            task.task_id
        )
        .fetch_one(&self.read_write.read_pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "error selecting tabular expiration");
            // TODO: should we reset task status here?
            e.into_error_model("failed to read task after picking one up".into())
        })?;

        tracing::info!("Expiration task: {:?}", expiration);
        Ok(Some(TabularExpirationTask {
            deletion_kind: expiration.deletion_kind.into(),
            tabular_id: expiration.tabular_id,
            warehouse_ident: expiration.warehouse_id.into(),
            tabular_type: expiration.tabular_type.into(),
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.read_write.write_pool).await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        n_retries: i32,
        error_details: &str,
    ) -> crate::api::Result<()> {
        record_failure(&self.read_write.write_pool, id, n_retries, error_details).await
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue(
        &self,
        TabularExpirationInput {
            tabular_id,
            warehouse_ident,
            tabular_type,
            purge,
        }: TabularExpirationInput,
    ) -> crate::api::Result<Uuid> {
        let mut transaction = self
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;

        tracing::info!(
            "Queuing expiration for '{tabular_id}' of type: '{}' under warehouse: '{warehouse_ident}'",
            tabular_type.to_string(),
        );

        let idempotency_key = Uuid::new_v5(&warehouse_ident, tabular_id.as_bytes());

        let task_id = queue_task(
            &mut transaction,
            self.queue_name(),
            None,
            idempotency_key,
            warehouse_ident,
        )
        .await?;

        let it = sqlx::query!(
            "INSERT INTO tabular_expirations(task_id, tabular_id, warehouse_id, typ, deletion_kind) VALUES ($1, $2, $3, $4, $5) RETURNING task_id",
            task_id,
            tabular_id,
            *warehouse_ident,
            match tabular_type {
                TabularType::Table => DbTabularType::Table,
                TabularType::View => DbTabularType::View,
            } as _,
            if purge {
                DeletionKind::Purge
            } else {
                DeletionKind::Default
            } as _
        )

        .fetch_optional(&mut *transaction)
        .await
        .map_err(|e| {
            tracing::error!(?e, "failed to insert into tabular_expirations");
            e.into_error_model("fail".into()) })?;

        match it {
            Some(row) => tracing::info!("Queued expiration task: {:?}", row),
            None => {
                tracing::info!("Expiration task already exists.");
            }
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("fail".into())
        })?;

        Ok(task_id)
    }
}

async fn record_success(id: Uuid, pool: &PgPool) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        UPDATE task
        SET status = 'done'
        WHERE task_id = $1
        "#,
        id
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("fail".into()))?;
    Ok(())
}
