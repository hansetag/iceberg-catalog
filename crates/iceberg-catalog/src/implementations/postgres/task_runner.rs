use crate::api::management::v1::TabularType;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::TabularType as DbTabularType;

use crate::service::task_queue::delete_queue::{DeleteInput, Deletion};
use crate::service::task_queue::tabular_expiration_queue::{ExpirationInput, TableExpirationTask};
use crate::service::task_queue::{Task, TaskQueue, TaskStatus};
use crate::WarehouseIdent;
use async_trait::async_trait;
use chrono::Utc;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

pub(crate) async fn queue_task(
    conn: &mut PgConnection,
    task_name: &str,
    parenet_task_id: Option<Uuid>,
    idempotency_key: Uuid,
) -> Result<Uuid, IcebergErrorResponse> {
    let task_id = Uuid::now_v7();
    let task_handle = sqlx::query!(
        r#"INSERT INTO task(
                        task_id,
                        task_name,
                        status,
                        parent_task_id,
                        idempotency_key)
        VALUES ($1, $2, 'pending', $3, $4)
        ON CONFLICT ON CONSTRAINT unique_idempotency_key DO NOTHING
        RETURNING task_id"#,
        task_id,
        task_name,
        parenet_task_id,
        idempotency_key
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

pub(crate) async fn queue_delete(
    conn: &mut PgConnection,
    DeleteInput {
        entity_id,
        location,
        warehouse_id,
        parent_task,
    }: DeleteInput,
    task_name: &'static str,
) -> Result<Uuid, IcebergErrorResponse> {
    tracing::info!(
        "Queuing delete for '{location}' under entity_id: {}",
        entity_id
    );

    let idempotency_key = Uuid::new_v5(&warehouse_id, location.as_bytes());

    let task_id = queue_task(conn, task_name, parent_task, idempotency_key).await?;

    let it = sqlx::query!(
        r#"
        INSERT INTO deletions (task_id, entity_id, location, warehouse_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT ON CONSTRAINT unique_location_per_warehouse DO NOTHING
        RETURNING task_id
        "#,
        task_id,
        entity_id,
        location,
        *warehouse_id,
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("fail".into()))?;

    match it {
        Some(row) => tracing::info!("Queued delete task: {:?}", row),
        None => {
            tracing::info!("Delete task already exists for location: {}", location);
        }
    }
    Ok(task_id)
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: String,
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
    conn: &mut PgConnection,
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
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| e.into_error_model("fail".into()))?;

    if let Some(deletion) = x.as_ref() {
        tracing::info!("Picked up task: {:?}", deletion);
    }

    Ok(x)
}

#[derive(Debug, Clone)]
pub struct DeleteTaskFetcher {
    pub pool: PgPool,
}

#[async_trait]
impl TaskQueue for DeleteTaskFetcher {
    type Task = Deletion;
    type Input = DeleteInput;

    fn queue_name(&self) -> &'static str {
        "deletion"
    }

    async fn poll(&self) -> crate::api::Result<Option<Self::Task>> {
        let mut conn = self
            .pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;
        let task = pick_task(&mut conn, self.queue_name()).await?;

        let Some(task) = task else {
            // this commit is probably unnecessary, but it's here to be safe
            conn.commit()
                .await
                .map_err(|e| e.into_error_model("fail".into()))?;
            tracing::info!("No task found: {}", self.queue_name());
            return Ok(None);
        };

        let deletion = sqlx::query!(
            r#"
            SELECT entity_id, location, warehouse_id
            FROM deletions
            WHERE task_id = $1
            "#,
            task.task_id
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| e.into_error_model("fail".into()))?;

        conn.commit()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;
        tracing::info!("Deletion task: {:?}", deletion);
        Ok(Some(Deletion {
            entity_id: deletion.entity_id,
            location: deletion.location,
            warehouse_id: deletion.warehouse_id,
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.pool).await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        n_retries: i32,
        error_details: String,
    ) -> crate::api::Result<()> {
        record_failure(&self.pool, id, n_retries, error_details).await
    }

    async fn enqueue(&self, task: Self::Input) -> crate::api::Result<Uuid> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!(?e, "failed to begin transaction");
            e.into_error_model("fail".into())
        })?;
        let task_id = queue_delete(&mut conn, task, self.queue_name())
            .await
            .map_err(|e| {
                tracing::error!(?e, "failed to queue delete");
                e
            })?;
        conn.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("fail".into())
        })?;
        Ok(task_id)
    }
}

#[derive(Debug, Clone)]
pub struct ExpirationTaskFetcher {
    pub pool: PgPool,
}

#[async_trait]
#[async_trait]
impl TaskQueue for ExpirationTaskFetcher {
    type Task = TableExpirationTask;
    type Input = ExpirationInput;

    fn queue_name(&self) -> &'static str {
        "expiration"
    }

    async fn poll(&self) -> crate::api::Result<Option<Self::Task>> {
        let mut conn = self
            .pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;
        let task = pick_task(&mut conn, self.queue_name()).await?;

        let Some(task) = task else {
            tracing::info!("No task found");
            // this commit is probably unnecessary, but it's here to be safe
            conn.commit()
                .await
                .map_err(|e| e.into_error_model("fail".into()))?;
            return Ok(None);
        };

        let deletion = sqlx::query!(
            r#"
            SELECT tabular_id, warehouse_id, typ as "tabular_type: DbTabularType"
            FROM tabular_expirations
            WHERE task_id = $1
            "#,
            task.task_id
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| {
            tracing::error!(?e, "error selecting tab expir");
            e.into_error_model("fail".into())
        })?;

        conn.commit()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;
        tracing::info!("Expiration task: {:?}", deletion);
        Ok(Some(TableExpirationTask {
            tabular_id: deletion.tabular_id,
            warehouse_ident: deletion.warehouse_id.into(),
            tabular_type: deletion.tabular_type.into(),
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.pool).await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        n_retries: i32,
        error_details: String,
    ) -> crate::api::Result<()> {
        record_failure(&self.pool, id, n_retries, error_details).await
    }

    async fn enqueue(
        &self,
        ExpirationInput {
            tabular_id,
            warehouse_ident,
            tabular_type,
        }: ExpirationInput,
    ) -> crate::api::Result<Uuid> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("fail".into()))?;

        tracing::info!(
            "Queuing expiration for '{tabular_id}' of type: '{}' under warehouse: '{warehouse_ident}'",
            tabular_type.to_string(),
        );

        let idempotency_key = Uuid::new_v5(&warehouse_ident, tabular_id.as_bytes());

        let task_id =
            queue_task(&mut transaction, self.queue_name(), None, idempotency_key).await?;

        let it = sqlx::query!(
            "INSERT INTO tabular_expirations(task_id, tabular_id, warehouse_id, typ) VALUES ($1, $2, $3, $4) RETURNING task_id",
            task_id,
            tabular_id,
            *warehouse_ident,
            match tabular_type {
                TabularType::Table => DbTabularType::Table,
                TabularType::View => DbTabularType::View,
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
