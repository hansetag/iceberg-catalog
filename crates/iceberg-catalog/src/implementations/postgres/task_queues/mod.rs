mod tabular_expiration_queue;
mod tabular_purge_queue;

use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::ReadWrite;
use crate::service::task_queue::{Task, TaskQueueConfig, TaskStatus};
use crate::WarehouseIdent;
pub use tabular_expiration_queue::TabularExpirationQueue;
pub use tabular_purge_queue::TabularPurgeQueue;

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

macro_rules! impl_pg_task_queue {
    ($name:ident) => {
        use crate::implementations::postgres::task_queues::PgQueue;
        use crate::implementations::postgres::ReadWrite;

        #[derive(Debug, Clone)]
        pub struct $name {
            pg_queue: PgQueue,
        }

        impl $name {
            #[must_use]
            pub fn new(read_write: ReadWrite) -> Self {
                Self {
                    pg_queue: PgQueue::new(read_write),
                }
            }

            /// Create a new `$name` with the default configuration.
            ///
            /// # Errors
            /// Returns an error if the max age duration is invalid.
            pub fn from_config(
                read_write: ReadWrite,
                config: TaskQueueConfig,
            ) -> anyhow::Result<Self> {
                Ok(Self {
                    pg_queue: PgQueue::from_config(read_write, config)?,
                })
            }
        }
    };
}
use impl_pg_task_queue;

#[derive(Debug, Clone)]
struct PgQueue {
    pub read_write: ReadWrite,
    pub config: TaskQueueConfig,
    pub max_age: sqlx::postgres::types::PgInterval,
}

impl PgQueue {
    fn new(read_write: ReadWrite) -> Self {
        let config = TaskQueueConfig::default();
        let microseconds = config
            .max_age
            .num_microseconds()
            .expect("Invalid max age duration for task queues hard-coded in Default.");
        Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds, // 3 seconds
            },
        }
    }

    fn from_config(read_write: ReadWrite, config: TaskQueueConfig) -> anyhow::Result<Self> {
        let microseconds = config
            .max_age
            .num_microseconds()
            .ok_or(anyhow::anyhow!("Invalid max age duration for task queues."))?;
        Ok(Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds,
            },
        })
    }
}

async fn queue_task(
    conn: &mut PgConnection,
    task_name: &str,
    parenet_task_id: Option<Uuid>,
    idempotency_key: Uuid,
    warehouse_ident: WarehouseIdent,
    suspend_until: Option<DateTime<Utc>>,
) -> Result<Option<Uuid>, IcebergErrorResponse> {
    let task_id = Uuid::now_v7();
    Ok(sqlx::query_scalar!(
        r#"INSERT INTO task(
                        task_id,
                        task_name,
                        status,
                        parent_task_id,
                        idempotency_key,
                        warehouse_id,
                        suspend_until)
        VALUES ($1, $2, 'pending', $3, $4, $5, $6)
        ON CONFLICT ON CONSTRAINT unique_idempotency_key DO NOTHING
        RETURNING task_id"#,
        task_id,
        task_name,
        parenet_task_id,
        idempotency_key,
        *warehouse_ident,
        suspend_until
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("fail".into()))?)
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
    max_age: &sqlx::postgres::types::PgInterval,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let x = sqlx::query_as!(
        Task,
        r#"
    WITH updated_task AS (
        SELECT task_id
        FROM task
        WHERE (status = 'pending' AND task_name = $1 AND ((suspend_until < now() AT TIME ZONE 'UTC') OR (suspend_until IS NULL)))
                OR (status = 'running' AND (now() - picked_up_at) > $3)
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
        Utc::now(),
        max_age,
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

#[cfg(test)]
mod test {
    use super::*;

    use crate::WarehouseIdent;
    use sqlx::PgPool;
    use uuid::Uuid;
    const TEST_WAREHOUSE: WarehouseIdent = WarehouseIdent(Uuid::nil());

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let idempotency_key = Uuid::new_v5(&TEST_WAREHOUSE, b"test");

        let id = queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap();

        assert!(queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .is_none());

        let id3 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) fn setup(pool: PgPool, config: TaskQueueConfig) -> PgQueue {
        PgQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), config).unwrap()
    }

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);
        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");

        record_failure(&pool, id, 5, "test").await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");

        record_failure(&pool, id, 2, "test").await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");

        record_success(id, &pool).await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            Some(Utc::now() + chrono::Duration::milliseconds(500)),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_age: chrono::Duration::milliseconds(500),
            ..Default::default()
        };
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", &queue.max_age)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.task_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
        assert_eq!(&task2.task_name, "test");

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();
    }
}
