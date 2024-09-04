use async_trait::async_trait;
use chrono::Utc;
use sqlx::FromRow;
use std::fmt::Debug;
use std::time::Duration;
use uuid::Uuid;

pub mod delete_queue;
pub mod tabular_expiration_queue;

#[async_trait]
pub trait TaskQueue: std::fmt::Debug {
    type Task: Send + Sync + 'static;
    type Input: Debug + Send + Sync + 'static;

    fn queue_name(&self) -> &'static str;

    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>>;
    async fn record_success(&self, id: Uuid) -> crate::api::Result<()>;
    async fn record_failure(
        &self,
        id: Uuid,
        n_retries: i32,
        error_details: String,
    ) -> crate::api::Result<()>;

    async fn enqueue(&self, task: Self::Input) -> crate::api::Result<Uuid>;
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "sqlx-postgres", derive(FromRow))]
pub struct Task {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub parent_task_id: Option<Uuid>,
    pub attempt: i32,
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_status", rename_all = "kebab-case")
)]
pub enum TaskStatus {
    Pending,
    Finished,
    Running,
    Failed,
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

pub(crate) use unwrap_or_continue;
