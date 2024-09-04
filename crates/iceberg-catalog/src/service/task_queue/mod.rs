use crate::service::task_queue::tabular_expiration_queue::{
    TabularExpirationInput, TabularExpirationTask,
};
use crate::service::task_queue::tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask};
use crate::service::{Catalog, SecretStore};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::FromRow;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

#[derive(Debug, Clone)]
pub struct TaskQueues {
    tabular_expiration: Arc<
        dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput>
            + Send
            + Sync
            + 'static,
    >,
    tabular_purge: Arc<
        dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput> + Send + Sync + 'static,
    >,
}

impl TaskQueues {
    #[must_use]
    pub fn new(
        expiration: Arc<
            dyn TaskQueue<Task = TabularExpirationTask, Input = TabularExpirationInput>
                + Send
                + Sync
                + 'static,
        >,
        purge: Arc<
            dyn TaskQueue<Task = TabularPurgeTask, Input = TabularPurgeInput>
                + Send
                + Sync
                + 'static,
        >,
    ) -> Self {
        Self {
            tabular_expiration: expiration,
            tabular_purge: purge,
        }
    }

    pub(crate) async fn queue_tabular_expiration(
        &self,
        task: TabularExpirationInput,
    ) -> crate::api::Result<Uuid> {
        self.tabular_expiration.enqueue(task).await
    }

    pub(crate) async fn queue_tabular_purge(
        &self,
        task: TabularPurgeInput,
    ) -> crate::api::Result<Uuid> {
        self.tabular_purge.enqueue(task).await
    }

    pub async fn spawn_queues<C, S>(
        &self,
        catalog_state: C::State,
        secret_store: S,
    ) -> Result<(), anyhow::Error>
    where
        C: Catalog,
        S: SecretStore,
    {
        let expiration_queue_handler =
            tokio::task::spawn(tabular_expiration_queue::tabular_expiration_task::<C>(
                self.tabular_expiration.clone(),
                self.tabular_purge.clone(),
                catalog_state.clone(),
            ));

        let purge_queue_handler = tokio::task::spawn(tabular_purge_queue::purge_task::<C, S>(
            self.tabular_purge.clone(),
            catalog_state.clone(),
            secret_store,
        ));

        tokio::select!(
            _ = expiration_queue_handler => {
                tracing::error!("Tabular expiration queue handler exited unexpectedly");
                Err(anyhow::anyhow!("Tabular expiration queue handler exited unexpectedly"))
            },
            _ = purge_queue_handler => {
                tracing::error!("Tabular purge queue handler exited unexpectedly");
                Err(anyhow::anyhow!("Tabular purge queue handler exited unexpectedly"))
            },
        )?;
        Ok(())
    }
}

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
        error_details: &str,
    ) -> crate::api::Result<()>;

    async fn enqueue(&self, task: Self::Input) -> crate::api::Result<Uuid>;

    async fn retrying_record_success(&self, task: &Task) {
        self.retrying_record_success_or_failure(task, SuccessOrFailure::Success)
            .await;
    }

    async fn retrying_record_failure(&self, task: &Task, details: &str) {
        self.retrying_record_success_or_failure(task, SuccessOrFailure::Failure(details))
            .await;
    }

    async fn retrying_record_success_or_failure(&self, task: &Task, result: SuccessOrFailure<'_>) {
        let mut retry = 0;
        while let Err(e) = match result {
            SuccessOrFailure::Success => self.record_success(task.task_id).await,
            SuccessOrFailure::Failure(details) => {
                self.record_failure(task.task_id, 5, details).await
            }
        } {
            tracing::error!("Failed to record {}: {:?}", result.as_str(), e);
            tokio::time::sleep(Duration::from_secs(1 + retry)).await;
            retry += 1;
            if retry > 5 {
                tracing::error!("Giving up trying to record {}.", result.as_str());
                break;
            }
        }
    }
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

#[derive(Debug)]
pub enum SuccessOrFailure<'a> {
    Success,
    Failure(&'a str),
}

impl<'a> SuccessOrFailure<'a> {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            SuccessOrFailure::Success => "success",
            SuccessOrFailure::Failure(_) => "failure",
        }
    }
}
