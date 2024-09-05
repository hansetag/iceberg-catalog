use crate::service::task_queue::tabular_expiration_queue::TabularExpirationInput;
use crate::service::task_queue::tabular_purge_queue::TabularPurgeInput;
use crate::service::{Catalog, SecretStore};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::FromRow;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

#[derive(Debug, Clone)]
pub struct TaskQueues {
    tabular_expiration: tabular_expiration_queue::ExpirationQueue,
    tabular_purge: tabular_purge_queue::TabularPurgeQueue,
}

impl TaskQueues {
    #[must_use]
    pub fn new(
        expiration: tabular_expiration_queue::ExpirationQueue,
        purge: tabular_purge_queue::TabularPurgeQueue,
    ) -> Self {
        Self {
            tabular_expiration: expiration,
            tabular_purge: purge,
        }
    }

    pub(crate) async fn queue_tabular_expiration(
        &self,
        task: TabularExpirationInput,
    ) -> crate::api::Result<()> {
        self.tabular_expiration.enqueue(task).await
    }

    pub(crate) async fn queue_tabular_purge(
        &self,
        task: TabularPurgeInput,
    ) -> crate::api::Result<()> {
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
pub trait TaskQueue: Debug {
    type Task: Send + Sync + 'static;
    type Input: Debug + Send + Sync + 'static;

    fn config(&self) -> &TaskQueueConfig;
    fn queue_name(&self) -> &'static str;

    async fn enqueue(&self, task: Self::Input) -> crate::api::Result<()>;
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>>;
    async fn record_success(&self, id: Uuid) -> crate::api::Result<()>;
    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()>;

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
            SuccessOrFailure::Failure(details) => self.record_failure(task.task_id, details).await,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskQueueConfig {
    pub max_retries: i32,
    #[serde(
        deserialize_with = "crate::config::seconds_to_duration",
        serialize_with = "crate::config::duration_to_seconds"
    )]
    pub max_age: chrono::Duration,
    #[serde(
        deserialize_with = "seconds_to_std_duration",
        serialize_with = "std_duration_to_seconds"
    )]
    pub poll_interval: Duration,
}

pub(crate) fn seconds_to_std_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    Ok(Duration::from_secs(
        u64::from_str(&buf).map_err(serde::de::Error::custom)?,
    ))
}

pub(crate) fn std_duration_to_seconds<S>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    duration.as_secs().to_string().serialize(serializer)
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            max_age: valid_max_age(3600),
            poll_interval: Duration::from_secs(10),
        }
    }
}

const fn valid_max_age(num: i64) -> chrono::Duration {
    assert!(num > 0, "max_age must be greater than 0");
    let dur = chrono::Duration::seconds(num);
    assert!(dur.num_microseconds().is_some());
    dur
}
