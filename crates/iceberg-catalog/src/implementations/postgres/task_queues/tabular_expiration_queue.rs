use crate::api::management::v1::TabularType;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::TabularType as DbTabularType;
use crate::implementations::postgres::task_queues::{
    pick_task, queue_task, record_failure, record_success,
};
use crate::implementations::postgres::DeletionKind;
use crate::service::task_queue::tabular_expiration_queue::{
    TabularExpirationInput, TabularExpirationTask,
};
use crate::service::task_queue::{TaskQueue, TaskQueueConfig};
use async_trait::async_trait;
use uuid::Uuid;

super::impl_pg_task_queue!(TabularExpirationQueue);

#[async_trait]
impl TaskQueue for TabularExpirationQueue {
    type Task = TabularExpirationTask;
    type Input = TabularExpirationInput;

    fn config(&self) -> &TaskQueueConfig {
        &self.pg_queue.config
    }

    fn queue_name(&self) -> &'static str {
        "tabular_expiration"
    }

    #[tracing::instrument(skip(self))]
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>> {
        let task = pick_task(
            &self.pg_queue.read_write.write_pool,
            self.queue_name(),
            &self.pg_queue.max_age,
        )
        .await?;

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
            .fetch_one(&self.pg_queue.read_write.read_pool)
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
        record_success(id, &self.pg_queue.read_write.write_pool).await
    }

    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()> {
        record_failure(
            &self.pg_queue.read_write.write_pool,
            id,
            self.config().max_retries,
            error_details,
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue(
        &self,
        TabularExpirationInput {
            tabular_id,
            warehouse_ident,
            tabular_type,
            purge,
            expire_at,
        }: TabularExpirationInput,
    ) -> crate::api::Result<()> {
        let mut transaction = self
            .pg_queue
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

        let Some(task_id) = queue_task(
            &mut transaction,
            self.queue_name(),
            None,
            idempotency_key,
            warehouse_ident,
            Some(expire_at),
        )
        .await?
        else {
            tracing::debug!("Task already exists");
            transaction.commit().await.map_err(|e| {
                tracing::error!(?e, "failed to commit");
                e.into_error_model("fail".into())
            })?;
            return Ok(());
        };

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
            } as _)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!(?e, "failed to insert into tabular_expirations");
                e.into_error_model("fail".into()) })?;

        if let Some(row) = it {
            tracing::debug!("Queued expiration task: {:?}", row.task_id);
        } else {
            tracing::debug!("Expiration task already exists.");
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("fail".into())
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::super::test::setup;
    use crate::service::task_queue::tabular_expiration_queue::TabularExpirationInput;
    use crate::service::task_queue::{TaskQueue, TaskQueueConfig};
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let pg_queue = setup(pool, config);
        let queue = super::TabularExpirationQueue { pg_queue };
        let input = TabularExpirationInput {
            tabular_id: uuid::Uuid::new_v4(),
            warehouse_ident: uuid::Uuid::new_v4().into(),
            tabular_type: crate::api::management::v1::TabularType::Table,
            purge: false,
            expire_at: chrono::Utc::now(),
        };
        queue.enqueue(input.clone()).await.unwrap();
        queue.enqueue(input.clone()).await.unwrap();

        let task = queue
            .pick_new_task()
            .await
            .unwrap()
            .expect("There should be a task");

        assert_eq!(task.warehouse_ident, input.warehouse_ident);
        assert_eq!(task.tabular_id, input.tabular_id);
        assert_eq!(task.tabular_type, input.tabular_type);
        assert_eq!(
            task.deletion_kind,
            crate::implementations::postgres::DeletionKind::Default.into()
        );

        let task = queue.pick_new_task().await.unwrap();
        assert!(
            task.is_none(),
            "There should only be one task, idempotency didn't work."
        );
    }
}
