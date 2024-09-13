#![allow(clippy::module_name_repetitions)]

use crate::service::TableIdentUuid;
use async_trait::async_trait;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait LocationCache: Send + Sync + 'static {
    async fn insert_location(&mut self, location: &str, table_id: TableIdentUuid);
    async fn get_table_id(&mut self, location: &str) -> Option<TableIdentUuid>;
    async fn remove_location(&mut self, location: &str) -> Option<TableIdentUuid>;
}

#[async_trait]
impl LocationCache for Arc<Mutex<LruCache<String, TableIdentUuid>>> {
    async fn insert_location(&mut self, location: &str, table_id: TableIdentUuid) {
        let mut lock = self.lock().await;
        lock.get_or_insert(location.to_string(), || table_id);
    }

    async fn get_table_id(&mut self, location: &str) -> Option<TableIdentUuid> {
        let mut lock = self.lock().await;
        lock.get(location).copied()
    }

    async fn remove_location(&mut self, location: &str) -> Option<TableIdentUuid> {
        let mut lock = self.lock().await;
        lock.pop(location)
    }
}
