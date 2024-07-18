use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait::async_trait]
pub trait HealthExt {
    async fn health(&self) -> Vec<(&'static str, HealthStatus)>;
    async fn update_health(&self);
    async fn update_health_task(&self) {
        loop {
            self.update_health().await;
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }
}

#[derive(Clone, Debug, strum::Display, Serialize)]
pub enum HealthStatus {
    #[serde(rename = "ok")]
    Healthy,
    #[serde(rename = "error")]
    Unhealthy,
    #[serde(rename = "unknown")]
    Unknown,
}
