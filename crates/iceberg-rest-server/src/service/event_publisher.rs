use serde::Serialize;
use std::fmt::Debug;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait EventPublisher
where
    Self: Sized + Send + Sync + Clone + Debug + 'static,
{
    async fn publish(&self, id: Uuid, typ: &str, data: impl Serialize + Send);
}

#[derive(Clone, Debug)]
pub struct NoOpPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpPublisher {
    async fn publish(&self, _id: Uuid, _typ: &str, _data: impl Serialize + Send) {}
}

#[derive(Clone, Debug)]
pub struct TracingPublisher;

#[async_trait::async_trait]
impl EventPublisher for TracingPublisher {
    async fn publish(&self, id: Uuid, typ: &str, data: impl Serialize + Send) {
        let data = serde_json::to_string(&data).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event of type: '{typ}' with id: '{id}', data: '{data}'");
    }
}
