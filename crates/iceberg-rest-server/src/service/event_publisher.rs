use serde::Serialize;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait EventPublisher
where
    Self: Sized + Send + Sync + Clone + 'static,
{
    fn publish(&self, id: Uuid, typ: &str, data: impl Serialize);
}

#[derive(Clone)]
pub struct NoOpPublisher;

impl EventPublisher for NoOpPublisher {
    fn publish(&self, _id: Uuid, _typ: &str, _data: impl Serialize) {}
}

#[derive(Clone)]
pub struct TracingPublisher;

impl EventPublisher for TracingPublisher {
    fn publish(&self, id: Uuid, typ: &str, data: impl Serialize) {
        let data = serde_json::to_string(&data).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event of type: '{typ}' with id: '{id}', data: '{data}'");
    }
}
