use serde::Serialize;
use std::borrow::Cow;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug)]
pub struct EventMetadata<'a> {
    pub table_id: Uuid,
    pub warehouse_id: Uuid,
    pub name: Cow<'a, str>,
    pub namespace: Cow<'a, str>,
    pub prefix: Cow<'a, str>,
    pub num_events: usize,
    pub sequence_number: usize,
    pub trace_id: Uuid,
}

#[async_trait::async_trait]
pub trait EventPublisher
where
    Self: Sized + Send + Sync + Clone + Debug + 'static,
{
    async fn publish<'c>(
        &self,
        id: Uuid,
        typ: &str,
        data: impl Serialize + Send,
        metadata: EventMetadata<'c>,
    );
}

#[derive(Clone, Debug)]
pub struct NoOpPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpPublisher {
    async fn publish<'c>(
        &self,
        _id: Uuid,
        _typ: &str,
        _data: impl Serialize + Send,
        _metadata: EventMetadata<'c>,
    ) {
    }
}

#[derive(Clone, Debug)]
pub struct TracingPublisher;

#[async_trait::async_trait]
impl EventPublisher for TracingPublisher {
    async fn publish<'c>(
        &self,
        id: Uuid,
        typ: &str,
        data: impl Serialize + Send,
        metadata: EventMetadata<'c>,
    ) {
        let data = serde_json::to_string(&data).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event of type: '{typ}' with id: '{id}', data: '{data}' and metadata: '{metadata:?}'");
    }
}
