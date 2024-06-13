use async_trait::async_trait;
use cloudevents::Event;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CloudEventsPublisher {
    tx: tokio::sync::mpsc::Sender<Message>,
}

impl CloudEventsPublisher {
    #[must_use] pub fn new(tx: tokio::sync::mpsc::Sender<Message>) -> Self {
        Self { tx }
    }

    pub async fn publish(
        &self,
        id: Uuid,
        typ: &str,
        data: serde_json::Value,
        metadata: EventMetadata,
    ) {
        let _ = self
            .tx
            .send(Message::Event(Payload {
                id,
                typ: typ.to_string(),
                data,
                metadata,
            }))
            .await
            .map_err(|e| tracing::warn!("Failed to emit event with id: '{}' due to: '{}'.", id, e));
    }
}

#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub table_id: Uuid,
    pub warehouse_id: Uuid,
    pub name: String,
    pub namespace: String,
    pub prefix: String,
    pub num_events: usize,
    pub sequence_number: usize,
    pub trace_id: Uuid,
}

#[derive(Debug)]
pub struct Payload {
    pub id: Uuid,
    pub typ: String,
    pub data: serde_json::Value,
    pub metadata: EventMetadata,
}

#[derive(Debug)]
pub enum Message {
    Event(Payload),
    Shutdown,
}

#[derive(Debug)]
pub struct CloudEventsPublisherBackgroundTask {
    pub source: tokio::sync::mpsc::Receiver<Message>,
    pub sinks: Vec<Arc<dyn CloudEventBackend + Sync + Send>>,
}

impl CloudEventsPublisherBackgroundTask {
    /// # Errors
    /// Returns an error if the `Event` cannot be built from the data passed into this function
    pub async fn publish(mut self) -> anyhow::Result<()> {
        while let Some(Message::Event(Payload {
            id,
            typ,
            data,
            metadata,
        })) = self.source.recv().await
        {
            use cloudevents::{EventBuilder, EventBuilderV10};

            let event_builder = EventBuilderV10::new()
                .id(id.to_string())
                .source(format!(
                    "uri:iceberg-catalog-service:{}",
                    hostname::get()
                        .map(|os| os.to_string_lossy().to_string())
                        .unwrap_or("hostname-unavailable".into())
                ))
                .ty(typ)
                .data("application/json", data);

            let EventMetadata {
                table_id,
                warehouse_id,
                name,
                namespace,
                prefix,
                num_events,
                sequence_number,
                trace_id,
            } = metadata;
            // TODO: this could be more elegant with a proc macro to give us IntoIter for EventMetadata
            let event = event_builder
                .extension("table-id", table_id.to_string())
                .extension("warehouse-id", warehouse_id.to_string())
                .extension("name", name.to_string())
                .extension("namespace", namespace.to_string())
                .extension("prefix", prefix.to_string())
                // TODO: decide what to do with these numbbers, likely they are never anywhere close to
                // saturating the respective int types, so probably a non-issue. Still we are converting
                // the numbers to_string here to avoid usize -> i64 which is what EventBuilderV10
                // uses to represent integers. The CloudEvents spec states i32 would be the correct int
                // type.
                .extension("num-events", num_events.to_string())
                .extension("sequence-number", sequence_number.to_string())
                // Implement distributed tracing: https://github.com/hansetag/iceberg-catalog/issues/63
                .extension("trace-id", trace_id.to_string())
                .build()?;

            for sink in &self.sinks {
                if let Err(e) = sink.publish(event.clone()).await {
                    tracing::warn!(
                        "Failed to emit event with id: '{}' on sink: '{}' due to: '{}'.",
                        id,
                        sink.name(),
                        e
                    );
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
pub trait CloudEventBackend: Debug {
    async fn publish(&self, event: Event) -> anyhow::Result<()>;
    fn name(&self) -> &str;
}

#[derive(Debug)]
pub struct NatsBackend {
    pub client: async_nats::Client,
    pub topic: String,
}

#[async_trait]
impl CloudEventBackend for NatsBackend {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        Ok(self
            .client
            .publish(self.topic.clone(), serde_json::to_vec(&event)?.into())
            .await?)
    }

    fn name(&self) -> &'static str {
        "nats-publisher"
    }
}

#[derive(Clone, Debug)]
pub struct TracingPublisher;

#[async_trait::async_trait]
impl CloudEventBackend for TracingPublisher {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let data = serde_json::to_string(&event).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event: {data}'");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "tracing-publisher"
    }
}
