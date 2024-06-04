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
    Self: Send + Sync + Clone + Debug + 'static,
{
    async fn publish<'c>(
        &self,
        id: Uuid,
        typ: &str,
        data: serde_json::Value,
        metadata: EventMetadata<'c>,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Debug)]
pub struct NoOpPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpPublisher {
    async fn publish<'c>(
        &self,
        _id: Uuid,
        _typ: &str,
        _data: serde_json::Value,
        _metadata: EventMetadata<'c>,
    ) -> anyhow::Result<()> {
        Ok(())
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
        data: serde_json::Value,
        metadata: EventMetadata<'c>,
    ) -> anyhow::Result<()> {
        let data = serde_json::to_string(&data).unwrap_or("Serialization failed".to_string());
        tracing::info!("Received event of type: '{typ}' with id: '{id}', data: '{data}' and metadata: '{metadata:?}'");
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct NatsPublisher {
    pub client: async_nats::Client,
    pub topic: String,
}

#[async_trait::async_trait]
impl EventPublisher for NatsPublisher {
    async fn publish<'c>(
        &self,
        id: Uuid,
        typ: &str,
        data: serde_json::Value,
        metadata: EventMetadata<'c>,
    ) -> anyhow::Result<()> {
        use cloudevents::{EventBuilder, EventBuilderV10};

        let event_builder = EventBuilderV10::new()
            .id(id.to_string())
            .source(format!(
                "uri:iceberg-rest-service:{}",
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
        let event_builder = event_builder
            .extension("table-id", table_id.to_string())
            .extension("warehouse-id", warehouse_id.to_string())
            .extension("name", name.to_string())
            .extension("namespace", namespace.to_string())
            .extension("prefix", prefix.to_string())
            // TODO: decide what to do with these numbbers, likely they are never anywhere close to
            // saturating the respective int types, so probably a non-issue. Still we are converting
            // the numbers to_string here to avoid usize -> i64 which is what EventBuilderV10
            // uses to represent integers. The Cloudevents spec states i32 would be the correct int
            // type.
            .extension("num-events", num_events.to_string())
            .extension("sequence-number", sequence_number.to_string())
            // TODO: decide if we want to stick to a simple UUID or go for the spec:
            //       https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md
            //       https://w3c.github.io/trace-context/#traceparent-header
            .extension("trace-id", trace_id.to_string());

        self.client
            .publish(
                self.topic.clone(),
                serde_json::to_vec(&event_builder.build()?)?.into(),
            )
            .await?;
        Ok(())
    }
}
