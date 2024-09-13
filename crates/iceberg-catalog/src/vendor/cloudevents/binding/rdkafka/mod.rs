// Vendored from
// https://github.com/cloudevents/sdk-rust/tree/a59c3f55a01d2c61afbc8ccadf4374cc159bf947/src/binding/rdkafka
// and modified as necessary
//
// Reason: cloudevents rust-sdk latest release at 2024-09-07 is 0.7, which depends on rdkafka 0.29
// We want to use 0.36
mod kafka_producer_record;

// keep this for now, maybe we need BaseRecord
// pub use kafka_producer_record::BaseRecordExt;
pub(crate) use kafka_producer_record::FutureRecordExt;
pub(crate) use kafka_producer_record::MessageRecord;
