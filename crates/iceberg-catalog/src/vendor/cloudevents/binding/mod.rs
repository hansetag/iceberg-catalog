// Vendored from
// https://github.com/cloudevents/sdk-rust/tree/a59c3f55a01d2c61afbc8ccadf4374cc159bf947/src/binding
// and modified as necessary
//
// Reason: cloudevents rust-sdk latest release at 2024-09-07 is 0.7, which depends on rdkafka 0.29
// We want to use 0.36

#[cfg(feature = "kafka")]
pub(crate) mod rdkafka;

#[cfg(feature = "kafka")]
pub(crate) mod kafka {
    pub(crate) static SPEC_VERSION_HEADER: &str = "ce_specversion";
    pub(crate) fn header_prefix(name: &str) -> String {
        super::header_prefix("ce_", name)
    }
}

pub(crate) static CLOUDEVENTS_JSON_HEADER: &str = "application/cloudevents+json";
pub(crate) static CLOUDEVENTS_BATCH_JSON_HEADER: &str = "application/cloudevents-batch+json";
pub(crate) static CONTENT_TYPE: &str = "content-type";

fn header_prefix(prefix: &str, name: &str) -> String {
    if name == "datacontenttype" {
        CONTENT_TYPE.to_string()
    } else {
        [prefix, name].concat()
    }
}

#[macro_export]
macro_rules! header_value_to_str {
    ($header_value:expr) => {
        $header_value
            .to_str()
            .map_err(|e| $crate::message::Error::Other {
                source: Box::new(e),
            })
    };
}
