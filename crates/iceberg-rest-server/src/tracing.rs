use http::Request;
use tower_http::trace::MakeSpan;
use tracing::{Level, Span};

/// A `MakeSpan` implementation that attaches the `request_id` to the span.
#[derive(Debug, Clone)]
pub(crate) struct RestMakeSpan {
    level: Level,
}

impl RestMakeSpan {
    /// Create a [tracing span] with a certain [`Level`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    pub(crate) fn new(level: Level) -> Self {
        Self { level }
    }
}

/// tower-http's `MakeSpan` implementation does not attach a `request_id` to the span. The impl below
/// does.
impl<B> MakeSpan<B> for RestMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        // This ugly macro is needed, unfortunately, because `tracing::span!`
        // required the level argument to be static. Meaning we can't just pass
        // `self.level`.
        macro_rules! make_span {
            ($level:expr) => {
                    tracing::span!(
                        $level,
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        version = ?request.version(),
                        request_id = %request
                                    .headers()
                                    .get("x-request-id")
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("MISSING-REQUEST-ID"),
                        )

            }
        }
        match self.level {
            Level::TRACE => make_span!(tracing::Level::TRACE),
            Level::DEBUG => make_span!(tracing::Level::DEBUG),
            Level::INFO => make_span!(tracing::Level::INFO),
            Level::WARN => make_span!(tracing::Level::WARN),
            Level::ERROR => make_span!(tracing::Level::ERROR),
        }
    }
}
