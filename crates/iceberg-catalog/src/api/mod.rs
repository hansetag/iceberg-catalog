pub mod iceberg;
pub mod management;

pub use iceberg_ext::catalog::rest::*;
use uuid::Uuid;

// Used only to group required traits for a State
pub trait ThreadSafe: Clone + Send + Sync + 'static {}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct ApiContext<S: ThreadSafe> {
    pub v1_state: S,
}

/// A struct to hold metadata about a request.
///
/// Currently, it only holds the `request_id`, later it can be expanded to hold more metadata for
/// Authz etc.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    pub request_id: Uuid,
}

impl RequestMetadata {
    #[cfg(test)]
    #[must_use]
    pub fn new_random() -> Self {
        Self {
            request_id: Uuid::new_v4(),
        }
    }
}

pub type Result<T, E = IcebergErrorResponse> = std::result::Result<T, E>;

/// This function will wait for a signal to shutdown the service.
/// It will wait for either a Ctrl+C signal or a SIGTERM signal.
///
/// # Panics
/// If the function fails to install the signal handler, it will panic.
pub async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
