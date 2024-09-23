pub mod iceberg;
pub mod management;

#[cfg(feature = "router")]
pub mod router;

pub use iceberg_ext::catalog::rest::*;

// Used only to group required traits for a State
pub trait ThreadSafe: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ApiContext<S: ThreadSafe> {
    pub v1_state: S,
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
