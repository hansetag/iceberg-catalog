pub use iceberg_ext::catalog::rest::*;

pub mod v1;

// Used only to group required traits for a State
pub trait State: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ApiContext<S: State> {
    pub v1_state: S,
}

pub type Result<T, E = IcebergErrorResponse> = std::result::Result<T, E>;

#[cfg(feature = "tokio")]
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
