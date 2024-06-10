use axum::Router;
pub use iceberg_ext::catalog::rest::*;
use uuid::Uuid;

pub mod v1;

// Used only to group required traits for a State
pub trait State: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ApiContext<S: State> {
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

pub fn new_v1_full_router<
    C: v1::config::Service<S>,
    T: v1::namespace::Service<S>
        + v1::tables::Service<S>
        + v1::metrics::Service<S>
        + v1::s3_signer::Service<S>,
    S: State,
>() -> Router<ApiContext<S>> {
    Router::new()
        .merge(v1::config::router::<C, S>())
        .merge(v1::namespace::router::<T, S>())
        .merge(v1::tables::router::<T, S>())
        .merge(v1::s3_signer::router::<T, S>())
        .merge(v1::metrics::router::<T, S>())
}

pub fn new_v1_config_router<C: v1::config::Service<S>, S: State>() -> Router<ApiContext<S>> {
    v1::config::router::<C, S>()
}

#[cfg(feature = "tokio")]
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
