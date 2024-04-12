use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace::TraceLayer,
};

use std::net::SocketAddr;

use axum::{http::header::AUTHORIZATION, Router};
pub use iceberg_ext::catalog::rest::*;

pub mod v1;

pub trait State: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ApiContext<S: State> {
    pub v1_state: S,
}

pub type Result<T, E = IcebergErrorResponse> = std::result::Result<T, E>;

fn api_router<I: v1::V1Service<S>, S: State>(api_context: ApiContext<S>) -> axum::Router {
    let v1_routes = Router::new()
        .merge(v1::config_router::<I, S>())
        .merge(v1::oauth_router::<I, S>())
        .merge(v1::namespace_router::<I, S>())
        .merge(v1::table_router::<I, S>())
        .merge(v1::metrics_router::<I, S>());
    // .merge(profiles::router())
    // .merge(articles::router())

    // This is the order that the modules were authored in.
    Router::new()
        .nest("/v1", v1_routes)
        // Enables logging. Use `RUST_LOG=tower_http=debug`
        .layer((
            SetSensitiveHeadersLayer::new([AUTHORIZATION]),
            CompressionLayer::new(),
            TraceLayer::new_for_http().on_failure(()),
            TimeoutLayer::new(std::time::Duration::from_secs(30)),
            CatchPanicLayer::new(),
        ))
        .with_state(api_context)
}

pub async fn serve<I: v1::V1Service<S>, S: State>(
    addr: SocketAddr,
    api_context: ApiContext<S>,
) -> anyhow::Result<()> {
    let app = api_router::<I, S>(api_context);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("error running HTTP server"))
}

async fn shutdown_signal() {
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
