use crate::service::event_publisher::CloudEventsPublisher;
use axum::Router;
use iceberg_rest_service::{new_v1_full_router, shutdown_signal, ApiContext};
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
};

use super::{
    auth::{AuthConfigHandler, AuthZHandler},
    config::ConfigProvider,
    Catalog, SecretStore, State,
};

#[allow(clippy::module_name_repetitions)]
pub fn new_full_router<
    CP: ConfigProvider<C>,
    C: Catalog,
    AH: AuthConfigHandler<A>,
    A: AuthZHandler,
    S: SecretStore,
>(
    auth_state: A::State,
    catalog_state: C::State,
    secrets_state: S::State,
    publisher: CloudEventsPublisher,
) -> Router {
    let v1_routes = new_v1_full_router::<
        crate::catalog::ConfigServer<CP, C, AH, A>,
        crate::catalog::CatalogServer<C, A, S>,
        State<A, C, S>,
    >();
    let management_routes = Router::new().merge(crate::api::ApiServer::v1_router());

    Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer((
            SetSensitiveHeadersLayer::new([axum::http::header::AUTHORIZATION]),
            CompressionLayer::new(),
            TraceLayer::new_for_http()
                .on_failure(())
                .make_span_with(trace::DefaultMakeSpan::new().level(tracing::Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
            TimeoutLayer::new(std::time::Duration::from_secs(30)),
            CatchPanicLayer::new(),
        ))
        .with_state(ApiContext {
            v1_state: State {
                auth: auth_state,
                catalog: catalog_state,
                secrets: secrets_state,
                publisher,
            },
        })
}

/// Serve the given router on the given listener
///
/// # Errors
/// Fails if the webserver panics
pub async fn serve(listener: tokio::net::TcpListener, router: Router) -> anyhow::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!(e).context("error running HTTP server"))
}
