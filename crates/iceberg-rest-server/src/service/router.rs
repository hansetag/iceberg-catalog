use crate::service::event_publisher::EventPublisher;
use crate::tracing::RestMakeSpan;

use axum::Router;
use iceberg_rest_service::{new_v1_full_router, shutdown_signal, ApiContext};
use tower::ServiceBuilder;
use tower_http::request_id::MakeRequestUuid;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
    ServiceBuilderExt,
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
    P: EventPublisher,
>(
    auth_state: A::State,
    catalog_state: C::State,
    secrets_state: S::State,
    publisher: P,
) -> Router {
    let v1_routes = new_v1_full_router::<
        crate::catalog::ConfigServer<CP, C, AH, A, P>,
        crate::catalog::CatalogServer<C, A, S, P>,
        State<A, C, S, P>,
    >();
    let management_routes = Router::new().merge(crate::api::ApiServer::v1_router());
    Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(
            ServiceBuilder::new()
                .set_x_request_id(MakeRequestUuid)
                .layer(CatchPanicLayer::new())
                .layer(SetSensitiveHeadersLayer::new([
                    axum::http::header::AUTHORIZATION,
                ]))
                .layer(
                    TraceLayer::new_for_http()
                        .on_failure(())
                        .make_span_with(RestMakeSpan::new(tracing::Level::INFO))
                        .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
                )
                .layer(CompressionLayer::new())
                .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)))
                .propagate_x_request_id(),
        )
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
