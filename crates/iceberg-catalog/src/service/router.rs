use crate::service::event_publisher::CloudEventsPublisher;
use crate::tracing::{MakeRequestUuid7, RestMakeSpan};

use crate::api::management::ApiServer;
use crate::api::{iceberg::v1::new_v1_full_router, shutdown_signal, ApiContext};
use crate::service::table_change_check::TableChangeCheckers;
use axum::{routing::get, Router};
use tower::ServiceBuilder;
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
>(
    auth_state: A::State,
    catalog_state: C::State,
    secrets_state: S::State,
    publisher: CloudEventsPublisher,
    table_change_checkers: TableChangeCheckers,
) -> Router {
    let v1_routes = new_v1_full_router::<
        crate::catalog::ConfigServer<CP, C, AH, A>,
        crate::catalog::CatalogServer<C, A, S>,
        State<A, C, S>,
    >();
    let management_routes = Router::new().merge(ApiServer::new_v1_router());
    Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .route("/health", get(|| async { "OK" }))
        .layer(axum::middleware::from_fn(
            crate::request_metadata::set_request_metadata,
        ))
        .layer(
            ServiceBuilder::new()
                .set_x_request_id(MakeRequestUuid7)
                .layer(SetSensitiveHeadersLayer::new([
                    axum::http::header::AUTHORIZATION,
                ]))
                .layer(CompressionLayer::new())
                .layer(
                    TraceLayer::new_for_http()
                        .on_failure(())
                        .make_span_with(RestMakeSpan::new(tracing::Level::INFO))
                        .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
                )
                .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)))
                .layer(CatchPanicLayer::new())
                .propagate_x_request_id(),
        )
        .with_state(ApiContext {
            v1_state: State {
                auth: auth_state,
                catalog: catalog_state,
                secrets: secrets_state,
                publisher,
                table_change_checkers,
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
