use crate::service::event_publisher::CloudEventsPublisher;
use crate::tracing::{MakeRequestUuid7, RestMakeSpan};

use crate::api::management::v1::ApiServer;
use crate::api::{iceberg::v1::new_v1_full_router, shutdown_signal, ApiContext};
use crate::service::contract_verification::ContractVerifiers;
use crate::service::token_verification::Verifier;
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use http::HeaderValue;
// use tower::util::option_layer;
use super::management::v1::ManagementApiDoc;
use crate::service::health::ServiceHealthProvider;
use crate::service::{
    auth::{AuthConfigHandler, AuthZHandler},
    config::ConfigProvider,
    Catalog, SecretStore, State,
};
use axum_extra::middleware::option_layer;
use tower::ServiceBuilder;
use tower_http::cors::AllowOrigin;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer,
    sensitive_headers::SetSensitiveHeadersLayer, timeout::TimeoutLayer, trace, trace::TraceLayer,
    ServiceBuilderExt,
};

use utoipa::OpenApi;

// consider passing state into here instead of the state in pieces
#[allow(clippy::module_name_repetitions, clippy::too_many_arguments)]
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
    table_change_checkers: ContractVerifiers,
    token_verifier: Option<Verifier>,
    svhp: ServiceHealthProvider,
    cors_origins: Option<Vec<HeaderValue>>,
) -> Router {
    let v1_routes = new_v1_full_router::<
        crate::catalog::ConfigServer<CP, C, AH, A>,
        crate::catalog::CatalogServer<C, A, S>,
        State<A, C, S>,
    >();
    let management_routes = Router::new().merge(ApiServer::new_v1_router());
    let maybe_cors_layer = option_layer(cors_origins.map(|origins| {
        tower_http::cors::CorsLayer::new().allow_origin(AllowOrigin::list(origins))
    }));
    let maybe_auth_layer = option_layer(token_verifier.map(|o| {
        axum::middleware::from_fn_with_state(
            o,
            crate::service::token_verification::auth_middleware_fn,
        )
    }));
    Router::new()
        .nest("/catalog/v1", v1_routes)
        .nest("/management/v1", management_routes)
        .layer(maybe_auth_layer)
        .route(
            "/health",
            get(|| async move {
                let health = svhp.collect_health().await;
                Json(health).into_response()
            }),
        )
        .merge(utoipa_swagger_ui::SwaggerUi::new("/swagger-ui").url(
            "/api-docs/management/v1/openapi.json",
            ManagementApiDoc::openapi(),
        ))
        .layer(axum::middleware::from_fn(
            crate::request_metadata::create_request_metadata_with_trace_id_fn,
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
                .layer(maybe_cors_layer)
                .propagate_x_request_id(),
        )
        .with_state(ApiContext {
            v1_state: State {
                auth: auth_state,
                catalog: catalog_state,
                secrets: secrets_state,
                publisher,
                contract_verifiers: table_change_checkers,
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
