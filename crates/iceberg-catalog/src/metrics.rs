use axum_prometheus::metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use axum_prometheus::{
    utils, PrometheusMetricLayer, PrometheusMetricLayerBuilder,
    AXUM_HTTP_REQUESTS_DURATION_SECONDS, PREFIXED_HTTP_REQUESTS_DURATION_SECONDS,
};

/// Creates `PrometheusRecorder` and installs it as the global metrics recorder, spawns a tokio task
/// behind the scenes that will serve metrics under 0.0.0.0:`metrics_port`. Creates and returns a
/// `PrometheusMetricLayer` which captures axum requests and responses.
///
/// # Errors
/// Fails if the `PrometheusBuilder` fails to build.
pub fn get_axum_layer_and_install_recorder(
    metrics_port: u16,
) -> anyhow::Result<PrometheusMetricLayer<'static>> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(
                PREFIXED_HTTP_REQUESTS_DURATION_SECONDS
                    .get()
                    .map_or(AXUM_HTTP_REQUESTS_DURATION_SECONDS, |s| s.as_str())
                    .to_string(),
            ),
            utils::SECONDS_DURATION_BUCKETS,
        )?
        .with_http_listener(([0, 0, 0, 0], metrics_port))
        .install_recorder()?;

    let (layer, _) = PrometheusMetricLayerBuilder::new()
        .with_metrics_from_fn(|| handle)
        .build_pair();

    Ok(layer)
}
