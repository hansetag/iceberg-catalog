use iceberg_rest_service::v1::{ApiContext, Result, TableParameters};
use iceberg_rest_service::RequestMetadata;

use crate::service::event_publisher::EventPublisher;
use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State};

use super::CatalogServer;

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore, P: EventPublisher>
    iceberg_rest_service::v1::metrics::Service<State<A, C, S, P>> for CatalogServer<C, A, S, P>
{
    async fn report_metrics(
        _: TableParameters,
        _: serde_json::Value,
        _: ApiContext<State<A, C, S, P>>,
        _: RequestMetadata,
    ) -> Result<()> {
        // ToDo: implement
        Ok(())
    }
}
