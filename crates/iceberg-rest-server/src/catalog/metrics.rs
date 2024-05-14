use http::HeaderMap;
use iceberg_rest_service::v1::{ApiContext, Result, TableParameters};

use crate::service::{auth::AuthZHandler, secrets::SecretStore, Catalog, State};

use super::CatalogServer;

#[async_trait::async_trait]
impl<C: Catalog, A: AuthZHandler, S: SecretStore>
    iceberg_rest_service::v1::metrics::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        _: TableParameters,
        _: serde_json::Value,
        _: ApiContext<State<A, C, S>>,
        _: HeaderMap,
    ) -> Result<()> {
        // ToDo: implement
        Ok(())
    }
}
