mod commit_tables;
pub(crate) mod compression_codec;
mod config;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
mod tables;
mod views;

pub use config::Server as ConfigServer;
use iceberg::spec::{TableMetadata, ViewMetadata};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub use namespace::{MAX_NAMESPACE_DEPTH, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::api::{iceberg::v1::Prefix, ErrorModel, Result};
use crate::service_modules::object_stores::StorageCredential;
use crate::{
    service_modules::{auth::AuthZHandler, secrets::SecretStore, CatalogBackend},
    WarehouseIdent,
};
use std::collections::HashMap;
use std::marker::PhantomData;

pub trait CommonMetadata {
    fn properties(&self) -> &HashMap<String, String>;
}

impl CommonMetadata for TableMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        TableMetadata::properties(self)
    }
}

impl CommonMetadata for ViewMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        ViewMetadata::properties(self)
    }
}

#[derive(Clone, Debug)]

pub struct CatalogServer<C: CatalogBackend, A: AuthZHandler, S: SecretStore> {
    auth_handler: PhantomData<A>,
    config_server: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}

pub(crate) async fn maybe_get_secret<S: SecretStore>(
    secret: Option<crate::SecretIdent>,
    state: &S,
) -> Result<Option<StorageCredential>, IcebergErrorResponse> {
    if let Some(secret_id) = &secret {
        Ok(Some(state.get_secret_by_id(secret_id).await?.secret))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::api::iceberg::types::Prefix;
    use crate::api::iceberg::v1::namespace::Service as _;
    use crate::api::iceberg::v1::views::Service as _;
    use crate::api::iceberg::v1::{DataAccess, NamespaceParameters, ViewParameters};
    use crate::api::management::v1::warehouse::{
        CreateWarehouseRequest, CreateWarehouseResponse, Service as _, TabularDeleteProfile,
    };
    use crate::api::management::v1::ApiServer;
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;
    use crate::request_metadata::RequestMetadata;
    use crate::service_modules::catalog_backends::implementations::postgres::tabular::table;
    use crate::service_modules::catalog_backends::implementations::postgres::{task_queues, CatalogState, PostgresCatalog, ReadWrite, SecretsState};
    use crate::service_modules::catalog_backends::implementations::{AllowAllAuthState, AllowAllAuthZHandler};
    use crate::service_modules::object_stores::{StorageCredential, StorageProfile, TestProfile};
    use crate::service_modules::{NamespaceIdentUuid, State, TableCreation, TableIdentUuid};
    use crate::{WarehouseIdent, CONFIG};
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::{
        CreateNamespaceRequest, CreateNamespaceResponse, CreateViewRequest, IcebergErrorResponse,
        LoadViewResult,
    };
    use sqlx::PgPool;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;
    use crate::service_modules::catalog_backends::implementations::postgres::tabular::table::create_table;
    use crate::service_modules::catalog_backends::implementations::postgres::tabular::table::tests::get_namespace_id;
    use crate::service_modules::contract_verification::ContractVerifiers;
    use crate::service_modules::event_publisher::CloudEventsPublisher;
    use crate::service_modules::task_queue::TaskQueues;

    pub(crate) async fn setup(
        pool: PgPool,
        namespace_name: Option<String>,
        storage_profile: Option<StorageProfile>,
        storage_credential: Option<StorageCredential>,
    ) -> (
        ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
        CreateNamespaceResponse,
        CreateWarehouseResponse,
    ) {
        let api_context = get_api_context(pool);
        let _state = api_context.v1_state.catalog.clone();
        let warehouse = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: format!("test-warehouse-{}", Uuid::now_v7()),
                project_id: Uuid::now_v7(),
                storage_profile: storage_profile.unwrap_or(StorageProfile::Test(TestProfile)),
                storage_credential,
                delete_profile: TabularDeleteProfile::Hard {},
            },
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let namespace = NamespaceIdent::new(namespace_name.unwrap_or(Uuid::now_v7().to_string()));

        let namespace = create_namespace(
            namespace,
            warehouse.warehouse_id.into(),
            api_context.clone(),
        )
        .await;

        (api_context, namespace, warehouse)
    }

    pub(crate) async fn create_namespace(
        ident: NamespaceIdent,
        warehouse_id: WarehouseIdent,
        api_context: ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
    ) -> CreateNamespaceResponse {
        CatalogServer::create_namespace(
            Some(Prefix(warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: ident,
                properties: None,
            },
            api_context,
            random_request_metadata(),
        )
        .await
        .unwrap()
    }

    pub(crate) async fn create_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
        namespace: NamespaceIdent,
        rq: CreateViewRequest,
        prefix: Option<String>,
    ) -> Result<LoadViewResult, IcebergErrorResponse> {
        CatalogServer::create_view(
            NamespaceParameters {
                namespace: namespace.clone(),
                prefix: Some(Prefix(
                    prefix.unwrap_or("b8683712-3484-11ef-a305-1bc8771ed40c".to_string()),
                )),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_random(),
        )
        .await
    }

    pub(crate) async fn load_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        CatalogServer::load_view(
            params,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_random(),
        )
        .await
    }

    pub(crate) struct InitializedTable {
        #[allow(dead_code)]
        pub(crate) namespace_id: NamespaceIdentUuid,
        pub(crate) namespace: NamespaceIdent,
        pub(crate) table_id: TableIdentUuid,
        pub(crate) table_ident: TableIdent,
    }

    pub(crate) async fn initialize_table(
        warehouse_id: WarehouseIdent,
        api_context: ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
        staged: bool,
        namespace: Option<NamespaceIdent>,
        table_name: Option<String>,
    ) -> InitializedTable {
        // my_namespace_<uuid>
        let namespace = if let Some(namespace) = namespace {
            namespace
        } else {
            let namespace =
                NamespaceIdent::from_vec(vec![format!("my_namespace_{}", Uuid::now_v7())]).unwrap();
            create_namespace(namespace.clone(), warehouse_id, api_context.clone()).await;
            namespace
        };
        let namespace_id = get_namespace_id(
            api_context.v1_state.catalog.clone(),
            warehouse_id,
            &namespace,
        )
        .await;

        let (request, metadata_location) = table::tests::create_request(Some(staged), table_name);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };
        let table_id = Uuid::now_v7().into();
        let table_location = request
            .location
            .as_deref()
            .map(FromStr::from_str)
            .transpose()
            .unwrap()
            .unwrap();
        let create = TableCreation {
            namespace_id,
            table_ident: &table_ident,
            table_id,
            table_location: &table_location,
            table_schema: request.schema,
            table_partition_spec: request.partition_spec,
            table_write_order: request.write_order,
            table_properties: request.properties,
            metadata_location: metadata_location.as_ref(),
        };
        let mut transaction = api_context
            .v1_state
            .catalog
            .write_pool()
            .begin()
            .await
            .unwrap();
        let _create_result = create_table(create, &mut transaction).await.unwrap();

        transaction.commit().await.unwrap();

        InitializedTable {
            namespace_id,
            namespace,
            table_id,
            table_ident,
        }
    }

    pub(crate) fn random_request_metadata() -> RequestMetadata {
        RequestMetadata {
            request_id: Uuid::new_v4(),
            auth_details: None,
        }
    }

    pub(crate) fn get_api_context(
        pool: PgPool,
    ) -> ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>> {
        let (tx, _) = tokio::sync::mpsc::channel(1000);

        ApiContext {
            v1_state: State {
                auth: AllowAllAuthState,
                catalog: CatalogState::from_pools(pool.clone(), pool.clone()),
                secrets: SecretsState::from_pools(pool.clone(), pool.clone()),
                publisher: CloudEventsPublisher::new(tx.clone()),
                contract_verifiers: ContractVerifiers::new(vec![]),
                queues: TaskQueues::new(
                    Arc::new(
                        task_queues::TabularExpirationQueue::from_config(
                            ReadWrite::from_pools(pool.clone(), pool.clone()),
                            CONFIG.queue_config.clone(),
                        )
                        .unwrap(),
                    ),
                    Arc::new(
                        task_queues::TabularPurgeQueue::from_config(
                            ReadWrite::from_pools(pool.clone(), pool),
                            CONFIG.queue_config.clone(),
                        )
                        .unwrap(),
                    ),
                ),
            },
        }
    }
}
