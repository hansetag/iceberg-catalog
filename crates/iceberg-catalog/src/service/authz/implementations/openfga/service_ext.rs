use crate::service::authz::{ErrorModel, Result};
use openfga_rs::open_fga_service_client::OpenFgaServiceClient;
use openfga_rs::{
    tonic::{
        self,
        codegen::{Body, Bytes, StdError},
    },
    ListStoresRequest, Store,
};

#[async_trait::async_trait]
pub trait ClientHelper {
    async fn get_store_by_name(&mut self, store_name: &str) -> Result<Store>;
}

#[async_trait::async_trait]
impl<T> ClientHelper for OpenFgaServiceClient<T>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<axum::body::Bytes, openfga_rs::tonic::Status>,
    >>::Future: Send,
{
    async fn get_store_by_name(&mut self, store_name: &str) -> Result<Store> {
        let mut continuation_token = String::new();
        let count = 0;
        let store = loop {
            let stores = self
                .list_stores(ListStoresRequest {
                    page_size: Some(100),
                    continuation_token: continuation_token.clone(),
                })
                .await
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to list stores",
                        "OpenFGAConnection",
                        Some(Box::new(e)),
                    )
                })?
                .into_inner();
            let num_stores = stores.stores.len();
            continuation_token.clone_from(&stores.continuation_token);
            let store = stores.stores.into_iter().find(|s| s.name == store_name);
            if let Some(store) = store {
                break store;
            }

            if continuation_token.is_empty() || num_stores < 100 || count > 100 {
                return Err(ErrorModel::internal(
                    format!("Store {store_name} not found"),
                    "OpenFGAConnection",
                    None,
                )
                .into());
            }
        };
        Ok(store)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // #[needs_env_var::needs_env_var("TEST_OPENFGA" = 1)]
    mod openfga {
        use openfga_rs::CreateStoreRequest;
        use tonic::transport::Channel;

        use super::*;
        use crate::service::authz::implementations::openfga::{
            new_unauthenticated_client, AUTH_CONFIG,
        };

        async fn new_store(client: &mut OpenFgaServiceClient<Channel>) -> String {
            let store_name = uuid::Uuid::now_v7().to_string();
            client
                .create_store(CreateStoreRequest {
                    name: store_name.clone(),
                })
                .await
                .unwrap();
            store_name
        }

        #[tokio::test]
        async fn test_get_store_by_name() {
            let mut client = new_unauthenticated_client(AUTH_CONFIG.endpoint.clone())
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client.get_store_by_name(&store_name).await.unwrap();
            assert_eq!(store.name, store_name);

            let retrieved_store = client.get_store_by_name("non-existent-store").await;
            assert!(retrieved_store.is_err());

            let retrieved_store = client.get_store_by_name(&store_name).await.unwrap();
            assert_eq!(retrieved_store.name, store_name);
            assert_eq!(retrieved_store, store);
        }

        #[tokio::test]
        async fn test_get_store_by_name_pagination() {
            // Create 201 stores
            let mut client = new_unauthenticated_client(AUTH_CONFIG.endpoint.clone())
                .await
                .expect("Failed to create OpenFGA client");

            let mut store_names = std::collections::HashSet::new();
            for _ in 0..201 {
                store_names.insert(new_store(&mut client).await);
            }

            assert_eq!(store_names.len(), 201);

            // Get all stores
            for store_name in &store_names {
                let store = client.get_store_by_name(store_name).await.unwrap();
                assert_eq!(store.name, *store_name);
            }
        }

        // #[tokio::test]
        // async fn test_get_auth_model() {
        //     let mut client = new_unauthenticated_client((*AUTH_CONFIG).endpoint.clone())
        //         .await
        //         .expect("Failed to create OpenFGA client");

        //     let store_name = new_store(&mut client).await;
        //     let store = client.get_store_by_name(&store_name).await.unwrap();

        //     let test_model = CollaborationModelVersion::latest().get_model();

        //     let model = client
        //         .write_authorization_model(WriteAuthorizationModelRequest {
        //             store_id: store.id,
        //             type_definitions: test_model.type_definitions.clone(),
        //             schema_version: test_model.schema_version.clone(),
        //             conditions: test_model.conditions.clone().unwrap_or_default(),
        //         })
        //         .await
        //         .unwrap();
        // }
    }
}
