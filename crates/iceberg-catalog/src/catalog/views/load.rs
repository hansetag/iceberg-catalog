use crate::api::iceberg::v1::{DataAccess, ViewParameters};
use crate::api::ApiContext;
use crate::catalog::tables::validate_table_or_view_ident;
use crate::catalog::{require_active_warehouse, require_warehouse_id};
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::storage::StorageCredential;
use crate::service::{Catalog, SecretStore, State, Transaction, ViewMetadataWithLocation};
use crate::service::{GetWarehouseResponse, Result};
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, LoadViewResult};

pub(crate) async fn load_view<C: Catalog, A: AuthZHandler, S: SecretStore>(
    parameters: ViewParameters,
    state: ApiContext<State<A, C, S>>,
    data_access: DataAccess,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix)?;
    // ToDo: Remove workaround when hierarchical namespaces are supported.
    // It is important for now to throw a 404 if a table cannot be found,
    // because spark might check if `table`.`branch` exists, which should return 404.
    // Only then will it treat it as a branch.
    // 404 is returned by the logic in the remainder of this function. Here, we only
    // need to make sure that we don't fail prematurely on longer namespaces.
    match validate_table_or_view_ident(&view) {
        Ok(()) => {}
        Err(e) => {
            if e.error.r#type != *"NamespaceDepthExceeded" {
                return Err(e);
            }
        }
    }

    // ------------------- AUTHZ -------------------
    let view_id = C::view_ident_to_id(warehouse_id, &view, state.v1_state.catalog.clone())
        .await
        // We can't fail before AuthZ.
        .transpose();

    A::check_load_view(
        &request_metadata,
        warehouse_id,
        Some(&view.namespace),
        view_id.as_ref().and_then(|id| id.as_ref().ok()),
        state.v1_state.auth,
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let namespace_id = C::namespace_ident_to_id(
        warehouse_id,
        &view.namespace,
        state.v1_state.catalog.clone(),
    )
    .await?
    .ok_or(
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Namespace does not exist".to_string())
            .r#type("NamespaceNotFound".to_string())
            .build(),
    )?;

    let view_id = view_id.transpose()?.ok_or_else(|| {
        tracing::debug!("View does not exist.");
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("View does not exist in warehouse {warehouse_id}"))
            .r#type("ViewNotFound".to_string())
            .build()
    })?;
    let mut transaction = C::Transaction::begin_read(state.v1_state.catalog).await?;

    let GetWarehouseResponse {
        id: _,
        name: _,
        project_id: _,
        storage_profile,
        storage_secret_id,
        status,
    } = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
    require_active_warehouse(status)?;

    let ViewMetadataWithLocation {
        metadata_location,
        metadata: view_metadata,
    } = C::load_view(view_id, transaction.transaction()).await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret: Option<StorageCredential> = if let Some(secret_id) = &storage_secret_id {
        Some(
            S::get_secret_by_id(secret_id, state.v1_state.secrets)
                .await?
                .secret,
        )
    } else {
        None
    };

    let access = storage_profile
        .generate_table_config(
            warehouse_id,
            namespace_id,
            view_metadata.uuid().into(),
            &data_access,
            storage_secret.as_ref(),
        )
        .await?;
    let load_table_result = LoadViewResult {
        metadata_location: metadata_location.clone(),
        metadata: view_metadata,
        config: Some(access),
    };

    transaction.commit().await?;
    Ok(load_table_result)
}

#[cfg(test)]
pub(crate) mod test {
    use crate::api::iceberg::v1::{views, DataAccess, Prefix, ViewParameters};
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;

    use crate::implementations::postgres::secrets::Server;

    use crate::implementations::postgres::Catalog;
    use crate::implementations::AllowAllAuthZHandler;

    use crate::service::State;

    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};

    use sqlx::PgPool;

    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::test::setup;

    pub(crate) async fn load_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, Catalog, Server>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<Catalog, AllowAllAuthZHandler, Server> as views::ViewService<
            State<AllowAllAuthZHandler, Catalog, Server>,
        >>::load_view(
            params,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
    }

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        )
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
    }
}
