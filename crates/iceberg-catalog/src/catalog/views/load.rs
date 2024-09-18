use std::str::FromStr as _;

use crate::api::iceberg::v1::{DataAccess, ViewParameters};
use crate::api::ApiContext;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::{require_active_warehouse, validate_table_or_view_ident};
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::object_stores::{StorageCredential, StoragePermissions};
use crate::service::{CatalogBackend, SecretStore, State, Transaction, ViewMetadataWithLocation};
use crate::service::{GetWarehouseResponse, Result};
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, LoadViewResult};
use iceberg_ext::configs::Location;

pub(crate) async fn load_view<C: CatalogBackend, A: AuthZHandler, S: SecretStore>(
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
        tabular_delete_profile: _,
    } = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
    require_active_warehouse(status)?;

    let ViewMetadataWithLocation {
        metadata_location,
        metadata: view_metadata,
    } = C::load_view(view_id, false, transaction.transaction()).await?;

    let view_location = Location::from_str(&view_metadata.location).map_err(|e| {
        ErrorModel::internal(
            format!("Invalid view location in DB: {e}"),
            "InvalidViewLocation",
            Some(Box::new(e)),
        )
    })?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret: Option<StorageCredential> = if let Some(secret_id) = &storage_secret_id {
        Some(
            state
                .v1_state
                .secrets
                .get_secret_by_id(secret_id)
                .await?
                .secret,
        )
    } else {
        None
    };

    let access = storage_profile
        .generate_table_config(
            &data_access,
            storage_secret.as_ref(),
            &view_location,
            // TODO: This should be a permission based on authz
            StoragePermissions::ReadWriteDelete,
        )
        .await?;
    let load_table_result = LoadViewResult {
        metadata_location: metadata_location.clone(),
        metadata: view_metadata,
        config: Some(access.into()),
    };

    transaction.commit().await?;
    Ok(load_table_result)
}

#[cfg(test)]
pub(crate) mod test {
    use crate::api::iceberg::v1::{views, DataAccess, Prefix, ViewParameters};
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;

    use crate::service::catalog_backends::implementations::postgres::secrets::SecretsState;

    use crate::service::catalog_backends::implementations::postgres::PostgresCatalog;
    use crate::service::catalog_backends::implementations::AllowAllAuthZHandler;

    use crate::service::State;

    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};

    use sqlx::PgPool;

    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::test::setup;

    pub(crate) async fn load_view(
        api_context: ApiContext<State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<PostgresCatalog, AllowAllAuthZHandler, SecretsState> as views::Service<
            State<AllowAllAuthZHandler, PostgresCatalog, SecretsState>,
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
