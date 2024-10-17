use crate::api::iceberg::v1::{DataAccess, ViewParameters};
use crate::api::{set_not_found_status_code, ApiContext};
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::{require_active_warehouse, validate_table_or_view_ident};
use crate::catalog::views::parse_view_location;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction};
use crate::service::storage::{StorageCredential, StoragePermissions};
use crate::service::{Catalog, SecretStore, State, Transaction, ViewMetadataWithLocation};
use crate::service::{GetWarehouseResponse, Result};
use iceberg_ext::catalog::rest::LoadViewResult;

pub(crate) async fn load_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
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
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            &CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let view_id = C::view_to_id(warehouse_id, &view, t.transaction()).await; // We can't fail before AuthZ
    let view_id = authorizer
        .require_view_action(
            &request_metadata,
            warehouse_id,
            view_id,
            &CatalogViewAction::CanGetMetadata,
        )
        .await
        .map_err(set_not_found_status_code)?;

    // ------------------- BUSINESS LOGIC -------------------
    let GetWarehouseResponse {
        id: _,
        name: _,
        project_id: _,
        storage_profile,
        storage_secret_id,
        status,
        tabular_delete_profile: _,
    } = C::require_warehouse(warehouse_id, t.transaction()).await?;
    require_active_warehouse(status)?;

    let ViewMetadataWithLocation {
        metadata_location,
        metadata: view_metadata,
    } = C::load_view(view_id, false, t.transaction()).await?;

    let view_location = parse_view_location(&view_metadata.location)?;

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

    t.commit().await?;
    Ok(load_table_result)
}

#[cfg(test)]
pub(crate) mod test {
    use crate::api::iceberg::v1::{views, DataAccess, Prefix, ViewParameters};
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;

    use crate::implementations::postgres::secrets::SecretsState;

    use crate::implementations::postgres::PostgresCatalog;
    use crate::service::authz::AllowAllAuthorizer;

    use crate::service::State;

    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{CreateViewRequest, LoadViewResult};

    use sqlx::PgPool;

    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::test::setup;

    pub(crate) async fn load_view(
        api_context: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        params: ViewParameters,
    ) -> crate::api::Result<LoadViewResult> {
        <CatalogServer<PostgresCatalog, AllowAllAuthorizer, SecretsState> as views::Service<
            State<AllowAllAuthorizer, PostgresCatalog, SecretsState>,
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
