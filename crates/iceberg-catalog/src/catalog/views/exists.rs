use crate::api::iceberg::v1::ViewParameters;
use crate::api::ApiContext;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::validate_table_or_view_ident;
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::Result;
use crate::service::{CatalogBackend, SecretStore, State};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

pub(crate) async fn view_exists<C: CatalogBackend, A: AuthZHandler, S: SecretStore>(
    parameters: ViewParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    validate_table_or_view_ident(&view)?;

    let view_id = C::view_ident_to_id(warehouse_id, &view, state.v1_state.catalog.clone())
        .await
        .transpose();

    A::check_view_exists(
        &request_metadata,
        warehouse_id,
        Some(&view.namespace),
        view_id.as_ref().and_then(|x| x.as_ref().ok()),
        state.v1_state.auth,
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    if view_id.transpose()?.is_some() {
        Ok(())
    } else {
        Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("Table does not exist in warehouse {warehouse_id}"))
            .r#type("TableNotFound".to_string())
            .build()
            .into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::api::iceberg::types::Prefix;
    use crate::api::iceberg::v1::ViewParameters;
    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::test::setup;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_view_exists(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let _ = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
        .await
        .unwrap();
        view_exists(
            ViewParameters {
                prefix: Some(prefix.clone()),
                view: TableIdent {
                    namespace: namespace.clone(),
                    name: view_name.to_string(),
                },
            },
            api_context.clone(),
            RequestMetadata::new_random(),
        )
        .await
        .unwrap();

        let non_exist = view_exists(
            ViewParameters {
                prefix: Some(prefix.clone()),
                view: TableIdent {
                    namespace: namespace.clone(),
                    name: "123".to_string(),
                },
            },
            api_context.clone(),
            RequestMetadata::new_random(),
        )
        .await
        .unwrap_err();

        assert_eq!(non_exist.error.code, StatusCode::NOT_FOUND);
    }
}
