use crate::api::iceberg::types::{DropParams, Prefix};
use crate::api::iceberg::v1::ViewParameters;
use crate::api::ApiContext;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::validate_table_or_view_ident;
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::contract_verification::ContractVerification;
use crate::service::event_publisher::EventMetadata;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{Catalog, SecretStore, State, Transaction};
use crate::service::{DropFlags, Result};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use uuid::Uuid;

pub(crate) async fn drop_view<C: Catalog, A: AuthZHandler, S: SecretStore>(
    parameters: ViewParameters,
    DropParams { purge_requested }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    validate_table_or_view_ident(&view)?;

    // ------------------- AUTHZ -------------------
    let view_id = C::view_ident_to_id(warehouse_id, &view, state.v1_state.catalog.clone())
        .await
        // We can't fail before AuthZ.
        .transpose();

    A::check_drop_view(
        &request_metadata,
        warehouse_id,
        view_id.as_ref().and_then(|id| id.as_ref().ok()),
        state.v1_state.auth,
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let view_id = view_id.transpose()?.ok_or_else(|| {
        tracing::debug!("View does not exist.");
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("View does not exist in warehouse {warehouse_id}"))
            .r#type("ViewNotFound".to_string())
            .build()
    })?;

    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularIdentUuid::View(*view_id))
        .await?
        .into_result()?;

    tracing::debug!("Proceeding to delete view");
    C::drop_view(
        view_id,
        DropFlags {
            purge: purge_requested.unwrap_or(false),
            hard_delete: false,
        },
        transaction.transaction(),
    )
    .await?;

    // TODO: Delete metadata files
    transaction.commit().await?;

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "dropView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id: *warehouse_id,
                name: view.name.clone(),
                namespace: view.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
        )
        .await;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::load::test::load_view;
    use crate::catalog::views::test::setup;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

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
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
            DropParams {
                purge_requested: None,
            },
            api_context.clone(),
            RequestMetadata::new_random(),
        )
        .await
        .expect("View should be droppable");

        let error = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);
    }
}
