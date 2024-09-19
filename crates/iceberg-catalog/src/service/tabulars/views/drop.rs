use crate::modules::auth::AuthZHandler;
use crate::modules::contract_verification::ContractVerification;
use crate::modules::event_publisher::EventMetadata;
use crate::modules::tabular_idents::TabularIdentUuid;
use crate::modules::task_queue::tabular_expiration_queue::TabularExpirationInput;
use crate::modules::task_queue::tabular_purge_queue::TabularPurgeInput;
use crate::modules::Result;
use crate::modules::{CatalogBackend, SecretStore, State, Transaction};
use crate::request_metadata::RequestMetadata;
use crate::rest::iceberg::types::{DropParams, Prefix};
use crate::rest::iceberg::v1::ViewParameters;
use crate::rest::management::v1::warehouse::TabularDeleteProfile;
use crate::rest::management::v1::TabularType;
use crate::rest::ApiContext;
use crate::service::require_warehouse_id;
use crate::service::tabulars::tables::validate_table_or_view_ident;
use iceberg_ext::catalog::rest::ErrorModel;
use uuid::Uuid;

pub(crate) async fn drop_view<C: CatalogBackend, A: AuthZHandler, S: SecretStore>(
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
    let purge_requested = purge_requested.unwrap_or(false);

    let view_id = view_id.transpose()?.ok_or_else(|| {
        tracing::debug!(?view, "View does not exist.");
        ErrorModel::not_found(
            format!("View does not exist in warehouse {warehouse_id}"),
            "ViewNotFound",
            None,
        )
    })?;

    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let warehouse = C::get_warehouse(warehouse_id, transaction.transaction()).await?;

    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularIdentUuid::View(*view_id))
        .await?
        .into_result()?;

    tracing::debug!("Proceeding to delete view");

    match warehouse.tabular_delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_view(view_id, transaction.transaction()).await?;
            // committing here means maybe dangling data if the queue fails
            // OTOH committing after queuing means we may end up with a view pointing to deleted files
            // I feel that some undeleted files are less bad than a view that cannot be loaded
            transaction.commit().await?;

            if purge_requested {
                state
                    .v1_state
                    .queues
                    .queue_tabular_purge(TabularPurgeInput {
                        tabular_location: location,
                        tabular_id: *view_id,
                        warehouse_ident: warehouse_id,
                        tabular_type: TabularType::View,
                        parent_id: None,
                    })
                    .await?;
            }
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            C::mark_tabular_as_deleted(TabularIdentUuid::View(*view_id), transaction.transaction())
                .await?;
            transaction.commit().await?;

            state
                .v1_state
                .queues
                .queue_tabular_expiration(TabularExpirationInput {
                    tabular_id: *view_id,
                    warehouse_ident: warehouse_id,
                    tabular_type: TabularType::View,
                    purge: purge_requested,
                    expire_at: chrono::Utc::now() + expiration_seconds,
                })
                .await?;
        }
    }

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
    use crate::rest::iceberg::types::{DropParams, Prefix};
    use crate::rest::iceberg::v1::ViewParameters;

    use crate::request_metadata::RequestMetadata;
    use crate::service::tabulars::views::drop::drop_view;
    use crate::service::test::{create_view, load_view, setup};
    use http::StatusCode;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None, None, None).await;
        let whi = whi.warehouse_id;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = create_view(
            api_context.clone(),
            namespace.namespace.clone(),
            rq,
            Some(prefix.into()),
        )
        .await
        .unwrap();
        let mut table_ident = namespace.namespace.clone().inner();
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
