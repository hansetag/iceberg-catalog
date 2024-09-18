use crate::api::iceberg::types::Prefix;
use crate::api::ApiContext;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::{maybe_body_to_json, validate_table_or_view_ident};
use crate::request_metadata::RequestMetadata;
use crate::service_modules::auth::AuthZHandler;
use crate::service_modules::contract_verification::ContractVerification;
use crate::service_modules::event_publisher::EventMetadata;
use crate::service_modules::tabular_idents::TabularIdentUuid;
use crate::service_modules::Result;
use crate::service_modules::{CatalogBackend, SecretStore, State, Transaction};
use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, RenameTableRequest};
use uuid::Uuid;

pub(crate) async fn rename_view<C: CatalogBackend, A: AuthZHandler, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    let RenameTableRequest {
        source,
        destination,
    } = &request;
    validate_table_or_view_ident(source)?;
    validate_table_or_view_ident(destination)?;

    // ------------------- AUTHZ -------------------
    let source_id = C::view_ident_to_id(
        warehouse_id,
        &request.source,
        state.v1_state.catalog.clone(),
    )
    .await
    // We can't fail before AuthZ.
    .transpose();

    // We need to be allowed to delete the old table and create the new one
    let rename_check = A::check_rename_view(
        &request_metadata,
        warehouse_id,
        source_id.as_ref().and_then(|id| id.as_ref().ok()),
        state.v1_state.auth.clone(),
    );
    let create_check = A::check_create_view(
        &request_metadata,
        warehouse_id,
        &destination.namespace,
        state.v1_state.auth,
    );
    futures::try_join!(rename_check, create_check)?;

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }
    let body = maybe_body_to_json(&request);

    let source_id = source_id.transpose()?.ok_or_else(|| {
        tracing::debug!("View does not exist.");
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("View does not exist in warehouse {warehouse_id}"))
            .r#type("ViewNotFound".to_string())
            .build()
    })?;

    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;

    C::rename_view(
        warehouse_id,
        source_id,
        source,
        destination,
        transaction.transaction(),
    )
    .await?;

    state
        .v1_state
        .contract_verifiers
        .check_rename(TabularIdentUuid::View(*source_id), destination)
        .await?
        .into_result()?;

    transaction.commit().await?;

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "renameView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*source_id),
                warehouse_id: *warehouse_id.as_uuid(),
                name: request.source.name,
                namespace: request.source.namespace.to_url_string(),
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
    use crate::api::iceberg::v1::ViewParameters;
    use crate::catalog::test::{create_view, load_view, setup};
    use crate::service_modules::catalog_backends::implementations::postgres::namespace::tests::initialize_namespace;
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_rename_view_without_namespace(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None, None, None).await;
        let whi = whi.warehouse_id;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = create_view(
            api_context.clone(),
            namespace.namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
        .await
        .unwrap();
        let destination = TableIdent {
            namespace: namespace.namespace.clone(),
            name: "my-renamed-view".to_string(),
        };
        let source = TableIdent {
            namespace: namespace.namespace.clone(),
            name: view_name.to_string(),
        };
        rename_view(
            Some(prefix.clone()),
            RenameTableRequest {
                source: source.clone(),
                destination: destination.clone(),
            },
            api_context.clone(),
            RequestMetadata::new_random(),
        )
        .await
        .unwrap();

        let exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: destination,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .unwrap();

        let not_exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: source,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .expect_err("View should not exist after renaming.");

        assert_eq!(created_view, exists);
        assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
    }

    #[sqlx::test]
    async fn test_rename_view_with_namespace(pool: PgPool) {
        let (api_context, _, whi) = setup(pool, None, None, None).await;
        let whi = whi.warehouse_id;

        let namespace = NamespaceIdent::from_vec(vec!["Someother-ns".to_string()]).unwrap();
        let new_ns = initialize_namespace(
            api_context.v1_state.catalog.clone(),
            whi.into(),
            &namespace,
            None,
        )
        .await
        .namespace;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
        .await
        .unwrap();
        let destination = TableIdent {
            namespace: new_ns.clone(),
            name: "my-renamed-view".to_string(),
        };
        let source = TableIdent {
            namespace: namespace.clone(),
            name: view_name.to_string(),
        };
        rename_view(
            Some(prefix.clone()),
            RenameTableRequest {
                source: source.clone(),
                destination: destination.clone(),
            },
            api_context.clone(),
            RequestMetadata::new_random(),
        )
        .await
        .unwrap();

        let exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: destination,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .unwrap();

        let not_exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: source,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .expect_err("View should not exist after renaming.");

        assert_eq!(created_view, exists);
        assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
    }
}
