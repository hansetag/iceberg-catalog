use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::{DataAccess, NamespaceParameters};
use crate::api::ApiContext;
use crate::catalog::io::write_metadata_file;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::{
    maybe_body_to_json, require_active_warehouse, require_no_location_specified,
    validate_table_or_view_ident,
};
use crate::catalog::views::validate_view_properties;
use crate::request_metadata::RequestMetadata;
use crate::service::auth::AuthZHandler;
use crate::service::event_publisher::EventMetadata;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{Catalog, SecretStore, State, TableIdentUuid, Transaction};
use crate::service::{GetWarehouseResponse, Result};
use http::StatusCode;
use iceberg::spec::ViewMetadataBuilder;
use iceberg::{TableIdent, ViewCreation};
use iceberg_ext::catalog::rest::{CreateViewRequest, ErrorModel, LoadViewResult};
use uuid::Uuid;

// TODO: split up into smaller functions
#[allow(clippy::too_many_lines)]
/// Create a view in the given namespace
pub(crate) async fn create_view<C: Catalog, A: AuthZHandler, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: DataAccess,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    let view = TableIdent::new(namespace.clone(), request.name.clone());

    validate_table_or_view_ident(&view)?;
    require_no_location_specified(&request.location)?;

    if request.location.is_some() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Specifying a View `location` is not supported. Location is managed by the Catalog.".to_string())
            .r#type("LocationNotSupported".to_string())
            .build()
            .into());
    }

    validate_view_properties(request.properties.keys())?;

    if request.view_version.representations().is_empty() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("View must have at least one query.".to_string())
            .r#type("EmptyView".to_string())
            .build()
            .into());
    }

    // ------------------- AUTHZ -------------------
    A::check_create_view(
        &request_metadata,
        warehouse_id,
        &namespace,
        state.v1_state.auth.clone(),
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let namespace_id =
        C::namespace_ident_to_id(warehouse_id, &namespace, state.v1_state.catalog.clone())
            .await?
            .ok_or(
                ErrorModel::builder()
                    .code(StatusCode::NOT_FOUND.into())
                    .message("Namespace does not exist".to_string())
                    .r#type("NamespaceNotFound".to_string())
                    .build(),
            )?;

    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let GetWarehouseResponse {
        id: _,
        name: _,
        project_id: _,
        storage_profile,
        storage_secret_id,
        status,
    } = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
    require_active_warehouse(status)?;

    let view_id: TabularIdentUuid = TabularIdentUuid::View(uuid::Uuid::now_v7());

    let view_location = storage_profile.tabular_location(namespace_id, view_id);
    let mut request = request;
    let metadata_location = storage_profile.metadata_location(&view_location, *view_id);
    request.location = Some(view_location.clone());
    let request = request;
    // serialize body before moving it
    let body = maybe_body_to_json(&request);
    let view_creation = ViewMetadataBuilder::from_view_creation(ViewCreation {
        name: view.name.clone(),
        location: view_location,
        representations: request.view_version.representations().clone(),
        schema: request.schema,
        properties: request.properties.clone(),
        default_namespace: request.view_version.default_namespace().clone(),
        default_catalog: request.view_version.default_catalog().cloned(),
        summary: request.view_version.summary().clone(),
    })
    .unwrap()
    .assign_uuid(*view_id.as_ref());

    let metadata = C::create_view(
        namespace_id,
        &view,
        view_creation.build().map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!("Failed to create view metadata: {e}"))
                .r#type("ViewMetadataCreationFailed".to_string())
                .build()
        })?,
        &metadata_location,
        transaction.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret = if let Some(secret_id) = &storage_secret_id {
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

    let file_io = storage_profile.file_io(storage_secret.as_ref())?;
    write_metadata_file(metadata_location.as_str(), &metadata, &file_io).await?;
    tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

    // Generate the storage profile. This requires the storage secret
    // because the table config might contain vended-credentials based
    // on the `data_access` parameter.
    // ToDo: There is a small inefficiency here: If storage credentials
    // are not required because of i.e. remote-signing and if this
    // is a stage-create, we still fetch the secret.
    let config = storage_profile
        .generate_table_config(
            warehouse_id,
            namespace_id,
            TableIdentUuid::from(*view_id),
            &data_access,
            storage_secret.as_ref(),
        )
        .await?;

    transaction.commit().await?;

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "createView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id: *warehouse_id.as_uuid(),
                name: view.name,
                namespace: view.namespace.encode_in_url(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
        )
        .await;

    let load_view_result = LoadViewResult {
        metadata_location,
        metadata,
        config: Some(config),
    };

    Ok(load_view_result)
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    use crate::catalog::views::test::new_namespace;

    use crate::implementations::postgres::secrets::SecretsState;
    use crate::implementations::AllowAllAuthZHandler;
    use iceberg::NamespaceIdent;
    use serde_json::json;
    use sqlx::PgPool;

    pub(crate) async fn create_view(
        api_context: ApiContext<
            State<AllowAllAuthZHandler, crate::implementations::postgres::Catalog, SecretsState>,
        >,
        namespace: NamespaceIdent,
        rq: CreateViewRequest,
        prefix: Option<String>,
    ) -> Result<LoadViewResult> {
        super::create_view(
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

    #[sqlx::test]
    async fn test_create_view(pool: PgPool) {
        let (api_context, namespace, whi) = crate::catalog::views::test::setup(pool, None).await;

        let mut rq = create_view_request(None, None);

        let _view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .unwrap();
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .expect_err("Recreate with same ident should fail.");
        assert_eq!(view.error.code, 409);
        let old_name = rq.name.clone();
        rq.name = "some-other-name".to_string();

        let _view = create_view(
            api_context.clone(),
            namespace,
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .expect("Recreate with with another name it should work");

        rq.name = old_name;
        let new_ns = new_namespace(
            api_context.v1_state.catalog.clone(),
            whi,
            Some(vec![Uuid::now_v7().to_string()]),
        )
        .await;
        let _view = create_view(api_context, new_ns, rq, Some(whi.to_string()))
            .await
            .expect("Recreate with same name but different ns should work.");
    }

    pub(crate) fn create_view_request(
        name: Option<&str>,
        location: Option<&str>,
    ) -> CreateViewRequest {
        serde_json::from_value(json!({
                                  "name": name.unwrap_or("myview"),
                                  "location": location,
                                  "schema": {
                                    "schema-id": 0,
                                    "type": "struct",
                                    "fields": [
                                      {
                                        "id": 0,
                                        "name": "id",
                                        "required": false,
                                        "type": "long"
                                      }
                                    ]
                                  },
                                  "view-version": {
                                    "version-id": 1,
                                    "schema-id": 0,
                                    "timestamp-ms": 1_719_395_654_343_i64,
                                    "summary": {
                                      "engine-version": "3.5.1",
                                      "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
                                      "engine-name": "spark",
                                      "app-id": "local-1719395622847"
                                    },
                                    "representations": [
                                      {
                                        "type": "sql",
                                        "sql": "select id, xyz from spark_demo.my_table",
                                        "dialect": "spark"
                                      }
                                    ],
                                    "default-namespace": []
                                  },
                                  "properties": {
                                    "create_engine_version": "Spark 3.5.1",
                                    "engine_version": "Spark 3.5.1",
                                    "spark.query-column-names": "id"
                                  }})).unwrap()
    }
}
