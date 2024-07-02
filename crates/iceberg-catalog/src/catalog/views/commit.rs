use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, DataAccess, ErrorModel, LoadViewResult, Prefix, Result,
    TableIdent, ViewParameters,
};
use crate::catalog::io::write_metadata_file;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::{
    maybe_body_to_json, require_active_warehouse, validate_table_or_view_ident,
};
use crate::catalog::views::validate_view_updates_updates;
use crate::request_metadata::RequestMetadata;
use crate::service::contract_verification::ContractVerification;
use crate::service::event_publisher::EventMetadata;
use crate::service::tabular_idents::TabularIdentUuid;
use crate::service::{
    auth::AuthZHandler, secrets::SecretStore, Catalog, GetWarehouseResponse, State, TableIdentUuid,
    Transaction,
};
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{IcebergErrorResponse, UpgradeFormatVersionUpdate, ViewUpdate};
use iceberg_ext::catalog::ViewRequirement;
use std::sync::Arc;
use uuid::Uuid;

/// Commit updates to a view
// TODO: break up into smaller fns
#[allow(clippy::too_many_lines)]
pub(crate) async fn commit_view<C: Catalog, A: AuthZHandler, S: SecretStore>(
    parameters: ViewParameters,
    mut request: CommitViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: DataAccess,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;

    if let Some(identifier) = &request.identifier {
        if identifier != &parameters.view {
            // When querying a branch, spark sends something like:
            // namespace: (<my>, <namespace>, <table_name>)
            // table_name: branch_<branch_name>
            let ns_parts = parameters.view.namespace.clone().inner();
            let table_name_candidate = if ns_parts.len() >= 2 {
                NamespaceIdent::from_vec(
                    ns_parts.iter().take(ns_parts.len() - 1).cloned().collect(),
                )
                .ok()
                .map(|n| TableIdent::new(n, ns_parts.last().cloned().unwrap_or_default()))
            } else {
                None
            };

            if table_name_candidate != Some(identifier.clone()) {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message(
                        "Table identifier in path does not match the one in the request body"
                            .to_string(),
                    )
                    .r#type("TableIdentifierMismatch".to_string())
                    .build()
                    .into());
            }
        }
    }

    if request.identifier.is_none() {
        request.identifier = Some(parameters.view.clone());
    }
    if let Some(ref mut identifier) = request.identifier {
        validate_table_or_view_ident(identifier)?;
    }

    let CommitViewRequest {
        identifier,
        requirements,
        updates,
    } = &request;

    validate_view_updates_updates(updates)?;

    identifier
        .as_ref()
        .map(validate_table_or_view_ident)
        .transpose()?;

    if let Some(identifier) = identifier {
        if identifier != &parameters.view {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(
                    "View identifier in path does not match the one in the request body"
                        .to_string(),
                )
                .r#type("ViewIdentifierMismatch".to_string())
                .build()
                .into());
        }
    }

    // ------------------- AUTHZ -------------------
    let table_id = C::view_ident_to_id(
        warehouse_id,
        &parameters.view,
        state.v1_state.catalog.clone(),
    )
    .await
    // We can't fail before AuthZ.
    .ok()
    .flatten();

    A::check_commit_view(
        &request_metadata,
        warehouse_id,
        table_id.as_ref(),
        Some(&parameters.view.namespace),
        state.v1_state.auth,
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let namespace_id = C::namespace_ident_to_id(
        warehouse_id,
        &parameters.view.namespace,
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

    let view_id = table_id.ok_or_else(|| {
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!("Table does not exist in warehouse {warehouse_id}"))
            .r#type("TableNotFound".to_string())
            .build()
    })?;

    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;

    let GetWarehouseResponse {
        id: _,
        name: _,
        project_id: _,
        storage_profile,
        storage_secret_id,
        status,
    } = C::get_warehouse(warehouse_id, transaction.transaction()).await?;
    require_active_warehouse(status)?;

    for assertion in requirements.as_deref().unwrap_or(&[]) {
        match assertion {
            ViewRequirement::AssertViewUuid(uuid) => {
                if uuid.uuid != *view_id {
                    return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message("View UUID does not match".to_string())
                        .r#type("ViewUuidMismatch".to_string())
                        .build()
                        .into());
                }
            }
        }
    }

    let before_update_metadata = C::load_view(view_id, transaction.transaction()).await?;

    state
        .v1_state
        .contract_verifiers
        .check_view_updates(updates, &before_update_metadata)
        .await?
        .into_result()?;
    // serialize body before moving it
    let body = maybe_body_to_json(&request);

    let mut last_added_schema_id = None;
    let mut last_version = None;
    for upd in request.updates {
        match upd {
            ViewUpdate::AssignUuid(_) => {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("Assigning UUIDs is not supported".to_string())
                    .r#type("AssignUuidNotSupported".to_string())
                    .build()
                    .into());
            }
            ViewUpdate::SetLocation(_) => {
                return Err(ErrorModel::builder()
                    .code(StatusCode::BAD_REQUEST.into())
                    .message("Setting location is not supported".to_string())
                    .r#type("SetLocationNotSupported".to_string())
                    .build()
                    .into());
            }

            ViewUpdate::UpgradeFormatVersion(UpgradeFormatVersionUpdate { format_version }) => {
                match format_version {
                    1 => {
                        // No-op
                    }
                    _ => {
                        return Err(ErrorModel::builder()
                            .code(StatusCode::BAD_REQUEST.into())
                            .message("Format version not supported".to_string())
                            .r#type("FormatVersionNotSupported".to_string())
                            .build()
                            .into());
                    }
                }
            }
            ViewUpdate::AddSchema(iceberg_ext::catalog::rest::AddSchemaUpdate {
                schema,
                last_column_id: _,
            }) => {
                let new_id =
                    C::add_view_schema(view_id, Arc::new(schema), transaction.transaction())
                        .await?;
                last_added_schema_id = Some(new_id);
            }
            ViewUpdate::SetProperties(props) => {
                C::insert_view_properties(view_id, &props.updates, transaction.transaction())
                    .await?;
            }
            ViewUpdate::RemoveProperties(props) => {
                C::delete_view_properties(view_id, &props.removals, transaction.transaction())
                    .await?;
            }
            ViewUpdate::AddViewVersion(vv) => {
                let mut view_version = vv.view_version;
                if view_version.schema_id == -1 {
                    view_version.schema_id =
                        last_added_schema_id.ok_or(IcebergErrorResponse::from(
                            ErrorModel::builder()
                                .code(StatusCode::BAD_REQUEST.into())
                                .message(
                                    "-1 is only valid as a schema if one is added before"
                                        .to_string(),
                                )
                                .r#type("SchemaIdNotSet".to_string())
                                .build(),
                        ))?;
                }
                last_version = Some(
                    C::create_view_version(
                        view_id,
                        Arc::new(view_version),
                        transaction.transaction(),
                    )
                    .await?,
                );
            }
            ViewUpdate::SetCurrentViewVersion(scvv) => {
                let version_id = if scvv.view_version_id == -1 {
                    last_version
                        .as_ref()
                        .ok_or(IcebergErrorResponse::from(
                            ErrorModel::builder()
                                .code(StatusCode::BAD_REQUEST.into())
                                .message(
                                    "-1 is only valid as a view version if one is added before"
                                        .to_string(),
                                )
                                .r#type("ViewVersionIdNotSet".to_string())
                                .build(),
                        ))?
                        .version_id
                } else {
                    i64::from(scvv.view_version_id)
                };
                C::set_current_view_version(view_id, version_id, transaction.transaction()).await?;
            }
        }
    }
    let tab_location =
        storage_profile.tabular_location(namespace_id, TabularIdentUuid::View(*view_id));
    let metadata_location = storage_profile.metadata_location(&tab_location, Uuid::now_v7());

    C::update_view_metadata_location(view_id, &metadata_location, transaction.transaction())
        .await?;
    let updated_meta = C::load_view(view_id, transaction.transaction()).await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret = if let Some(secret_id) = &storage_secret_id {
        Some(
            S::get_secret_by_id(secret_id, state.v1_state.secrets)
                .await?
                .secret,
        )
    } else {
        None
    };

    let file_io = storage_profile.file_io(storage_secret.as_ref())?;
    write_metadata_file(metadata_location.as_str(), &updated_meta, &file_io).await?;
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
            "commitView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id: *warehouse_id,
                name: parameters.view.name,
                namespace: parameters.view.namespace.encode_in_url(),
                prefix: parameters
                    .prefix
                    .map(Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
        )
        .await;

    return Ok(LoadViewResult {
        metadata_location,
        metadata: updated_meta,
        config: Some(config),
    });
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::v1::{views, DataAccess, Prefix, ViewParameters};
    
    
    
    
    
    
    
    
    
    
    
    
    
    use iceberg::{TableIdent};
    use iceberg_ext::catalog::rest::{CommitViewRequest};
    use maplit::hashmap;

    use serde_json::json;
    use sqlx::PgPool;

    
    use crate::catalog::views::create::test::{create_view, create_view_request};
    use crate::catalog::views::test::setup;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_commit_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();
        let view_name = "myview";
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        )
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(Some(view.metadata.view_uuid));

        let res = super::commit_view(
            views::ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(
                    namespace.inner().into_iter().chain([view_name.into()]),
                )
                .unwrap(),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
        .unwrap();

        assert_eq!(res.metadata.current_version_id, 2);
        assert_eq!(res.metadata.schemas.len(), 3);
        assert_eq!(res.metadata.versions.len(), 2);
        let max_schema = res.metadata.schemas.keys().max();
        assert_eq!(
            res.metadata.current_version().schema_id,
            *max_schema.unwrap()
        );

        assert_eq!(
            res.metadata.properties,
            hashmap! {
                "create_engine_version".to_string() => "Spark 3.5.1".to_string(),
                "spark.query-column-names".to_string() => "id".to_string(),
            }
        );
    }

    #[sqlx::test]
    async fn test_commit_view_fails_with_wrong_assertion(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool).await;
        let prefix = whi.to_string();
        let view_name = "myview";
        let _ = create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        )
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(Some(Uuid::now_v7()));

        let err = super::commit_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(
                    namespace.inner().into_iter().chain([view_name.into()]),
                )
                .unwrap(),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_random(),
        )
        .await
        .expect_err("This unexpectedly didn't fail the uuid assertion.");
        assert_eq!(err.error.code, 400);
        assert_eq!(err.error.r#type, "ViewUuidMismatch");
    }

    fn spark_commit_update_request(asserted_uuid: Option<Uuid>) -> CommitViewRequest {
        let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
            u.to_string()
        });
        serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "uuid": &uuid
    }
  ],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "create_engine_version": "Spark 3.5.1",
        "spark.query-column-names": "id",
        "engine_version": "Spark 3.5.1"
      }
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 1,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "id",
            "required": false,
            "type": "long",
            "doc": "id of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 2,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "id",
            "required": false,
            "type": "long",
            "doc": "id of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-view-version",
      "view-version": {
        "version-id": 2,
        "schema-id": -1,
        "timestamp-ms": 1_719_494_740_509_i64,
        "summary": {
          "engine-name": "spark",
          "engine-version": "3.5.1",
          "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
          "app-id": "local-1719494665567"
        },
        "representations": [
          {
            "type": "sql",
            "sql": "select id from spark_demo.my_table",
            "dialect": "spark"
          }
        ],
        "default-namespace": []
      }
    },
    {
        "action": "remove-properties",
        "removals": ["engine_version"]
    },
    {
      "action": "set-current-view-version",
      "view-version-id": -1
    }
  ]
})).unwrap()
    }
}
