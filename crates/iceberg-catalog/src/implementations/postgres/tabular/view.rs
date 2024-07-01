mod load;

use crate::implementations::postgres::dbutils::DBErrorHandler as _;
use crate::{
    service::{ErrorModel, NamespaceIdentUuid, Result, TableIdent, TableIdentUuid},
    WarehouseIdent,
};

use http::StatusCode;

use crate::implementations::postgres::tabular::{
    create_tabular, list_tabulars, CreateTabular, TabularIdentBorrowed, TabularIdentUuid,
    TabularType,
};
use crate::implementations::postgres::CatalogState;
use chrono::{DateTime, Utc};
use iceberg::spec::{SchemaRef, ViewMetadata, ViewRepresentation, ViewVersion, ViewVersionRef};
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{CreateViewRequest, IcebergErrorResponse};
use maplit::hashmap;
use serde::Deserialize;
use sqlx::{FromRow, Postgres, Transaction};
use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) use crate::service::ViewMetadataWithLocation;
pub(crate) use load::load_view;

pub(crate) async fn view_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    table: &TableIdent,
    catalog_state: E,
) -> Result<Option<TableIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    crate::implementations::postgres::tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentBorrowed::View(table),
        false,
        catalog_state,
    )
    .await?
    .map(|id| match id {
        TabularIdentUuid::Table(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a table when filtering for views.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
        TabularIdentUuid::View(view) => Ok(view.into()),
    })
    .transpose()
}

pub(crate) enum CreateViewVersion {
    AsCurrent(ViewVersionRef),
}

impl CreateViewVersion {
    fn inner(&self) -> &ViewVersion {
        match self {
            Self::AsCurrent(v) => v.as_ref(),
        }
    }
}

pub(crate) async fn create_view(
    namespace_id: NamespaceIdentUuid,
    view: &TableIdent,
    view_id: TableIdentUuid,
    request: CreateViewRequest,
    metadata_location: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ViewMetadata> {
    let TableIdent { namespace: _, name } = view;
    let CreateViewRequest {
        name: _,
        location,
        schema,
        view_version,
        properties,
    } = request;

    let location = location.ok_or_else(|| {
        // TODO: encode this in the request struct? I.e. decouple rest interface from db interface
        tracing::error!("Server failed to set view location, this should not be possible.");
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Server failed to set view location.".to_string())
            .r#type("SetViewLocationFailed".to_string())
            .build()
    })?;

    let metadata = ViewMetadata {
        format_version: iceberg::spec::ViewFormatVersion::V1,
        view_uuid: *view_id,
        location: metadata_location.to_string(),
        current_version_id: view_version.version_id,
        versions: hashmap!(view_version.version_id => Arc::new(view_version)),
        // we'll populate this on insert
        version_log: vec![],
        schemas: hashmap!(schema.schema_id() => Arc::new(schema)),
        properties,
    };

    let tabular_id = create_tabular(
        CreateTabular {
            id: *view_id,
            name: name.as_str(),
            namespace_id: *namespace_id,
            typ: TabularType::View,
            metadata_location: Some(metadata_location),
            location: &location,
        },
        &mut *transaction,
    )
    .await?;

    let view_id = sqlx::query_scalar!(
        r#"
        INSERT INTO view (view_id, view_format_version)
        VALUES ($1, $2)
        returning view_id
        "#,
        tabular_id,
        ViewFormatVersion::from(metadata.format_version()) as _,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error creating view".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build(),

        _ => e.into_error_model("Error creating view".to_string()),
    })?;

    tracing::debug!("Inserted base view and tabular.");

    let schema_id =
        create_view_schema(view_id, metadata.current_schema().clone(), transaction).await?;

    tracing::debug!("Inserted schema with id: '{}'", schema_id);

    let ViewVersionResponse {
        version_id,
        view_id,
    } = create_view_version(
        view_id,
        CreateViewVersion::AsCurrent(metadata.current_version().clone()),
        transaction,
    )
    .await?;

    tracing::debug!(
        "Created view version with id: '{}' for view_id: '{}'",
        version_id,
        view_id
    );

    insert_view_properties(metadata.properties(), view_id, transaction).await?;

    tracing::debug!("Inserted view properties for view",);

    load_view(TableIdentUuid::from(view_id), transaction)
        .await
        .map(|metadata| metadata.metadata)
}

// TODO: do we wanna do this via a trigger?
async fn insert_view_version_log(
    view_id: Uuid,
    version_id: i64,
    timestamp_ms: Option<DateTime<Utc>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), IcebergErrorResponse> {
    if let Some(ts) = timestamp_ms {
        sqlx::query!(
            r#"
        INSERT INTO view_version_log (view_id, version_id, timestamp)
        VALUES ($1, $2, $3)
        "#,
            view_id,
            version_id,
            ts
        )
    } else {
        sqlx::query!(
            r#"
        INSERT INTO view_version_log (view_id, version_id)
        VALUES ($1, $2)
        "#,
            view_id,
            version_id,
        )
    }
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view version log".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    tracing::debug!("Inserted view version log");
    Ok(())
}

pub(crate) async fn insert_view_properties(
    properties: &HashMap<String, String>,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), IcebergErrorResponse> {
    for (key, value) in properties {
        sqlx::query!(
            r#"
            INSERT INTO view_properties (view_id, key, value)
            VALUES ($1, $2, $3)
            "#,
            view_id,
            key,
            value
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            let message = "Error inserting view property".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?;
    }
    Ok(())
}

pub(crate) async fn create_view_schema(
    view_id: Uuid,
    schema: SchemaRef,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i32> {
    let schema_as_value = serde_json::to_value(&schema).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing view schema".to_string())
            .r#type("ViewSchemaSerializationError".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;
    Ok(sqlx::query_scalar!(
        r#"
        INSERT INTO view_schema (view_id, schema_id, schema)
        VALUES ($1, $2, $3)
        RETURNING schema_id
        "#,
        view_id,
        schema.schema_id(),
        schema_as_value
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::CONFLICT.into())
            .message("View schema already exists".to_string())
            .r#type("ViewSchemaAlreadyExists".to_string())
            .build(),
        _ => e.into_error_model("Error creating view schema".to_string()),
    })?)
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, FromRow, Clone, Copy)]
pub(crate) struct ViewVersionResponse {
    pub(crate) version_id: i64,
    pub(crate) view_id: Uuid,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn create_view_version(
    view_id: Uuid,
    view_version_request: CreateViewVersion,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<ViewVersionResponse> {
    let view_version = view_version_request.inner();
    let version_id = view_version.version_id();
    let schema_id = view_version.schema_id();
    let default_ns = view_version.default_namespace();
    let default_ns = default_ns.clone().inner();
    let default_namespace_id: Option<Uuid> = sqlx::query_scalar!(
        r#"
        SELECT namespace_id
        FROM namespace
        WHERE namespace_name = $1
        "#,
        &default_ns
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error fetching namespace_id".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;

    let default_cat = view_version.default_catalog();
    let summary = serde_json::to_value(view_version.summary()).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing view_version summary".to_string())
            .r#type("ViewSummarySerializationFailed".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let insert_response = sqlx::query_as!(ViewVersionResponse,
                r#"
                    INSERT INTO view_version (view_id, version_id, schema_id, timestamp, default_namespace_id, default_catalog, summary)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    returning view_id, version_id
                "#,
                view_id,
                version_id,
                schema_id,
                view_version.timestamp(),
                default_namespace_id,
                default_cat,
                summary
            )
        .fetch_one(&mut **transaction)
        .await.map_err(|e| {
            if let sqlx::Error::RowNotFound = e {
                let message = "View version already exists";
                tracing::debug!(?e,"{}", message);
                ErrorModel::builder()
                    .code(StatusCode::CONFLICT.into())
                    .message(message.to_string())
                    .r#type("ViewVersionAlreadyExists".to_string())
                    .build()
            } else {
                let message = "Error creating view version";
                tracing::warn!(?e, "{} for: '{}'/'{}' with schema_id: '{}' due to: '{}'",
                message,
                view_id,
                version_id,
                schema_id,
                e
            );
                e.into_error_model(message.to_string())
            }
    })?;

    for rep in view_version.representations() {
        insert_representation(rep, transaction, insert_response).await?;
    }

    let CreateViewVersion::AsCurrent(_) = view_version_request;

    set_current_view_metadata_version(version_id, view_id, transaction).await?;

    tracing::debug!(
        "Inserted version: '{}' as current view metadata version for '{}'",
        version_id,
        view_id
    );

    Ok(insert_response)
}

pub(crate) async fn set_current_view_metadata_version(
    version_id: i64,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), IcebergErrorResponse> {
    sqlx::query!(
        r#"
        INSERT INTO current_view_metadata_version (version_id, view_id)
        VALUES ($1, $2)
        ON CONFLICT (view_id)
        DO UPDATE SET version_id = $1
        WHERE current_view_metadata_version.view_id = $2
        "#,
        version_id,
        view_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error setting current view metadata version".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;

    insert_view_version_log(view_id, version_id, None, transaction).await?;
    tracing::debug!(
        "Successfully set current view metadata version and inserted view version log."
    );
    Ok(())
}

pub(crate) async fn list_views(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    catalog_state: CatalogState,
) -> Result<HashMap<TableIdentUuid, TableIdent>> {
    list_tabulars(
        warehouse_id,
        namespace,
        false,
        catalog_state,
        Some(TabularType::View),
    )
    .await?
    .into_iter()
    .map(|(k, v)| match k {
        TabularIdentUuid::Table(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a table when filtering for tables.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
        TabularIdentUuid::View(t) => Ok((TableIdentUuid::from(t), v.into_inner())),
    })
    .collect::<Result<HashMap<TableIdentUuid, TableIdent>>>()
}

async fn insert_representation(
    rep: &ViewRepresentation,
    transaction: &mut Transaction<'_, Postgres>,
    view_version_response: ViewVersionResponse,
) -> Result<(), IcebergErrorResponse> {
    let ViewRepresentation::SqlViewRepresentation(repr) = rep;
    sqlx::query!(
        r#"
            INSERT INTO view_representation (view_id, view_version_id, typ, sql, dialect)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        view_version_response.view_id,
        view_version_response.version_id,
        ViewRepresentationType::from(rep) as _,
        repr.sql.as_str(),
        repr.dialect.as_str()
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view_representation".to_string();
        tracing::warn!(?e, "{}", message);
        e.into_error_model(message)
    })?;
    Ok(())
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "view_format_version", rename_all = "kebab-case")]
pub(crate) enum ViewFormatVersion {
    V1,
}

#[derive(sqlx::Type, Debug, Deserialize)]
#[sqlx(type_name = "view_representation_type", rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ViewRepresentationType {
    Sql,
}

impl From<&iceberg::spec::ViewRepresentation> for ViewRepresentationType {
    fn from(value: &ViewRepresentation) -> Self {
        match value {
            ViewRepresentation::SqlViewRepresentation(_) => Self::Sql,
        }
    }
}

impl From<iceberg::spec::ViewFormatVersion> for ViewFormatVersion {
    fn from(value: iceberg::spec::ViewFormatVersion) -> Self {
        match value {
            iceberg::spec::ViewFormatVersion::V1 => Self::V1,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use std::sync::Arc;

    use crate::implementations::postgres::tabular::view::load_view;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::implementations::postgres::CatalogState;

    use crate::service::TableIdentUuid;

    use iceberg::spec::ViewMetadata;
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use maplit::hashmap;
    use serde_json::json;
    use sqlx::{Acquire, PgPool};
    use uuid::Uuid;

    fn view_request() -> CreateViewRequest {
        serde_json::from_value(json!({
                                  "name": "myview",
                                  "location": "s3://my_bucket/my_table/metadata/bar",
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
                                        "sql": "select id from spark_demo.my_table",
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

    #[sqlx::test]
    async fn create_view(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };
        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                warehouse_id,
                &namespace,
            )
            .await;

        let request = view_request();
        let table_uuid = Uuid::now_v7().into();
        let mut tx = pool.begin().await.unwrap();
        let created_meta = super::create_view(
            namespace_id,
            &TableIdent {
                namespace: namespace.clone(),
                name: request.name.clone(),
            },
            table_uuid,
            request.clone(),
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
        )
        .await
        .unwrap();

        // recreate with same uuid should fail
        let created_view = super::create_view(
            namespace_id,
            &TableIdent {
                namespace: namespace.clone(),
                name: request.name.clone(),
            },
            table_uuid,
            request.clone(),
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
        )
        .await
        .expect_err("recreation should fail");
        assert_eq!(created_view.error.code, 409);

        // recreate with other uuid should fail
        let created_view = super::create_view(
            namespace_id,
            &TableIdent {
                namespace: namespace.clone(),
                name: request.name.clone(),
            },
            Uuid::now_v7().into(),
            request.clone(),
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
        )
        .await
        .expect_err("recreation should fail");
        assert_eq!(created_view.error.code, 409);

        tx.commit().await.unwrap();

        let views = super::list_views(warehouse_id, &namespace, state.clone())
            .await
            .unwrap();
        assert_eq!(views.len(), 1);
        let (view_id, view) = views.into_iter().next().unwrap();
        assert_eq!(view_id, table_uuid);
        assert_eq!(view.name, "myview");

        let mut conn = state.read_pool.acquire().await.unwrap();
        let metadata = load_view(TableIdentUuid::from(created_meta.view_uuid), &mut conn)
            .await
            .unwrap();
        assert_eq!(metadata.metadata, created_meta);
    }

    #[sqlx::test]
    async fn create_view_add_schema(pool: sqlx::PgPool) {
        let (state, created_meta) = prepare_view(pool).await;

        let schema = created_meta
            .current_schema()
            .as_ref()
            .clone()
            .into_builder();

        let schema_ref = Arc::new(schema.with_schema_id(2).build().unwrap());
        let mut tx = state.read_pool.begin().await.unwrap();
        let _ = super::create_view_schema(created_meta.view_uuid, schema_ref.clone(), &mut tx)
            .await
            .unwrap();

        tx.commit().await.unwrap();

        let mut conn = state.read_pool.acquire().await.unwrap();
        let metadata = load_view(TableIdentUuid::from(created_meta.view_uuid), &mut conn)
            .await
            .unwrap();
        assert_eq!(metadata.metadata.schemas.get(&2).unwrap(), &schema_ref);
    }

    #[sqlx::test]
    async fn create_view_add_version_set_version(pool: sqlx::PgPool) {
        let (state, created_meta) = prepare_view(pool).await;

        let rep1 = iceberg::spec::ViewRepresentation::SqlViewRepresentation(
            iceberg::spec::SqlViewRepresentation {
                sql: "select * from my_table".to_string(),
                dialect: "spark".to_string(),
            },
        );

        let rep2 = iceberg::spec::ViewRepresentation::SqlViewRepresentation(
            iceberg::spec::SqlViewRepresentation {
                sql: "select * from my_table".to_string(),
                dialect: "spark".to_string(),
            },
        );

        let view_version = Arc::new(
            iceberg::spec::ViewVersion::builder()
                .with_version_id(2)
                .with_schema_id(0)
                .with_default_namespace(
                    NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap(),
                )
                .with_default_catalog(Some("my_catalog".to_string()))
                .with_summary(hashmap!(
                    "engine-version".into() => "3.5.1".into(),
                ))
                .with_representations(vec![rep1, rep2])
                .with_timestamp_ms(1_719_395_654_343_i64)
                .build(),
        );

        let mut tx = state.read_pool.begin().await.unwrap();
        let _ = super::create_view_version(
            created_meta.view_uuid,
            super::CreateViewVersion::AsCurrent(view_version.clone()),
            &mut tx,
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();

        let mut conn = state.read_pool.acquire().await.unwrap();
        let metadata = load_view(TableIdentUuid::from(created_meta.view_uuid), &mut conn)
            .await
            .unwrap();
        assert_eq!(*metadata.metadata.current_version(), view_version);
        assert_eq!(metadata.metadata.version_log.len(), 2);
        assert_eq!(
            metadata.metadata.version_log.last().map(|e| e.version_id),
            Some(2)
        );

        let mut tx = conn.begin().await.unwrap();
        super::set_current_view_metadata_version(1, created_meta.view_uuid, &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let metadata = load_view(TableIdentUuid::from(created_meta.view_uuid), &mut conn)
            .await
            .unwrap();
        assert_eq!(metadata.metadata.current_version().version_id(), 1);
        assert_eq!(metadata.metadata.version_log.len(), 3);
        assert_eq!(
            metadata.metadata.version_log.last().map(|e| e.version_id),
            Some(1)
        );
    }

    #[sqlx::test]
    async fn create_view_add_prop(pool: sqlx::PgPool) {
        let (state, created_meta) = prepare_view(pool).await;
        let mut tx = state.read_pool.begin().await.unwrap();

        super::insert_view_properties(
            &hashmap!("foo".to_string() => "bar".to_string()),
            created_meta.view_uuid,
            &mut tx,
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();

        let mut conn = state.read_pool.acquire().await.unwrap();
        let metadata = load_view(TableIdentUuid::from(created_meta.view_uuid), &mut conn)
            .await
            .unwrap();
        assert_eq!(
            *metadata.metadata.properties(),
            hashmap!("foo".to_string() => "bar".to_string(),
                     "create_engine_version".into()=> "Spark 3.5.1".into(),
                     "engine_version".into()=> "Spark 3.5.1".into(),
                     "spark.query-column-names".into()=> "id".into())
        );
    }

    async fn prepare_view(pool: PgPool) -> (CatalogState, ViewMetadata) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };
        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                warehouse_id,
                &namespace,
            )
            .await;

        let request = view_request();
        let table_uuid = Uuid::now_v7().into();
        let mut tx = pool.begin().await.unwrap();
        let created_meta = super::create_view(
            namespace_id,
            &TableIdent {
                namespace: namespace.clone(),
                name: request.name.clone(),
            },
            table_uuid,
            request.clone(),
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        (state, created_meta)
    }
}
