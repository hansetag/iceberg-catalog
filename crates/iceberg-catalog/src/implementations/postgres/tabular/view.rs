mod load;

use crate::implementations::postgres::dbutils::DBErrorHandler as _;
use crate::{
    service::{ErrorModel, NamespaceIdentUuid, Result, TableIdent, TableIdentUuid},
    WarehouseIdent,
};

use http::StatusCode;

use crate::implementations::postgres::tabular::{
    create_tabular, drop_tabular, list_tabulars, CreateTabular, TabularIdentBorrowed,
    TabularIdentUuid, TabularType,
};
use crate::implementations::postgres::{tabular, CatalogState};
use chrono::{DateTime, Utc};
use iceberg::spec::{SchemaRef, ViewMetadata, ViewRepresentation, ViewVersionRef};
use iceberg::NamespaceIdent;
use serde::Deserialize;
use sqlx::{FromRow, Postgres, Transaction};
use std::collections::HashMap;
use std::default::Default;
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

pub(crate) async fn create_view(
    namespace_id: NamespaceIdentUuid,
    metadata_location: &str,
    transaction: &mut Transaction<'_, Postgres>,
    name: &str,
    metadata: ViewMetadata,
) -> Result<ViewMetadata> {
    let location = metadata.location.as_str();
    let tabular_id = create_tabular(
        CreateTabular {
            id: metadata.view_uuid,
            name,
            namespace_id: *namespace_id,
            typ: TabularType::View,
            metadata_location: Some(metadata_location),
            location,
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
    for schema in metadata.schemas.values() {
        let schema_id = create_view_schema(view_id, schema.clone(), transaction).await?;
        tracing::debug!("Inserted schema with id: '{}'", schema_id);
    }

    for view_version in metadata.versions.values() {
        let ViewVersionResponse {
            version_id,
            view_id,
        } = create_view_version(view_id, view_version.clone(), transaction).await?;

        tracing::debug!(
            "Inserted view version with id: '{}' for view_id: '{}'",
            version_id,
            view_id
        );
    }

    set_current_view_metadata_version(metadata.current_version_id, metadata.view_uuid, transaction)
        .await?;

    for history in &metadata.version_log {
        insert_view_version_log(
            view_id,
            history.version_id,
            Some(history.timestamp()),
            transaction,
        )
        .await?;
    }

    set_view_properties(metadata.properties(), view_id, transaction).await?;

    tracing::debug!("Inserted view properties for view",);

    Ok(metadata)
}

pub(crate) async fn drop_view<'a>(
    view_id: TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
         DELETE FROM view
         WHERE view_id = $1
         AND view_id IN (select view_id from active_views)
         RETURNING "view_id"
         "#,
        *view_id,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("View not found".to_string())
                .r#type("NoSuchViewError".to_string())
                .build()
        } else {
            tracing::warn!("Error dropping view: {}", e);
            e.into_error_model("Error dropping view".to_string())
        }
    })?;

    drop_tabular(TabularIdentUuid::View(*view_id), transaction).await?;
    Ok(())
}

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_view(
    warehouse_id: WarehouseIdent,
    source_id: TableIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    tabular::rename_tabular(
        warehouse_id,
        TabularIdentUuid::View(*source_id),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

// TODO: do we wanna do this via a trigger?
async fn insert_view_version_log(
    view_id: Uuid,
    version_id: i64,
    timestamp_ms: Option<DateTime<Utc>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
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

pub(crate) async fn set_view_properties(
    properties: &HashMap<String, String>,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .unzip();
    sqlx::query!(
        r#"INSERT INTO view_properties (view_id, key, value)
           VALUES ($1, UNNEST($2::text[]), UNNEST($3::text[]))
              ON CONFLICT (view_id, key)
                DO UPDATE SET value = EXCLUDED.value
           ;"#,
        view_id,
        &keys,
        &vals
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view property".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
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
            .source(Some(Box::new(e)))
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
struct ViewVersionResponse {
    version_id: i64,
    view_id: Uuid,
}

#[allow(clippy::too_many_lines)]
async fn create_view_version(
    view_id: Uuid,
    view_version_request: ViewVersionRef,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<ViewVersionResponse> {
    let view_version = view_version_request;
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
            .source(Some(Box::new(e)))
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

    tracing::debug!(
        "Inserted version: '{}' view metadata version for '{}'",
        version_id,
        view_id
    );

    Ok(insert_response)
}

pub(crate) async fn set_current_view_metadata_version(
    version_id: i64,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
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

    tracing::debug!("Successfully set current view metadata version");
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
) -> Result<()> {
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

    use crate::implementations::postgres::tabular::view::load_view;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::implementations::postgres::CatalogState;

    use crate::service::TableIdentUuid;

    use iceberg::spec::{ViewMetadata, ViewMetadataBuilder};
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::WarehouseIdent;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn view_request(view_id: Option<Uuid>) -> ViewMetadata {
        serde_json::from_value(json!({
  "format-version": 1,
  "view-uuid": view_id.unwrap_or_else(Uuid::now_v7).to_string(),
  "location": "s3://examples/initial-warehouse/86ebaeae-351e-11ef-92b0-f7afa1e3ea1a/01905db5-44b7-7582-80e0-e52cbccdfa0f/metadata/01905db5-4e2d-7691-af92-789507cca618.gz.metadata.json",
  "current-version-id": 2,
  "versions": [
    {
      "version-id": 1,
      "schema-id": 0,
      "timestamp-ms": 1_719_559_079_091_usize,
      "summary": {
        "engine-name": "spark",
        "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
        "engine-version": "3.5.1",
        "app-id": "local-1719559068458"
      },
      "representations": [
        {
          "type": "sql",
          "sql": "select id, strings from spark_demo.my_table",
          "dialect": "spark"
        }
      ],
      "default-namespace": []
    },
    {
      "version-id": 2,
      "schema-id": 1,
      "timestamp-ms": 1_719_559_081_510_usize,
      "summary": {
        "app-id": "local-1719559068458",
        "engine-version": "3.5.1",
        "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
        "engine-name": "spark"
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
  ],
  "version-log": [
    {
      "version-id": 1,
      "timestamp-ms": 1_719_559_079_095_usize
    }
  ],
  "schemas": [
    {
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
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {
          "id": 0,
          "name": "id",
          "required": false,
          "type": "long"
        },
        {
          "id": 1,
          "name": "strings",
          "required": false,
          "type": "string"
        }
      ]
    }
  ],
  "properties": {
    "create_engine_version": "Spark 3.5.1",
    "spark.query-column-names": "id",
    "engine_version": "Spark 3.5.1"
  }
}
 )).unwrap()
    }

    #[sqlx::test]
    async fn create_view(pool: sqlx::PgPool) {
let state = CatalogState::from_pools(pool.clone(), pool.clone());
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
        let table_uuid = TableIdentUuid::from(Uuid::now_v7());

        let request = view_request(Some(*table_uuid));
        let mut tx = pool.begin().await.unwrap();
        let created_meta = super::create_view(
            namespace_id,
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
            "myview",
            request.clone(),
        )
        .await
        .unwrap();

        // recreate with same uuid should fail
        let created_view = super::create_view(
            namespace_id,
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
            "myview",
            request.clone(),
        )
        .await
        .expect_err("recreation should fail");
        assert_eq!(created_view.error.code, 409);

        // recreate with other uuid should fail
        let build = ViewMetadataBuilder::new(request)
            .assign_uuid(Uuid::now_v7())
            .build()
            .unwrap();
        let created_view = super::create_view(
            namespace_id,
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
            "myview",
            build,
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
    async fn drop_view(pool: sqlx::PgPool) {
        let (state, created_meta, _, _, _) = prepare_view(pool).await;
        let mut tx = state.read_pool.begin().await.unwrap();
        super::drop_view(created_meta.view_uuid.into(), &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        load_view(
            created_meta.view_uuid.into(),
            &mut state.read_pool.acquire().await.unwrap(),
        )
        .await
        .expect_err("dropped view should not be loadable");
    }

    #[sqlx::test]
    async fn view_exists(pool: sqlx::PgPool) {
        let (state, created_meta, warehouse_ident, namespace, name) = prepare_view(pool).await;
        let exists = super::view_ident_to_id(
            warehouse_ident,
            &TableIdent {
                namespace: namespace.clone(),
                name,
            },
            &state.read_pool,
        )
        .await
        .unwrap();
        assert_eq!(
            exists,
            Some(created_meta.view_uuid.into()),
            "view should exist"
        );

        assert_eq!(
            super::view_ident_to_id(
                warehouse_ident,
                &TableIdent {
                    namespace,
                    name: "non_existing".to_string(),
                },
                &state.read_pool,
            )
            .await
            .unwrap(),
            None,
            "non existing view should not exist"
        );
    }

    #[sqlx::test]
    async fn drop_view_not_existing(pool: sqlx::PgPool) {
        let (state, _, _, _, _) = prepare_view(pool).await;
        let mut tx = state.read_pool.begin().await.unwrap();
        let e = super::drop_view(Uuid::now_v7().into(), &mut tx)
            .await
            .expect_err("dropping random uuid should not succeed");
        tx.commit().await.unwrap();
        assert_eq!(e.error.code, 404);
    }

    async fn prepare_view(
        pool: PgPool,
    ) -> (
        CatalogState,
        ViewMetadata,
        WarehouseIdent,
        NamespaceIdent,
        String,
    ) {
let state = CatalogState::from_pools(pool.clone(), pool.clone());
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

        let request = view_request(None);

        let mut tx = pool.begin().await.unwrap();
        let created_meta = super::create_view(
            namespace_id,
            "s3://my_bucket/my_table/metadata/bar",
            &mut tx,
            "myview",
            request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        (
            state,
            created_meta,
            warehouse_id,
            namespace,
            "myview".into(),
        )
    }
}
