use crate::implementations::postgres::{dbutils::DBErrorHandler as _, CatalogState};
use crate::{
    service::{ErrorModel, NamespaceIdentUuid, Result, TableIdent, TableIdentUuid},
    WarehouseIdent,
};

use http::StatusCode;
use iceberg_ext::NamespaceIdent;

use crate::implementations::postgres::tabular::{
    create_tabular, drop_tabular, list_tabulars, CreateTabular, TabularIdentOwned, TabularIdentRef,
    TabularIdentUuid, TabularType,
};
use chrono::{DateTime, Utc};
use iceberg::spec::{
    Schema, SchemaRef, ViewMetadata, ViewMetadataBuilder, ViewRepresentation, ViewVersion,
    ViewVersionLog, ViewVersionRef,
};
use iceberg::ViewCreation;
use iceberg_ext::catalog::rest::{CreateViewRequest, IcebergErrorResponse};
use sqlx::{FromRow, PgConnection, Postgres, Transaction};
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

#[instrument(skip(catalog_state))]
pub(crate) async fn view_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: &WarehouseIdent,
    table: &TableIdent,
    catalog_state: E,
) -> Result<Option<TableIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    crate::implementations::postgres::tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentRef::View(table),
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

pub(crate) async fn view_idents_to_ids<'e, 'c: 'e, E>(
    warehouse_id: &WarehouseIdent,
    tables: HashSet<&TableIdent>,
    include_staged: bool,
    catalog_state: E,
) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let table_map = crate::implementations::postgres::tabular::tabular_idents_to_ids(
        warehouse_id,
        tables.into_iter().map(TabularIdentRef::Table).collect(),
        include_staged,
        catalog_state,
    )
    .await?
    .into_iter()
    .map(|(k, v)| match k {
        TabularIdentOwned::Table(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a table when filtering for views.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
        TabularIdentOwned::View(t) => Ok((t, v.map(|v| TableIdentUuid::from(*v)))),
    })
    .collect::<Result<HashMap<_, Option<TableIdentUuid>>>>()?;

    Ok(table_map)
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

#[instrument(skip(transaction))]
pub(crate) async fn create_view(
    namespace_id: &NamespaceIdentUuid,
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
    } = dbg!(request);

    let location = location.ok_or_else(|| {
        // TODO: encode this in the function signature / request struct? We shouldn't fail here when
        //       we set the location in the handler fn.
        tracing::error!("Server failed to set view location, this should not be possible.");
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Server failed to set view location.".to_string())
            .r#type("SetViewLocationFailed".to_string())
            .build()
    })?;

    let summary = view_version.summary().clone();
    let representations = view_version.representations().clone();

    // TODO: how to not go the request -> creation -> builder -> metadata route?
    //       some of the request stuff is not properly validated, e.g. default_namespace() can be empty
    //       when trying to read this from DB, NamespaceIdent::from_vec says no to an empty vec and we
    //       have to hack it as a vec![""]. Going via ViewMetadataBuilder further renumbers versions
    //       and it also doesn't seem to produce the a ViewVersionLog entry for the current version.
    //       This is all not very nice, maybe it's best to move all the heavy lifting into the
    //       service layer.
    let vc = ViewCreation {
        name: name.clone(),
        location,
        representations: representations.clone(),
        schema,
        properties,
        default_namespace: view_version.default_namespace().clone(),
        default_catalog: view_version.default_catalog().map(ToString::to_string),
        summary: summary.clone(),
    };

    let metadata = dbg!(ViewMetadataBuilder::from_view_creation(vc)
        .map_err(|e| {
            // TODO: can we be more specific about errors here?
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error creating view metadata".to_string())
                .r#type("ViewMetadataCreationError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
        .assign_uuid(view_id.into_uuid())
        // assigning uuid should be infallible, why isn't it?
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error assigning view UUID".to_string())
                .r#type("ViewUUIDAssignmentError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?
        .build()
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error building the viewmetadata".to_string())
                .r#type("ViewUUIDAssignmentError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?);

    let tabular_id = create_tabular(
        CreateTabular {
            id: view_id.into_uuid(),
            name: name.as_str(),
            namespace_id: namespace_id.into_uuid(),
            typ: TabularType::View,
            metadata_location: Some(metadata_location),
        },
        &mut *transaction,
    )
    .await?;
    eprintln!("Done tabular");
    let view_id = sqlx::query_scalar!(
        r#"
        INSERT INTO view (view_id, view_format_version, location, metadata_location)
        VALUES ($1, $2, $3, $4)
        returning view_id
        "#,
        tabular_id,
        ViewFormatVersion::from(metadata.format_version()) as _,
        // TODO: ViewCreation sets location to location so it ends up as metadata.location is this different from
        // metadata_location?
        metadata.location(),
        metadata_location,
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

    let (version_id, version_uuid) = create_view_version(
        view_id,
        // We gotta use metadata.current_version() here since ViewMetadataBuilder::from_view_creation
        // renumbers the version_ids, 1 becomes 0 if there's no other version, that sounds troublesome?
        CreateViewVersion::AsCurrent(metadata.current_version().clone()),
        transaction,
    )
    .await?;

    tracing::debug!(
        "Created view version with id: '{}' and version_uuid: '{}'",
        version_id,
        version_uuid
    );

    insert_view_properties(&metadata, view_id, transaction).await?;

    tracing::debug!("Inserted view properties for view",);

    Ok(metadata)
}

// TODO: do we wanna do this via a trigger?
async fn insert_view_version_log(
    view_id: Uuid,
    version_id: i64,
    // TODO: which one to use here? Metadata will create its own on build but using metadata to create
    //       views is..messy.
    timestamp_ms: Option<DateTime<Utc>>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), IcebergErrorResponse> {
    sqlx::query!(
        r#"
        INSERT INTO view_version_log (id, view_id, version_id, timestamp)
        VALUES ($1, $2, $3, $4)
        "#,
        Uuid::now_v7(),
        view_id,
        version_id,
        timestamp_ms.unwrap_or_else(Utc::now)
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting view version log".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    Ok(())
}

async fn insert_view_properties(
    metadata: &ViewMetadata,
    view_id: Uuid,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), IcebergErrorResponse> {
    for (key, value) in metadata.properties() {
        sqlx::query!(
            r#"
            INSERT INTO view_properties (property_id, view_id, key, value)
            VALUES ($1, $2, $3, $4)
            "#,
            Uuid::now_v7(),
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

async fn create_view_schema(
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
        INSERT INTO view_schema (schema_uuid, view_id, schema_id, schema)
        VALUES ($1, $2, $3, $4)
        RETURNING schema_id
        "#,
        Uuid::now_v7(),
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

async fn create_view_version(
    view_id: Uuid,
    view_version_request: CreateViewVersion,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(i64, Uuid)> {
    let view_version = view_version_request.inner();
    let version_id = view_version.version_id();
    let schema_id = view_version.schema_id();

    let val = serde_json::to_value(&view_version).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing view version".to_string())
            .r#type("ViewVersionSerializationError".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;
    let version_uuid = sqlx::query_scalar!(
        r#"
                    INSERT INTO view_version (view_version_uuid, view_id, version_id, schema_id, version)
                    VALUES ($1, $2, $3, $4, $5)
                    returning view_version_uuid
                "#,
        Uuid::now_v7(),
        view_id,
        version_id,
        schema_id,
        val,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            let message = "View version already exists";
            eprintln!("{} {:?}", message, e.to_string());
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(message.to_string())
                .r#type("ViewVersionAlreadyExists".to_string())
                .build()
        }
        _ => {
            let message = "Error creating view version";
            tracing::warn!(
                "{} for: '{}'/'{}' with schema_id: '{}' due to: '{}'",
                message,
                view_id,
                version_id,
                schema_id,
                e
            );
            e.into_error_model(message.to_string())
        }
    })?;

    let CreateViewVersion::AsCurrent(_) = view_version_request;

    // TODO: metadatabuilder doesn't set this on its own, why?
    insert_view_version_log(view_id, version_id, None, transaction).await?;

    // TODO: make this a trigger?
    sqlx::query!(
        r#"
                INSERT INTO current_view_metadata_version (view_id, version_uuid, version_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (view_id)
                DO UPDATE SET version_uuid = $2, version_id = $3
            "#,
        view_id,
        version_uuid,
        version_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error setting current view version".to_string();
        tracing::warn!(?e, "{}", message);
        e.into_error_model(message)
    })?;

    tracing::debug!(
        "Inserted version: '{}'/'{}' as current view metadata version for '{}'",
        version_uuid,
        version_id,
        view_id
    );

    Ok((version_id, version_uuid))
}

#[derive(FromRow, Debug)]
pub(crate) struct View {
    view_id: uuid::Uuid,
    #[sqlx(rename = "view_format_version: ViewFormatVersion")]
    view_format_version: ViewFormatVersion,
    view_location: String,
    // TODO: We can construct this from uuid + location, why are we storing this?
    metadata_location: Option<String>,
    current_version_id: i64,
}

pub(crate) struct MetadataFetcher {
    warehouse_id: Uuid,
    namespace: NamespaceIdent,
    name: String,
    view: Option<View>,
    properties: Option<HashMap<String, String>>,
    schemas: Option<HashMap<i32, SchemaRef>>,
    versions: Option<HashMap<i64, ViewVersionRef>>,
    version_log: Option<Vec<ViewVersionLog>>,
}

impl MetadataFetcher {
    pub(crate) fn new(
        warehouse_ident: WarehouseIdent,
        namespace_ident: NamespaceIdent,
        name: String,
    ) -> Self {
        Self {
            warehouse_id: *warehouse_ident.as_uuid(),
            namespace: namespace_ident,
            name,
            view: None,
            properties: None,
            schemas: None,
            versions: None,
            version_log: None,
        }
    }

    pub(crate) async fn fetch_metadata(self, conn: &mut PgConnection) -> ViewMetadata {
        let slf = self
            .fetch_view(&mut *conn)
            .await
            .fetch_schemas(&mut *conn)
            .await
            .fetch_properties(&mut *conn)
            .await
            .fetch_versions(&mut *conn)
            .await
            .fetch_version_log(&mut *conn)
            .await;
        let view = slf.view.unwrap();
        ViewMetadataBuilder::from_parts(
            match view.view_format_version {
                ViewFormatVersion::V1 => iceberg::spec::ViewFormatVersion::V1,
            },
            view.view_id,
            view.view_location,
            view.current_version_id,
            slf.versions.unwrap(),
            slf.version_log.unwrap(),
            slf.schemas.unwrap(),
            slf.properties.unwrap(),
        )
        .build()
        .unwrap()
    }

    pub(crate) async fn fetch_version_log(mut self, conn: &mut PgConnection) -> Self {
        self.version_log = Some(
            sqlx::query!(
                r#"
                SELECT version_id, timestamp
                FROM view_version_log
                WHERE view_id = $1
                "#,
                self.view.as_ref().unwrap().view_id
            )
            .fetch_all(conn)
            .await
            .unwrap()
            .into_iter()
            .map(|r| ViewVersionLog {
                version_id: r.version_id,
                timestamp_ms: r.timestamp.timestamp_millis(),
            })
            .collect(),
        );
        self
    }

    pub(crate) async fn fetch_view(mut self, conn: &mut PgConnection) -> Self {
        let view = sqlx::query_as!(
            View,
            r#"
    select
        v.view_id,
        view_format_version as "view_format_version: ViewFormatVersion",
        location as view_location,
        v.metadata_location,
        cvv.version_id as current_version_id
     from view v
        LEFT JOIN view_properties vp ON v.view_id = vp.view_id
        JOIN view_version vv on v.view_id = vv.view_id
        JOIN tabular ta ON v.view_id=ta.tabular_id
        JOIN namespace n ON ta.namespace_id = n.namespace_id
        JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        JOIN current_view_metadata_version cvv ON v.view_id = cvv.view_id
    WHERE n.warehouse_id = $1 AND n.namespace_name = $2 AND ta.name = $3
    AND w.status = 'active' AND ta.typ = 'view'
    GROUP BY v.view_id, cvv.version_id"#,
            self.warehouse_id,
            &self.namespace.clone().inner(),
            &self.name
        )
        .fetch_one(conn)
        .await
        .unwrap();

        self.view = Some(view);
        self
    }

    pub(crate) async fn fetch_schemas(mut self, conn: &mut PgConnection) -> Self {
        let schemas = sqlx::query!(
            r#"
            SELECT schema_id, schema
            FROM view_schema
            WHERE view_id = $1
            "#,
            self.view.as_ref().unwrap().view_id
        )
        .fetch_all(conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| {
            let schema: Schema = serde_json::from_value(r.schema).unwrap();
            (r.schema_id, Arc::new(schema))
        })
        .collect();

        self.schemas = Some(schemas);
        self
    }

    pub(crate) async fn fetch_properties(mut self, conn: &mut PgConnection) -> Self {
        let properties = sqlx::query!(
            r#"
            SELECT key, value
            FROM view_properties
            WHERE view_id = $1
            "#,
            self.view.as_ref().unwrap().view_id
        )
        .fetch_all(conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect();

        self.properties = Some(properties);
        self
    }

    pub(crate) async fn fetch_versions(mut self, conn: &mut PgConnection) -> Self {
        let mut versions = HashMap::new();
        let rows = sqlx::query!(
            r#"SELECT version_id, version FROM view_version WHERE view_id = $1"#,
            self.view.as_ref().unwrap().view_id
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        for r in rows {
            let version: ViewVersion = serde_json::from_value(r.version).unwrap();

            versions.insert(r.version_id, Arc::new(version));
        }

        self.versions = Some(versions);
        self
    }
}

pub(crate) async fn load_view(
    warehouse_id: &WarehouseIdent,
    table: &TableIdent,
    conn: &mut PgConnection,
) -> Result<ViewMetadata> {
    let fetcher = crate::implementations::postgres::tabular::view::MetadataFetcher::new(
        warehouse_id.clone(),
        table.namespace.clone(),
        table.name.clone(),
    );
    Ok(fetcher.fetch_metadata(conn).await)
}

pub(crate) async fn list_views(
    warehouse_id: &WarehouseIdent,
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

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_view(
    warehouse_id: &WarehouseIdent,
    source_id: &TableIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    crate::implementations::postgres::tabular::rename_tabular(
        warehouse_id,
        TabularIdentUuid::View(source_id.into_uuid()),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

pub(crate) async fn drop_view<'a>(
    whi: &WarehouseIdent,
    table_id: &TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
        DELETE FROM view
        WHERE view_id = $1
        AND view_id IN (
            select tabular_id from active_tabulars
            where tabular_id = $1
        )
        RETURNING "view_id"
        "#,
        table_id.as_uuid(),
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

    drop_tabular(
        whi,
        TabularIdentUuid::View(*table_id.as_uuid()),
        transaction,
    )
    .await?;
    Ok(())
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "view_format_version", rename_all = "kebab-case")]
pub(crate) enum ViewFormatVersion {
    V1,
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

    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use serde_json::json;

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
                                    "timestamp-ms": 1719395654343i64,
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
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                &warehouse_id,
                &namespace,
            )
            .await;

        let request = view_request();
        let table_uuid = Uuid::now_v7().into();
        let mut tx = pool.begin().await.unwrap();
        let created_meta = dbg!(
            super::create_view(
                &namespace_id,
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
        )
        .unwrap();

        // recreate with same uuid should fail
        let created_view = super::create_view(
            &namespace_id,
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
            &namespace_id,
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

        let views = super::list_views(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap();
        assert_eq!(views.len(), 1);
        let (view_id, view) = views.into_iter().next().unwrap();
        assert_eq!(view_id, table_uuid);
        assert_eq!(view.name, "myview");
        let mut conn = state.read_pool.acquire().await.unwrap();
        let metadata = load_view(
            &warehouse_id,
            &TableIdent {
                namespace: namespace,
                name: request.name.clone(),
            },
            &mut conn,
        )
        .await
        .unwrap();
        assert_eq!(metadata, created_meta)
    }

    #[sqlx::test]
    async fn test_drop_view(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };
        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;
        let namespace_id =
            crate::implementations::postgres::tabular::table::tests::get_namespace_id(
                state.clone(),
                &warehouse_id,
                &namespace,
            )
            .await;

        let request = view_request();
        let table_uuid = Uuid::now_v7().into();
        let mut tx = pool.begin().await.unwrap();
        let _ = super::create_view(
            &namespace_id,
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

        let views = super::list_views(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap();
        assert_eq!(views.len(), 1);
        let (view_id, view) = views.into_iter().next().unwrap();
        assert_eq!(view_id, table_uuid);
        assert_eq!(view.name, "myview");
        let mut tx = pool.begin().await.unwrap();

        super::drop_view(&warehouse_id, &table_uuid, &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let views = super::list_views(&warehouse_id, &namespace, state.clone())
            .await
            .unwrap();
        assert_eq!(views.len(), 0);
    }
}
