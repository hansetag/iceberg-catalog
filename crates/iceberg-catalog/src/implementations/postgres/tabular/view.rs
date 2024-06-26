use crate::implementations::postgres::{dbutils::DBErrorHandler as _, CatalogState};
use crate::{
    service::{
        storage::StorageProfile, CommitTableResponse, CommitTableResponseExt,
        CommitTransactionRequest, CreateTableRequest, CreateTableResponse, ErrorModel,
        GetStorageConfigResponse, GetTableMetadataResponse, LoadTableResponse, NamespaceIdentUuid,
        Result, TableIdent, TableIdentUuid,
    },
    SecretIdent, WarehouseIdent,
};

use http::StatusCode;
use iceberg_ext::{
    spec::{TableMetadata, TableMetadataAggregate},
    NamespaceIdent, TableRequirement, TableUpdate,
};

use crate::api::{TableRequirementExt as _, TableUpdateExt};
use crate::implementations::postgres::tabular::{
    create_tabular, drop_tabular, list_tabulars, try_parse_namespace_ident, CreateTabular,
    TabularIdentOwned, TabularIdentRef, TabularIdentUuid, TabularType,
};
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use iceberg::spec::{
    Schema, SchemaRef, ViewMetadata, ViewMetadataBuilder, ViewRepresentation, ViewVersion,
    ViewVersionLog,
};
use iceberg::ViewCreation;
use iceberg_ext::catalog::rest::{CreateViewRequest, IcebergErrorResponse, LoadViewResult};
use openssl::derive;
use openssl::version::version;
use serde_json::to_string;
use sqlx::types::Json;
use sqlx::{FromRow, Postgres, Transaction};
use std::default::Default;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};
use tracing::{instrument, metadata};
use utoipa::schema;
use uuid::Uuid;

const MAX_PARAMETERS: usize = 30000;

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

pub enum CreateViewVersion {
    Append(ViewVersion),
    AsCurrent(ViewVersion),
}

impl CreateViewVersion {
    fn inner(&self) -> &ViewVersion {
        match self {
            Self::Append(v) => v,
            Self::AsCurrent(v) => v,
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
        name,
        location,
        schema,
        view_version,
        properties,
    } = request;

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

    let metadata = ViewMetadataBuilder::from_view_creation(vc)
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
        })?;

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
    .map_err(|e| {
        if let e = sqlx::Error::RowNotFound {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error creating view".to_string())
                .r#type("InternalDatabaseError".to_string())
                .build()
        } else {
            e.into_error_model("Error creating view".to_string())
        }
    })?;

    let schema_id =
        create_view_schema(view_id, metadata.current_schema().clone(), transaction).await?;

    let (version_id, version_uuid) = create_view_version(
        view_id,
        CreateViewVersion::AsCurrent(view_version),
        transaction,
    )
    .await?;

    insert_view_properties(&metadata, view_id, transaction).await?;

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
    .map_err(|e| {
        if let e = sqlx::Error::RowNotFound {
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("View schema already exists".to_string())
                .r#type("ViewSchemaAlreadyExists".to_string())
                .build()
        } else {
            e.into_error_model("Error creating view schema".to_string())
        }
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

    let version_uuid = sqlx::query_scalar!(
                r#"
                    INSERT INTO view_version (view_version_uuid, view_id, version_id, schema_id, timestamp)
                    VALUES ($1, $2, $3, $4, $5)
                    returning view_version_uuid
                "#,
                Uuid::now_v7(),
                view_id,
                version_id,
                schema_id,
                view_version.timestamp()
            )
        .fetch_one(&mut **transaction)
        .await.map_err(|e| {
        if let e = sqlx::Error::RowNotFound {
            let message = "View version already exists";
            tracing::debug!("{}", message);
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message(message.to_string())
                .r#type("ViewVersionAlreadyExists".to_string())
                .build()
        } else {
            let message = "Error creating view version";
            tracing::warn!("{} for: '{}'/'{}' with schema_id: '{}' due to: '{}'", message, view_id, version_id, schema_id, e);
            e.into_error_model(message.to_string())
        }
    })?;

    for (k, v) in view_version.summary().into_iter() {
        sqlx::query!(
            r#"
            INSERT INTO metadata_summary (summary_tuple_id, version_id, key, value)
            VALUES ($1, $2, $3, $4)
            "#,
            Uuid::now_v7(),
            version_uuid,
            k,
            v
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            let message = "Error inserting metadata summary".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?;
    }

    for rep in view_version.representations() {
        let ViewRepresentation::SqlViewRepresentation(repr) = rep;
        sqlx::query!(
            r#"
            INSERT INTO view_representation (view_id, view_version_uuid, view_representation_id, typ, sql, dialect)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            view_id,
            version_uuid,
            Uuid::now_v7(),
            ViewRepresentationType::from(rep) as _,
            repr.sql.as_str(),
            repr.dialect.as_str()
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            let message = "Error inserting view_representation".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?;
    }

    match view_version_request {
        CreateViewVersion::Append(_) => {
            todo!()
        }
        CreateViewVersion::AsCurrent(view_version) => {
            insert_view_version_log(view_id, version_id, None, transaction).await?;

            // UPSERT as current view version
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
    }
}

#[derive(FromRow, Debug)]
pub struct BaseQuery {
    view_id: uuid::Uuid,
    #[sqlx(rename = "view_format_version: ViewFormatVersion")]
    view_format_version: ViewFormatVersion,
    view_location: String,
    metadata_location: Option<String>,
    current_version_id: i64,
    property_keys: Vec<String>,
    property_values: Vec<String>,
}

impl BaseQuery {}

pub(crate) async fn load_view(
    warehouse_id: &WarehouseIdent,
    table: &TableIdent,
    catalog_state: CatalogState,
) -> Result<LoadViewResult> {
    let TableIdent { namespace, name } = table;
    let view = (sqlx::query_as!(
        BaseQuery,
        r#"
    select
        v.view_id,
        view_format_version as "view_format_version: ViewFormatVersion",
        location as view_location,
        v.metadata_location,
        cvv.version_id as current_version_id,
        COALESCE(ARRAY_AGG(vp.key) filter (where vp.value is not null), '{}') AS "property_keys!",
        COALESCE(ARRAY_AGG(vp.value) filter (where vp.value is not null), '{}') as "property_values!"
     from view v
        LEFT JOIN view_properties vp ON v.view_id = vp.view_id
        JOIN tabular ta ON v.view_id=ta.tabular_id
        JOIN namespace n ON ta.namespace_id = n.namespace_id
        JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        JOIN current_view_metadata_version cvv ON v.view_id = cvv.view_id
    WHERE n.warehouse_id = $1 AND n.namespace_name = $2 AND ta.name = $3
    AND w.status = 'active' AND ta.typ = 'view'
    GROUP BY v.view_id, cvv.version_id"#,
        warehouse_id.as_uuid(),
        &namespace.clone().inner(),
        name
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .unwrap());
    eprintln!("{:?}", view);
    // let table = sqlx::query!(
    //     r#"
    //     SELECT v.view_id,
    //            view_format_version,
    //            location as view_location,
    //            metadata_location as metadata_location,
    //            current_version_id,
    //            ARRAY_AGG(vm.id) AS ids,
    //            ARRAY_AGG(vm.view_id) AS view_ids,
    //            ARRAY_AGG(vm.version_id) AS version_ids,
    //            ARRAY_AGG(vm.schema_id) AS schema_ids,
    //            ARRAY_AGG(vm.timestamp) AS timestamps,
    //            ARRAY_AGG(vm.created_at) AS created_ats,
    //            ARRAY_AGG(vm.updated_at) AS updated_ats
    //     FROM view v
    //     JOIN current_view_version cvv ON v.view_id = cvv.view_id
    //     JOIN view_metadata_versions vmv ON v.view_id = vmv.view_id
    //     JOIN metadata_summary ms on vmv version_id = ms.version_id AND ms.view_id = v.view_id
    //     JOIN view_metadata_version_representation vmvr ON vmv.id = vmvr.id
    //
    //     GROUP BY v.view_id;
    //     "#,
    // )
    // .fetch_one(&catalog_state.read_pool)
    // .await
    // .map_err(|e| match e {
    //     sqlx::Error::RowNotFound => ErrorModel::builder()
    //         .code(StatusCode::NOT_FOUND.into())
    //         .message("Table not found".to_string())
    //         .r#type("NoSuchTableError".to_string())
    //         .build(),
    //     _ => e.into_error_model("Error fetching table".to_string()),
    // })?;

    todo!()
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

#[derive(sqlx::Type, Debug)]
#[sqlx(type_name = "view_representation_type", rename_all = "kebab-case")]
pub(crate) enum ViewRepresentationType {
    SQL,
}

impl From<&iceberg::spec::ViewRepresentation> for ViewRepresentationType {
    fn from(value: &ViewRepresentation) -> Self {
        match value {
            ViewRepresentation::SqlViewRepresentation(_) => Self::SQL,
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
    use crate::implementations::postgres::tabular::table::tests::initialize_table;
    use crate::implementations::postgres::tabular::view::load_view;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::implementations::postgres::CatalogState;
    use crate::service::TableIdentUuid;
    use iceberg::spec::{NestedField, Schema, SqlViewRepresentation, ViewVersion};
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::OnceLock;
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
        let created_view = dbg!(
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

        let metadata = load_view(
            &warehouse_id,
            &TableIdent {
                namespace: namespace,
                name: request.name.clone(),
            },
            state,
        )
        .await
        .unwrap();
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
        let created_view = dbg!(
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
