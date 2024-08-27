use crate::implementations::postgres::{dbutils::DBErrorHandler as _, CatalogState};
use crate::service::TableCommit;
use crate::{
    service::{
        storage::StorageProfile, CreateTableRequest, CreateTableResponse, ErrorModel,
        GetTableMetadataResponse, LoadTableResponse, NamespaceIdentUuid, Result, TableIdent,
        TableIdentUuid,
    },
    SecretIdent, WarehouseIdent,
};

use http::StatusCode;
use iceberg_ext::{
    spec::{TableMetadata, TableMetadataAggregate},
    NamespaceIdent,
};

use crate::api::iceberg::v1::{PaginatedTabulars, PaginationQuery};
use crate::implementations::postgres::tabular::{
    create_tabular, drop_tabular, list_tabulars, try_parse_namespace_ident, CreateTabular,
    TabularIdentBorrowed, TabularIdentOwned, TabularIdentUuid, TabularType,
};
use sqlx::types::Json;
use std::default::Default;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

const MAX_PARAMETERS: usize = 30000;

pub(crate) async fn table_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    table: &TableIdent,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<Option<TableIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    crate::implementations::postgres::tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentBorrowed::Table(table),
        list_flags,
        catalog_state,
    )
    .await?
    .map(|id| match id {
        TabularIdentUuid::Table(tab) => Ok(tab.into()),
        TabularIdentUuid::View(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a view when filtering for tables.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
    })
    .transpose()
}

pub(crate) async fn table_idents_to_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    tables: HashSet<&TableIdent>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let table_map = crate::implementations::postgres::tabular::tabular_idents_to_ids(
        warehouse_id,
        tables
            .into_iter()
            .map(TabularIdentBorrowed::Table)
            .collect(),
        list_flags,
        catalog_state,
    )
    .await?
    .into_iter()
    .map(|(k, v)| match k {
        TabularIdentOwned::Table(t) => Ok((t, v.map(|v| TableIdentUuid::from(*v)))),
        TabularIdentOwned::View(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a view when filtering for tables.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
    })
    .collect::<Result<HashMap<_, Option<TableIdentUuid>>>>()?;

    Ok(table_map)
}

pub(crate) async fn create_table(
    namespace_id: NamespaceIdentUuid,
    table: &TableIdent,
    table_id: TableIdentUuid,
    request: CreateTableRequest,
    // Metadata location may be none if stage-create is true
    metadata_location: Option<&str>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateTableResponse> {
    let TableIdent { namespace: _, name } = table;
    let CreateTableRequest {
        name: _,
        location,
        schema,
        partition_spec,
        write_order,
        // Stage-create is already handled in the catalog service.
        // If stage-create is true, the metadata_location is None,
        // otherwise, it is the location of the metadata file.
        stage_create: _,
        properties,
    } = request;

    let location = location.ok_or_else(|| {
        ErrorModel::builder()
            .code(StatusCode::CONFLICT.into())
            .message("Table location is required".to_string())
            .r#type("CreateTableLocationRequired".to_string())
            .build()
    })?;

    let mut builder = TableMetadataAggregate::new(location.clone(), schema);
    if let Some(partition_spec) = partition_spec {
        builder.add_partition_spec(partition_spec)?;
        builder.set_default_partition_spec(-1)?;
    }
    if let Some(write_order) = write_order {
        builder.add_sort_order(write_order)?;
        builder.set_default_sort_order(-1)?;
    }
    builder.set_properties(properties.unwrap_or_default())?;
    builder.assign_uuid(*table_id)?;

    let table_metadata = builder.build()?;

    let table_metadata_ser = serde_json::to_value(table_metadata.clone()).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing table metadata".to_string())
            .r#type("TableMetadataSerializationError".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;

    let tabular_id = create_tabular(
        CreateTabular {
            id: *table_id,
            name,
            namespace_id: *namespace_id,
            typ: TabularType::Table,
            metadata_location,
            location: location.as_str(),
        },
        &mut *transaction,
    )
    .await?;

    let _update_result = sqlx::query!(
        r#"
        INSERT INTO "table" (table_id, "metadata")
        (
            SELECT $1, $2
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.table_id = $1))
        ON CONFLICT ON CONSTRAINT "table_pkey"
        DO UPDATE SET "metadata" = $2
        RETURNING "table_id"
        "#,
        tabular_id,
        table_metadata_ser,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error creating table: {}", e);
        e.into_error_model("Error creating table".to_string())
    })?;

    Ok(CreateTableResponse { table_metadata })
}

pub(crate) async fn load_tables(
    warehouse_id: WarehouseIdent,
    tables: impl IntoIterator<Item = TableIdentUuid>,
    include_deleted: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
    let tables = sqlx::query!(
        r#"
        SELECT
            t."table_id",
            ti."namespace_id",
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            ti.location as "table_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1
        AND w.status = 'active'
        AND (ti.deleted_at IS NULL OR $3)
        AND t."table_id" = ANY($2)
        "#,
        *warehouse_id,
        &tables.into_iter().map(Into::into).collect::<Vec<_>>(),
        include_deleted
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error fetching table".to_string()))?;

    Ok(tables
        .into_iter()
        .map(|table| {
            let table_id = table.table_id.into();
            (
                table_id,
                LoadTableResponse {
                    table_id,
                    namespace_id: table.namespace_id.into(),
                    table_metadata: table.metadata.deref().clone(),
                    metadata_location: table.metadata_location,
                    storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
                    storage_profile: table.storage_profile.deref().clone(),
                },
            )
        })
        .collect())
}

pub(crate) async fn list_tables(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
    pagination_query: PaginationQuery,
) -> Result<PaginatedTabulars<TableIdentUuid, TableIdent>> {
    let tabulars = list_tabulars(
        warehouse_id,
        namespace,
        list_flags,
        catalog_state,
        Some(TabularType::Table),
        pagination_query,
    )
    .await?;
    let tables = tabulars
        .tabulars
        .into_iter()
        .map(|(k, v)| match k {
            TabularIdentUuid::Table(t) => Ok((TableIdentUuid::from(t), v.into_inner())),
            TabularIdentUuid::View(_) => Err(ErrorModel::internal(
                "DB returned a view when filtering for tables.",
                "InternalDatabaseError",
                None,
            )),
        })
        .collect::<Result<HashMap<TableIdentUuid, TableIdent>, ErrorModel>>()?;
    Ok(PaginatedTabulars {
        tabulars: tables,
        next_page_token: tabulars.next_page_token,
    })
}

pub(crate) async fn get_table_metadata_by_id(
    warehouse_id: WarehouseIdent,
    table: TableIdentUuid,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
) -> Result<GetTableMetadataResponse> {
    let table = sqlx::query!(
        r#"
        SELECT
            t."table_id",
            ti.name as "table_name",
            ti.location as "table_location",
            namespace_name,
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1 AND t."table_id" = $2
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $3)
        "#,
        *warehouse_id,
        *table,
        list_flags.include_deleted
    )
    .fetch_one(&catalog_state.read_pool())
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table not found".to_string())
            .r#type("NoSuchTableError".to_string())
            .build(),
        _ => e.into_error_model("Error fetching table".to_string()),
    })?;

    if !list_flags.include_staged && table.metadata_location.is_none() {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table is staged and not yet created".to_string())
            .r#type("TableStaged".to_string())
            .build()
            .into());
    }

    let namespace = try_parse_namespace_ident(table.namespace_name)?;

    Ok(GetTableMetadataResponse {
        table: TableIdent {
            namespace,
            name: table.table_name,
        },
        table_id: table.table_id.into(),
        warehouse_id,
        location: table.table_location,
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    })
}

pub(crate) async fn get_table_metadata_by_s3_location(
    warehouse_id: WarehouseIdent,
    location: &str,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
) -> Result<GetTableMetadataResponse> {
    // Location might also be a subpath of the table location.
    // We need to make sure that the location starts with the table location.
    let table = sqlx::query!(
        r#"
        SELECT
            t."table_id",
            ti.name as "table_name",
            ti.location as "table_location",
            namespace_name,
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1
            AND $2 like ti."location" || '%'
            AND LENGTH(ti."location") <= $3
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $4)

        "#,
        *warehouse_id,
        location,
        i32::try_from(location.len()).unwrap_or(i32::MAX),
        list_flags.include_deleted
    )
    .fetch_one(&catalog_state.read_pool())
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table not found".to_string())
            .r#type("NoSuchTableError".to_string())
            .stack(vec![
                location.to_string(),
                format!("Warehouse: {}", warehouse_id),
            ])
            .build(),
        _ => e.into_error_model("Error fetching table".to_string()),
    })?;

    if !list_flags.include_staged && table.metadata_location.is_none() {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table is staged and not yet created".to_string())
            .r#type("TableStaged".to_string())
            .build()
            .into());
    }

    let namespace = try_parse_namespace_ident(table.namespace_name)?;

    Ok(GetTableMetadataResponse {
        table: TableIdent {
            namespace,
            name: table.table_name,
        },
        table_id: table.table_id.into(),
        warehouse_id,
        location: table.table_location,
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    })
}

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_table(
    warehouse_id: WarehouseIdent,
    source_id: TableIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    crate::implementations::postgres::tabular::rename_tabular(
        warehouse_id,
        TabularIdentUuid::Table(*source_id),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

pub(crate) async fn drop_table<'a>(
    table_id: TableIdentUuid,
    hard_delete: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    drop_tabular(TabularIdentUuid::Table(*table_id), hard_delete, transaction).await
}

pub(crate) async fn commit_table_transaction<'a>(
    // We do not need the warehouse_id here, because table_ids are unique across warehouses
    _: WarehouseIdent,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    if commits.len() > (MAX_PARAMETERS / 4) {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Too updates in single commit".to_string())
            .r#type("TooManyTablesForCommit".to_string())
            .build()
            .into());
    }

    let mut query_builder_table = sqlx::QueryBuilder::new(
        r#"
        UPDATE "table" as t
        SET "metadata" = c."metadata"
        FROM (VALUES
        "#,
    );

    let mut query_builder_tabular = sqlx::QueryBuilder::new(
        r#"
        UPDATE "tabular" as t
        SET "metadata_location" = c."metadata_location",
        "location" = c."location"
        FROM (VALUES
        "#,
    );

    for (i, commit) in commits.iter().enumerate() {
        let metadata_ser = serde_json::to_value(&commit.new_metadata).map_err(|e| {
            ErrorModel::internal(
                "Error serializing table metadata",
                "TableMetadataSerializationError",
                Some(Box::new(e)),
            )
        })?;

        query_builder_table.push("(");
        query_builder_table.push_bind(commit.new_metadata.uuid());
        query_builder_table.push(", ");
        query_builder_table.push_bind(metadata_ser);
        query_builder_table.push(")");

        query_builder_tabular.push("(");
        query_builder_tabular.push_bind(commit.new_metadata.uuid());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(commit.new_metadata_location.to_string());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(commit.new_metadata.location());
        query_builder_tabular.push(")");

        if i != commits.len() - 1 {
            query_builder_table.push(", ");
            query_builder_tabular.push(", ");
        }
    }

    query_builder_table.push(") as c(table_id, metadata) WHERE c.table_id = t.table_id");
    query_builder_tabular.push(
        ") as c(table_id, metadata_location, location) WHERE c.table_id = t.tabular_id AND t.typ = 'table'",
    );

    query_builder_table.push(" RETURNING t.table_id");
    query_builder_tabular.push(" RETURNING t.tabular_id");

    let query_meta_update = query_builder_table.build();
    let query_meta_location_update = query_builder_tabular.build();

    // futures::try_join didn't work due to concurrent mutable borrow of transaction
    let updated_meta = query_meta_update
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| e.into_error_model("Error committing tablemetadata updates".to_string()))?;

    let updated_meta_location = query_meta_location_update
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_error_model("Error committing tablemetadata location updates".to_string())
        })?;

    if updated_meta.len() != commits.len() || updated_meta_location.len() != commits.len() {
        return Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error committing table updates".to_string())
            .r#type("CommitTableUpdateError".to_string())
            .build()
            .into());
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    // Desired behaviour:
    // - Stage-Create => Load fails with 404
    // - No Stage-Create => Next create fails with 409, load succeeds
    // - Stage-Create => Next stage-create works & overwrites
    // - Stage-Create => Next regular create works & overwrites

    use std::str::FromStr as _;

    use super::*;

    use crate::api::iceberg::types::PageToken;
    use crate::api::management::v1::warehouse::WarehouseStatus;
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use crate::implementations::postgres::warehouse::set_warehouse_status;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::service::ListFlags;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, UnboundPartitionSpec};
    use iceberg::NamespaceIdent;
    use iceberg_ext::configs::Location;

    fn create_request(
        stage_create: Option<bool>,
        table_name: Option<String>,
    ) -> (CreateTableRequest, Option<String>) {
        let metadata_location = if let Some(stage_create) = stage_create {
            if stage_create {
                None
            } else {
                Some("s3://my_bucket/my_table/metadata/foo".to_string())
            }
        } else {
            Some("s3://my_bucket/my_table/metadata/bar".to_string())
        };

        (
            CreateTableRequest {
                name: table_name.unwrap_or("my_table".to_string()),
                location: Some("s3://my_bucket/my_table".to_string()),
                schema: Schema::builder()
                    .with_fields(vec![
                        NestedField::required(
                            1,
                            "id",
                            iceberg::spec::Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        NestedField::required(
                            1,
                            "name",
                            iceberg::spec::Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
                partition_spec: Some(UnboundPartitionSpec {
                    spec_id: None,
                    fields: vec![],
                }),
                write_order: None,
                stage_create,
                properties: None,
            },
            metadata_location,
        )
    }

    pub(crate) async fn get_namespace_id(
        state: CatalogState,
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
    ) -> NamespaceIdentUuid {
        let namespace = sqlx::query!(
            r#"
            SELECT namespace_id
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_name = $2
            "#,
            *warehouse_id,
            &**namespace
        )
        .fetch_one(&state.read_pool())
        .await
        .unwrap();
        namespace.namespace_id.into()
    }

    pub(crate) struct InitializedTable {
        #[allow(dead_code)]
        pub(crate) namespace_id: NamespaceIdentUuid,
        pub(crate) namespace: NamespaceIdent,
        pub(crate) table_id: TableIdentUuid,
        pub(crate) table_ident: TableIdent,
    }

    pub(crate) async fn initialize_table(
        warehouse_id: WarehouseIdent,
        state: CatalogState,
        staged: bool,
        namespace: Option<NamespaceIdent>,
        table_name: Option<String>,
    ) -> InitializedTable {
        // my_namespace_<uuid>
        let namespace = if let Some(namespace) = namespace {
            namespace
        } else {
            let namespace =
                NamespaceIdent::from_vec(vec![format!("my_namespace_{}", uuid::Uuid::now_v7())])
                    .unwrap();
            initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
            namespace
        };
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(staged), table_name);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = state.write_pool().begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let _create_result = create_table(
            namespace_id,
            &table_ident,
            table_id,
            request.clone(),
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        InitializedTable {
            namespace_id,
            namespace,
            table_id,
            table_ident,
        }
    }

    #[sqlx::test]
    async fn test_final_create(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(None, None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let create_result = create_table(
            namespace_id,
            &table_ident,
            table_id,
            request.clone(),
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();
        // Second create should fail
        let create_err = create_table(
            namespace_id,
            &table_ident,
            uuid::Uuid::now_v7().into(),
            request,
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap_err();
        assert_eq!(
            create_err.error.code,
            StatusCode::CONFLICT,
            "{create_err:?}"
        );

        // Load should succeed
        let mut t = pool.begin().await.unwrap();
        let load_result = load_tables(warehouse_id, vec![table_id], false, &mut t)
            .await
            .unwrap();
        assert_eq!(load_result.len(), 1);
        assert_eq!(
            load_result.get(&table_id).unwrap().table_metadata,
            create_result.table_metadata
        );
    }

    #[sqlx::test]
    async fn test_stage_create(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(true), None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let _create_result = create_table(
            namespace_id,
            &table_ident,
            table_id,
            request.clone(),
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Its staged - should not have metadata_location
        let load = load_tables(
            warehouse_id,
            vec![table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load.len(), 1);
        assert!(load.get(&table_id).unwrap().metadata_location.is_none());

        // Second create should succeed, even with different id
        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let create_result = create_table(
            namespace_id,
            &table_ident,
            table_id,
            request,
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(create_result.table_metadata, create_result.table_metadata);

        // We can overwrite the table with a regular create
        let (request, metadata_location) = create_request(Some(false), None);
        let mut transaction = pool.begin().await.unwrap();
        let create_result = create_table(
            namespace_id,
            &table_ident,
            table_id,
            request,
            metadata_location.as_deref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let load_result = load_tables(
            warehouse_id,
            vec![table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load_result.len(), 1);
        assert_eq!(
            load_result.get(&table_id).unwrap().table_metadata,
            create_result.table_metadata
        );
        assert_eq!(
            load_result.get(&table_id).unwrap().metadata_location,
            metadata_location
        );
    }

    #[sqlx::test]
    async fn test_to_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = table_ident_to_id(
            warehouse_id,
            &table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());
        drop(table_ident);

        let table = initialize_table(warehouse_id, state.clone(), true, None, None).await;

        // Table is staged - no result if include_staged is false
        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_to_ids(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = table_idents_to_ids(
            warehouse_id,
            vec![&table_ident].into_iter().collect(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.len() == 1 && exists.get(&table_ident).unwrap().is_none());
        drop(table_ident);

        let table_1 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let mut tables = HashSet::new();
        tables.insert(&table_1.table_ident);

        // Table is staged - no result if include_staged is false
        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 1);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 1);
        assert_eq!(
            exists.get(&table_1.table_ident).unwrap(),
            &Some(table_1.table_id)
        );

        // Second Table
        let table_2 = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        tables.insert(&table_2.table_ident);

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 2);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());
        assert_eq!(
            exists.get(&table_2.table_ident).unwrap(),
            &Some(table_2.table_id)
        );

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 2);
        assert_eq!(
            exists.get(&table_1.table_ident).unwrap(),
            &Some(table_1.table_id)
        );
        assert_eq!(
            exists.get(&table_2.table_ident).unwrap(),
            &Some(table_2.table_id)
        );
    }

    #[sqlx::test]
    async fn test_rename_without_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let new_table_ident = TableIdent {
            namespace: table.namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            warehouse_id,
            table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        // Table id should be the same
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_rename_with_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let new_namespace = NamespaceIdent::from_vec(vec!["new_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &new_namespace, None).await;

        let new_table_ident = TableIdent {
            namespace: new_namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            warehouse_id,
            table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags::default(),
            state.clone(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);

        let table1 = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let tables = list_tables(
            warehouse_id,
            &table1.namespace,
            ListFlags::default(),
            state.clone(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table1.table_id), Some(&table1.table_ident));

        let table2 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let tables = list_tables(
            warehouse_id,
            &table2.namespace,
            ListFlags::default(),
            state.clone(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);
        let tables = list_tables(
            warehouse_id,
            &table2.namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            state.clone(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table2.table_id), Some(&table2.table_ident));
    }

    #[sqlx::test]
    async fn test_list_tables_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags::default(),
            state.clone(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);

        let _ = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace.clone()),
            Some("t1".into()),
        )
        .await;
        let table2 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            Some("t2".into()),
        )
        .await;
        let table3 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            Some("t3".into()),
        )
        .await;

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            state.clone(),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 2);

        assert_eq!(tables.get(&table2.table_id), Some(&table2.table_ident));

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            state.clone(),
            PaginationQuery {
                page_token: PageToken::Present(tables.next_page_token.unwrap()),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table3.table_id), Some(&table3.table_ident));

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            state.clone(),
            PaginationQuery {
                page_token: PageToken::Present(tables.next_page_token.unwrap()),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);
        assert!(tables.next_page_token.is_none());
    }

    #[sqlx::test]
    async fn test_commit_transaction(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table1 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let table2 = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        let _ = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let loaded_tables = load_tables(
            warehouse_id,
            vec![table1.table_id, table2.table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(loaded_tables.len(), 2);
        assert!(loaded_tables
            .get(&table1.table_id)
            .unwrap()
            .metadata_location
            .is_none());
        assert!(loaded_tables
            .get(&table2.table_id)
            .unwrap()
            .metadata_location
            .is_some());

        let table1_metadata = &loaded_tables.get(&table1.table_id).unwrap().table_metadata;
        let table2_metadata = &loaded_tables.get(&table2.table_id).unwrap().table_metadata;

        let mut builder1 = TableMetadataAggregate::new_from_metadata(table1_metadata.clone());
        builder1
            .set_properties(HashMap::from_iter(vec![(
                "t1_key".to_string(),
                "t1_value".to_string(),
            )]))
            .unwrap();
        let mut builder2 = TableMetadataAggregate::new_from_metadata(table2_metadata.clone());
        builder2
            .set_properties(HashMap::from_iter(vec![(
                "t2_key".to_string(),
                "t2_value".to_string(),
            )]))
            .unwrap();
        let updated_metadata1 = builder1.build().unwrap();
        let updated_metadata2 = builder2.build().unwrap();

        let commits = vec![
            TableCommit {
                new_metadata: updated_metadata1.clone(),
                new_metadata_location: Location::from_str("s3://my_bucket/table1/metadata/foo")
                    .unwrap(),
            },
            TableCommit {
                new_metadata: updated_metadata2.clone(),
                new_metadata_location: Location::from_str("s3://my_bucket/table2/metadata/foo")
                    .unwrap(),
            },
        ];

        let mut transaction = pool.begin().await.unwrap();
        commit_table_transaction(warehouse_id, commits.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let loaded_tables = load_tables(
            warehouse_id,
            vec![table1.table_id, table2.table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(loaded_tables.len(), 2);

        let loaded_metadata1 = &loaded_tables.get(&table1.table_id).unwrap().table_metadata;
        let loaded_metadata2 = &loaded_tables.get(&table2.table_id).unwrap().table_metadata;

        assert_eq!(loaded_metadata1, &updated_metadata1);
        assert_eq!(loaded_metadata2, &updated_metadata2);
    }

    #[sqlx::test]
    async fn test_get_metadata_by_location(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let metadata = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();

        // Exact path works
        let metadata = get_table_metadata_by_s3_location(
            warehouse_id,
            &metadata.location,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(metadata.table, table.table_ident);
        assert_eq!(metadata.table_id, table.table_id);

        // Subpath works
        let metadata = get_table_metadata_by_s3_location(
            warehouse_id,
            &format!("{}/data/foo.parquet", &metadata.location),
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();

        // Shorter path does not work
        get_table_metadata_by_s3_location(
            warehouse_id,
            &metadata.location[0..metadata.location.len() - 1],
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap_err();
    }

    #[sqlx::test]
    async fn test_cannot_get_table_of_inactive_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        let mut transaction = pool.begin().await.unwrap();
        set_warehouse_status(warehouse_id, WarehouseStatus::Inactive, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let err = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_drop_table_works(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let mut transaction = pool.begin().await.unwrap();
        drop_table(table.table_id, false, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let err = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, StatusCode::NOT_FOUND);

        let ok = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags {
                include_deleted: true,
                ..ListFlags::default()
            },
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(ok.table_id, table.table_id);
    }
}
