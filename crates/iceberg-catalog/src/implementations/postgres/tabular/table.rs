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
use sqlx::types::Json;
use std::default::Default;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};
use tracing::instrument;

const MAX_PARAMETERS: usize = 30000;

pub(crate) async fn table_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: &WarehouseIdent,
    table: &TableIdent,
    include_staged: bool,
    catalog_state: E,
) -> Result<Option<TableIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    crate::implementations::postgres::tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentRef::Table(table),
        include_staged,
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

#[instrument(skip(transaction))]
pub(crate) async fn create_table(
    namespace_id: &NamespaceIdentUuid,
    table: &TableIdent,
    table_id: &TableIdentUuid,
    request: CreateTableRequest,
    // Metadata location may be none if stage-create is true
    metadata_location: Option<&String>,
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
    builder.assign_uuid(table_id.as_uuid().to_owned())?;

    let table_metadata = builder.build()?;

    let table_metadata_ser = serde_json::to_value(table_metadata.clone()).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing table metadata".to_string())
            .r#type("TableMetadataSerializationError".to_string())
            .stack(Some(vec![e.to_string()]))
            .build()
    })?;

    let tabular_id = create_tabular(
        CreateTabular {
            id: table_id.into_uuid(),
            name,
            namespace_id: namespace_id.into_uuid(),
            typ: TabularType::Table,
            metadata_location: metadata_location.map(std::string::String::as_str),
            location: location.as_str(),
        },
        &mut *transaction,
    )
    .await?;

    // ToDo: Should we keep the old table_id?
    let _update_result = sqlx::query!(
        r#"
        INSERT INTO "table" (table_id, "metadata")
        (
            SELECT $1, $2
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.tabular_id = $1))
        ON CONFLICT ON CONSTRAINT "table_pkey"
        DO UPDATE SET "metadata" = $2
        RETURNING "table_id"
        "#,
        tabular_id,
        table_metadata_ser,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::CONFLICT.into())
            .message("Table already exists in Namespace".to_string())
            .r#type("TableAlreadyExists".to_string())
            .build(),
        _ => e.as_error_model("Error creating table".to_string()),
    })?;

    Ok(CreateTableResponse { table_metadata })
}

pub(crate) async fn load_table(
    warehouse_id: &WarehouseIdent,
    table: &TableIdent,
    catalog_state: CatalogState,
) -> Result<LoadTableResponse> {
    let TableIdent { namespace, name } = table;

    let table = sqlx::query!(
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
        WHERE w.warehouse_id = $1 AND namespace_name = $2 AND ti.name = $3
        AND w.status = 'active'
        AND ti."metadata_location" IS NOT NULL
        "#,
        warehouse_id.as_uuid(),
        &**namespace,
        &**name
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table not found".to_string())
            .r#type("NoSuchTableError".to_string())
            .build(),
        _ => e.into_error_model("Error fetching table".to_string()),
    })?;

    Ok(LoadTableResponse {
        table_id: table.table_id.into(),
        namespace_id: table.namespace_id.into(),
        table_metadata: table.metadata.deref().clone(),
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    })
}

pub(crate) async fn list_tables(
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    include_staged: bool,
    catalog_state: CatalogState,
) -> Result<HashMap<TableIdentUuid, TableIdent>> {
    list_tabulars(
        warehouse_id,
        namespace,
        include_staged,
        catalog_state,
        Some(TabularType::Table),
    )
    .await?
    .into_iter()
    .map(|(k, v)| match k {
        TabularIdentUuid::Table(t) => Ok((TableIdentUuid::from(t), v.into_inner())),
        TabularIdentUuid::View(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a view when filtering for tables.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
    })
    .collect::<Result<HashMap<TableIdentUuid, TableIdent>>>()
}

pub(crate) async fn get_table_metadata_by_id(
    warehouse_id: &WarehouseIdent,
    table: &TableIdentUuid,
    include_staged: bool,
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
        "#,
        warehouse_id.as_uuid(),
        table.as_uuid()
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table not found".to_string())
            .r#type("NoSuchTableError".to_string())
            .build(),
        _ => e.into_error_model("Error fetching table".to_string()),
    })?;

    if !include_staged && table.metadata_location.is_none() {
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
        warehouse_id: warehouse_id.clone(),
        location: table.table_location,
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    })
}

pub(crate) async fn get_table_metadata_by_s3_location(
    warehouse_id: &WarehouseIdent,
    location: &str,
    include_staged: bool,
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
        "#,
        warehouse_id.as_uuid(),
        location,
        i32::try_from(location.len()).unwrap_or(i32::MAX)
    )
    .fetch_one(&catalog_state.read_pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Table not found".to_string())
            .r#type("NoSuchTableError".to_string())
            .stack(Some(vec![
                location.to_string(),
                format!("Warehouse: {}", warehouse_id),
            ]))
            .build(),
        _ => e.into_error_model("Error fetching table".to_string()),
    })?;

    if !include_staged && table.metadata_location.is_none() {
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
        warehouse_id: warehouse_id.clone(),
        location: table.table_location,
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    })
}

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_table(
    warehouse_id: &WarehouseIdent,
    source_id: &TableIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    crate::implementations::postgres::tabular::rename_tabular(
        warehouse_id,
        TabularIdentUuid::Table(source_id.into_uuid()),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

pub(crate) async fn drop_table<'a>(
    whi: &WarehouseIdent,
    table_id: &TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
        DELETE FROM "table"
        WHERE table_id = $1
        AND table_id IN (
            select table_id from active_tables
        )
        RETURNING "table_id"
        "#,
        table_id.as_uuid(),
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Table not found".to_string())
                .r#type("NoSuchTableError".to_string())
                .build()
        } else {
            tracing::warn!("Error dropping tabular: {}", e);
            e.into_error_model("Error dropping table".to_string())
        }
    })?;

    drop_tabular(
        whi,
        TabularIdentUuid::Table(*table_id.as_uuid()),
        transaction,
    )
    .await?;
    Ok(())
}

#[derive(Debug)]
struct CommitContext {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
    storage_profile: StorageProfile,
    storage_secret_ident: Option<SecretIdent>,
    #[allow(dead_code)]
    namespace_id: NamespaceIdentUuid,
    metadata: TableMetadata,
    metadata_location: Option<String>,
}

async fn get_commit_context<'a>(
    request: CommitTransactionRequest,
    table_ids: &HashMap<TableIdent, TableIdentUuid>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<CommitContext>> {
    let CommitTransactionRequest { table_changes } = request;

    let metadata = sqlx::query!(
        r#"
        SELECT 
            t."table_id", 
            t."metadata" as "metadata: Json<TableMetadata>", 
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id",
            n.namespace_id
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE "table_id" = ANY($1)
        AND w.status = 'active'
        "#,
        &table_ids
            .values()
            .map(|id| id.as_uuid().clone())
            .collect::<Vec<_>>()
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error fetching table metadata for commit".to_string()))?;

    let mut contexts = vec![];
    for change in table_changes {
        let table_ident = change.identifier.ok_or(
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Table identifier must be specified for all changes".to_string())
                .r#type("TableIdentifierRequired".to_string())
                .build(),
        )?;
        let table_id = table_ids.get(&table_ident).ok_or_else(|| {
            ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Table identifier not found".to_string())
                .r#type("TableIdentifierNotFound".to_string())
                .stack(Some(vec![format!("{:?}", table_ident)]))
                .build()
        })?;

        let record = metadata
            .iter()
            .find(|m| &m.table_id == table_id.as_uuid())
            .ok_or_else(|| {
                ErrorModel::builder()
                    .code(StatusCode::NOT_FOUND.into())
                    .message("Table not found".to_string())
                    .r#type("NoSuchTableError".to_string())
                    .stack(Some(vec![format!("Table Ident {:?}", table_ident)]))
                    .build()
            })?;

        contexts.push(CommitContext {
            requirements: change.requirements,
            updates: change.updates,
            storage_profile: record.storage_profile.deref().clone(),
            metadata: record.metadata.deref().clone(),
            metadata_location: record.metadata_location.clone(),
            storage_secret_ident: record.storage_secret_id.map(SecretIdent::from),
            namespace_id: record.namespace_id.into(),
        });
    }

    Ok(contexts)
}

fn apply_commits(commits: Vec<CommitContext>) -> Result<Vec<CommitTableResponseExt>> {
    // ToDo: Set default snapshot retention
    let mut responses = vec![];
    for context in commits {
        let previous_location = context.metadata.location.clone();
        let previous_uuid = context.metadata.uuid();
        let metadata_id = uuid::Uuid::now_v7();
        let metadata_location = context
            .storage_profile
            .metadata_location(&previous_location, &metadata_id);
        let previous_table_metadata = context.metadata.clone();
        let mut builder = TableMetadataAggregate::new_from_metadata(context.metadata);
        for update in context.updates {
            match &update {
                TableUpdate::AssignUuid { uuid } => {
                    if uuid != &previous_uuid {
                        return Err(ErrorModel::builder()
                            .code(StatusCode::BAD_REQUEST.into())
                            .message("Cannot assign a new UUID".to_string())
                            .r#type("AssignUuidNotAllowed".to_string())
                            .build()
                            .into());
                    }
                }
                TableUpdate::SetLocation { location } => {
                    if location != &previous_location {
                        return Err(ErrorModel::builder()
                            .code(StatusCode::BAD_REQUEST.into())
                            .message("Cannot change table location".to_string())
                            .r#type("SetLocationNotAllowed".to_string())
                            .build()
                            .into());
                    }
                }
                _ => {
                    TableUpdateExt::apply(update, &mut builder)?;
                }
            }
        }
        let new_metadata = builder.build()?;
        responses.push(CommitTableResponseExt {
            commit_response: CommitTableResponse {
                metadata_location: metadata_location.clone(),
                metadata: new_metadata.clone(),
                config: None,
            },
            storage_config: GetStorageConfigResponse {
                storage_profile: context.storage_profile,
                storage_secret_ident: context.storage_secret_ident,
            },
            previous_table_metadata,
        });
    }

    Ok(responses)
}

pub(crate) async fn commit_table_transaction<'a>(
    // We do not need the warehouse_id here, because table_ids are unique across warehouses
    _: &WarehouseIdent,
    request: CommitTransactionRequest,
    table_ids: &HashMap<TableIdent, TableIdentUuid>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<CommitTableResponseExt>> {
    let contexts = get_commit_context(request, table_ids, transaction).await?;

    if contexts.len() > (MAX_PARAMETERS / 4) {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Too updates in single commit".to_string())
            .r#type("TooManyTablesForCommit".to_string())
            .build()
            .into());
    }

    // Check all requirements
    for context in &contexts {
        context
            .requirements
            .iter()
            .map(|r| r.assert(&context.metadata, context.metadata_location.is_some()))
            .collect::<Result<Vec<_>>>()?;
    }

    // Apply updates
    let responses = apply_commits(contexts)?;

    let mut query_builder_metadata = sqlx::QueryBuilder::new(
        r#"
        UPDATE "table" as t
        SET "metadata" = c."metadata"
        FROM (VALUES
        "#,
    );

    let mut query_builder_metadata_location = sqlx::QueryBuilder::new(
        r#"
        UPDATE "tabular" as t
        SET "metadata_location" = c."metadata_location"
        FROM (VALUES
        "#,
    );

    for (i, response) in responses.iter().enumerate() {
        let metadata_ser =
            serde_json::to_value(&response.commit_response.metadata).map_err(|e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error serializing table metadata".to_string())
                    .r#type("TableMetadataSerializationError".to_string())
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?;

        query_builder_metadata.push("(");
        query_builder_metadata.push_bind(response.commit_response.metadata.uuid());
        query_builder_metadata.push(", ");
        query_builder_metadata.push_bind(metadata_ser);
        query_builder_metadata.push(")");

        query_builder_metadata_location.push("(");
        query_builder_metadata_location.push_bind(response.commit_response.metadata.uuid());
        query_builder_metadata_location.push(", ");
        query_builder_metadata_location
            .push_bind(response.commit_response.metadata_location.clone());
        query_builder_metadata_location.push(")");

        if i != responses.len() - 1 {
            query_builder_metadata.push(", ");
            query_builder_metadata_location.push(", ");
        }
    }

    query_builder_metadata.push(") as c(table_id, metadata) WHERE c.table_id = t.table_id");
    query_builder_metadata_location.push(
        ") as c(table_id, metadata_location) WHERE c.table_id = t.tabular_id AND t.typ = 'table'",
    );

    query_builder_metadata.push(" RETURNING t.table_id");
    query_builder_metadata_location.push(" RETURNING t.tabular_id");

    let query_meta_update = query_builder_metadata.build();
    let query_meta_location_update = query_builder_metadata_location.build();

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

    if updated_meta.len() != responses.len() || updated_meta_location.len() != responses.len() {
        return Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error committing table updates".to_string())
            .r#type("CommitTableUpdateError".to_string())
            .build()
            .into());
    }

    Ok(responses)
}

#[cfg(test)]
pub(crate) mod tests {
    // Desired behaviour:
    // - Stage-Create => Load fails with 404
    // - No Stage-Create => Next create fails with 409, load succeeds
    // - Stage-Create => Next stage-create works & overwrites
    // - Stage-Create => Next regular create works & overwrites

    use crate::api::management::v1::warehouse::WarehouseStatus;
    use crate::api::CommitTableRequest;
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use crate::implementations::postgres::warehouse::set_warehouse_status;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, UnboundPartitionSpec};
    use iceberg::NamespaceIdent;

    use super::*;

    fn create_request(stage_create: Option<bool>) -> (CreateTableRequest, Option<String>) {
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
                name: "my_table".to_string(),
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
        warehouse_id: &WarehouseIdent,
        namespace: &NamespaceIdent,
    ) -> NamespaceIdentUuid {
        let namespace = sqlx::query!(
            r#"
            SELECT namespace_id
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_name = $2
            "#,
            warehouse_id.as_uuid(),
            &**namespace
        )
        .fetch_one(&state.read_pool)
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
        warehouse_id: &WarehouseIdent,
        state: CatalogState,
        staged: bool,
    ) -> InitializedTable {
        // my_namespace_<uuid>
        let namespace =
            NamespaceIdent::from_vec(vec![format!("my_namespace_{}", uuid::Uuid::now_v7())])
                .unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(staged));
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = state.write_pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let _create_result = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request.clone(),
            metadata_location.as_ref(),
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
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), &warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let create_result = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request.clone(),
            metadata_location.as_ref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();
        // Second create should fail
        let table_id = uuid::Uuid::now_v7().into();
        let create_err = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request,
            metadata_location.as_ref(),
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
        let load_result = load_table(&warehouse_id, &table_ident, state.clone())
            .await
            .unwrap();
        assert_eq!(load_result.table_metadata, create_result.table_metadata);
    }

    #[sqlx::test]
    async fn test_stage_create(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), &warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(true));
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let _create_result = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request.clone(),
            metadata_location.as_ref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Load should fail
        let load_err = load_table(&warehouse_id, &table_ident, state.clone())
            .await
            .unwrap_err();
        assert_eq!(load_err.error.code, StatusCode::NOT_FOUND);

        // Second create should succeed
        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let create_result = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request,
            metadata_location.as_ref(),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(create_result.table_metadata, create_result.table_metadata);

        // We can overwrite the table with a regular create
        let (request, metadata_location) = create_request(Some(false));
        let mut transaction = pool.begin().await.unwrap();
        let create_result = create_table(
            &namespace_id,
            &table_ident,
            &table_id,
            request,
            dbg!(metadata_location.as_ref()),
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let load_result = load_table(&warehouse_id, &table_ident, state.clone())
            .await
            .unwrap();
        assert_eq!(load_result.table_metadata, create_result.table_metadata);
    }

    #[sqlx::test]
    async fn test_to_id(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = table_ident_to_id(&warehouse_id, &table_ident, false, &state.read_pool)
            .await
            .unwrap();
        assert!(exists.is_none());
        drop(table_ident);

        let table = initialize_table(&warehouse_id, state.clone(), true).await;

        // Table is staged - no result if include_staged is false
        let exists = table_ident_to_id(&warehouse_id, &table.table_ident, false, &state.read_pool)
            .await
            .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(&warehouse_id, &table.table_ident, true, &state.read_pool)
            .await
            .unwrap();
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_to_ids(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = table_idents_to_ids(
            &warehouse_id,
            vec![&table_ident].into_iter().collect(),
            false,
            &state.read_pool,
        )
        .await
        .unwrap();
        assert!(exists.len() == 1 && exists.get(&table_ident).unwrap().is_none());
        drop(table_ident);

        let table_1 = initialize_table(&warehouse_id, state.clone(), true).await;
        let mut tables = HashSet::new();
        tables.insert(&table_1.table_ident);

        // Table is staged - no result if include_staged is false
        let exists = table_idents_to_ids(&warehouse_id, tables.clone(), false, &state.read_pool)
            .await
            .unwrap();
        assert_eq!(exists.len(), 1);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());

        let exists = table_idents_to_ids(&warehouse_id, tables.clone(), true, &state.read_pool)
            .await
            .unwrap();
        assert_eq!(exists.len(), 1);
        assert_eq!(
            exists.get(&table_1.table_ident).unwrap(),
            &Some(table_1.table_id)
        );

        // Second Table
        let table_2 = initialize_table(&warehouse_id, state.clone(), false).await;
        tables.insert(&table_2.table_ident);

        let exists =
            dbg!(
                table_idents_to_ids(&warehouse_id, tables.clone(), false, &state.read_pool)
                    .await
                    .unwrap()
            );
        assert_eq!(exists.len(), 2);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());
        assert_eq!(
            exists.get(&table_2.table_ident).unwrap(),
            &Some(table_2.table_id)
        );

        let exists = table_idents_to_ids(&warehouse_id, tables.clone(), true, &state.read_pool)
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
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(&warehouse_id, state.clone(), false).await;

        let new_table_ident = TableIdent {
            namespace: table.namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            &warehouse_id,
            &table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = table_ident_to_id(&warehouse_id, &table.table_ident, false, &state.read_pool)
            .await
            .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(&warehouse_id, &new_table_ident, false, &state.read_pool)
            .await
            .unwrap();
        // Table id should be the same
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_rename_with_namespace(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(&warehouse_id, state.clone(), false).await;

        let new_namespace = NamespaceIdent::from_vec(vec!["new_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &new_namespace, None).await;

        let new_table_ident = TableIdent {
            namespace: new_namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            &warehouse_id,
            &table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = table_ident_to_id(&warehouse_id, &table.table_ident, false, &state.read_pool)
            .await
            .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(&warehouse_id, &new_table_ident, false, &state.read_pool)
            .await
            .unwrap();
        assert_eq!(exists, Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), &warehouse_id, &namespace, None).await;
        let tables = list_tables(&warehouse_id, &namespace, false, state.clone())
            .await
            .unwrap();
        assert_eq!(tables.len(), 0);

        let table1 = initialize_table(&warehouse_id, state.clone(), false).await;

        let tables = list_tables(&warehouse_id, &table1.namespace, false, state.clone())
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table1.table_id), Some(&table1.table_ident));

        let table2 = initialize_table(&warehouse_id, state.clone(), true).await;
        let tables = list_tables(&warehouse_id, &table2.namespace, false, state.clone())
            .await
            .unwrap();
        assert_eq!(tables.len(), 0);
        let tables = list_tables(&warehouse_id, &table2.namespace, true, state.clone())
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table2.table_id), Some(&table2.table_ident));
    }

    #[sqlx::test]
    async fn test_commit_transaction(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table1 = initialize_table(&warehouse_id, state.clone(), true).await;
        let table2 = initialize_table(&warehouse_id, state.clone(), false).await;

        let request = CommitTransactionRequest {
            table_changes: vec![
                CommitTableRequest {
                    identifier: Some(table1.table_ident.clone()),
                    requirements: vec![TableRequirement::NotExist {}],
                    updates: vec![TableUpdate::SetProperties {
                        updates: HashMap::from_iter(vec![(
                            "t1_key".to_string(),
                            "t1_value".to_string(),
                        )]),
                    }],
                },
                CommitTableRequest {
                    identifier: Some(table2.table_ident.clone()),
                    requirements: vec![TableRequirement::UuidMatch {
                        uuid: table2.table_id.as_uuid().to_owned(),
                    }],
                    updates: vec![TableUpdate::SetProperties {
                        updates: HashMap::from_iter(vec![(
                            "t2_key".to_string(),
                            "t2_value".to_string(),
                        )]),
                    }],
                },
            ],
        };

        let table_ids = table_idents_to_ids(
            &warehouse_id,
            vec![&table1.table_ident, &table2.table_ident]
                .into_iter()
                .collect(),
            true,
            &state.read_pool,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, v.unwrap()))
        .collect();

        let mut transaction = pool.begin().await.unwrap();
        let responses =
            commit_table_transaction(&warehouse_id, request, &table_ids, &mut transaction)
                .await
                .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(responses.len(), 2);

        let response1 = responses
            .iter()
            .find(|r| &r.commit_response.metadata.uuid() == table1.table_id.as_uuid())
            .unwrap();
        let response2 = responses
            .iter()
            .find(|r| &r.commit_response.metadata.uuid() == table2.table_id.as_uuid())
            .unwrap();

        assert_eq!(
            response1.commit_response.metadata.properties,
            HashMap::from_iter(vec![("t1_key".to_string(), "t1_value".to_string())])
        );
        assert_eq!(
            response2.commit_response.metadata.properties,
            HashMap::from_iter(vec![("t2_key".to_string(), "t2_value".to_string())])
        );
    }

    #[sqlx::test]
    async fn test_get_metadata_by_location(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(&warehouse_id, state.clone(), false).await;

        let metadata =
            get_table_metadata_by_id(&warehouse_id, &table.table_id, false, state.clone())
                .await
                .unwrap();

        // Exact path works
        let metadata = get_table_metadata_by_s3_location(
            &warehouse_id,
            &metadata.location,
            false,
            state.clone(),
        )
        .await
        .unwrap();

        assert_eq!(metadata.table, table.table_ident);
        assert_eq!(metadata.table_id, table.table_id);

        // Subpath works
        let metadata = get_table_metadata_by_s3_location(
            &warehouse_id,
            &format!("{}/data/foo.parquet", &metadata.location),
            false,
            state.clone(),
        )
        .await
        .unwrap();

        // Shorter path does not work
        get_table_metadata_by_s3_location(
            &warehouse_id,
            &metadata.location[0..metadata.location.len() - 1],
            false,
            state.clone(),
        )
        .await
        .unwrap_err();
    }

    #[sqlx::test]
    async fn test_cannot_get_table_of_inactive_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(&warehouse_id, state.clone(), false).await;
        let mut transaction = pool.begin().await.unwrap();
        set_warehouse_status(&warehouse_id, WarehouseStatus::Inactive, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let err = get_table_metadata_by_id(&warehouse_id, &table.table_id, false, state.clone())
            .await
            .unwrap_err();
        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_drop_table_works(pool: sqlx::PgPool) {
        let state = CatalogState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
        };

        let warehouse_id = initialize_warehouse(state.clone(), None, None).await;
        let table = initialize_table(&warehouse_id, state.clone(), false).await;

        let mut transaction = pool.begin().await.unwrap();
        drop_table(&warehouse_id, &table.table_id, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let err = get_table_metadata_by_id(&warehouse_id, &table.table_id, false, state.clone())
            .await
            .unwrap_err();
        assert_eq!(err.error.code, StatusCode::NOT_FOUND);
    }
}
