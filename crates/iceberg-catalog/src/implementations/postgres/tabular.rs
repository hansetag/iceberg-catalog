pub(crate) mod table;
pub(crate) mod view;

use super::{dbutils::DBErrorHandler as _, CatalogState};
use crate::{
    service::{ErrorModel, Result, TableIdent},
    WarehouseIdent,
};
use http::StatusCode;
use iceberg_ext::NamespaceIdent;

use crate::api::iceberg::v1::{PaginatedTabulars, PaginationQuery, MAX_PAGE_SIZE};
use crate::implementations::postgres::pagination::{PaginateToken, V1PaginateToken};
use crate::service::tabular_idents::{TabularIdentBorrowed, TabularIdentOwned, TabularIdentUuid};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, Execute, FromRow, PgConnection, Postgres, QueryBuilder};
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::fmt::Debug;
use uuid::Uuid;

const MAX_PARAMETERS: usize = 30000;

#[derive(Debug, sqlx::Type, Copy, Clone, strum::Display)]
#[sqlx(type_name = "tabular_type", rename_all = "kebab-case")]
pub(crate) enum TabularType {
    Table,
    View,
}

pub(crate) async fn tabular_ident_to_id<'a, 'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    table: &TabularIdentBorrowed<'a>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<Option<TabularIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let t = table.to_table_ident_tuple();
    let typ: TabularType = table.into();

    let rows = sqlx::query!(
        r#"
        SELECT t.tabular_id, t.typ as "typ: TabularType"
        FROM tabular t
        INNER JOIN namespace n ON t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.namespace_name = $1 AND t.name = $2
        AND n.warehouse_id = $3
        AND w.status = 'active'
        AND t.typ = $4
        AND (t.deleted_at IS NULL OR $5)
        AND (t.metadata_location IS NOT NULL OR $6)
        "#,
        t.namespace.as_ref(),
        t.name,
        *warehouse_id,
        typ as _,
        list_flags.include_deleted,
        list_flags.include_staged
    )
    .fetch_one(catalog_state)
    .await
    .map(|r| {
        Some(match r.typ {
            TabularType::Table => TabularIdentUuid::Table(r.tabular_id),
            TabularType::View => TabularIdentUuid::View(r.tabular_id),
        })
    });

    match rows {
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(None),
            _ => Err(e
                .into_error_model(format!("Error fetching {}", table.typ_str()))
                .into()),
        },
        Ok(opt) => Ok(opt),
    }
}

#[derive(Debug, FromRow)]
struct TabularRow {
    tabular_id: Uuid,
    namespace: Vec<String>,
    tabular_name: String,
    metadata_location: Option<String>,
    // apparently this is needed, we need 'as "typ: TabularType"' in the query else the select won't
    // work, but that apparently aliases the whole column to "typ: TabularType"
    #[sqlx(rename = "typ: TabularType")]
    typ: TabularType,
}

pub(crate) async fn tabular_idents_to_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    tables: HashSet<TabularIdentBorrowed<'_>>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<HashMap<TabularIdentOwned, Option<TabularIdentUuid>>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let batch_tables = tables
        .iter()
        .map(|t| {
            let TableIdent { namespace, name } = t.to_table_ident_tuple();
            let typ: TabularType = t.into();
            (namespace, name, typ)
        })
        .collect::<Vec<_>>();

    if batch_tables.is_empty() {
        return Ok(HashMap::new());
    }

    if batch_tables.len() > (MAX_PARAMETERS / 2) {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Too many tables or views to fetch".to_string())
            .r#type("TooManyTablesOrViews".to_string())
            .build()
            .into());
    }

    // This query is statically verified against our DB, we then take it apart to do some dynamic
    // extension further down before reconstructing it.
    let mut statically_checked_query = sqlx::query_as!(
        TabularRow,
        r#"
        SELECT t.tabular_id,
               n.namespace_name as "namespace",
               t.name as tabular_name,
               t.metadata_location,
               t.typ as "typ: TabularType"
        FROM tabular t
        INNER JOIN namespace n ON t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.status = 'active' and n."warehouse_id" = $1 AND (t.deleted_at is NULL OR $2) "#,
        *warehouse_id,
        list_flags.include_deleted
    );
    let checked_sql = statically_checked_query.sql();

    let mut query_builder: QueryBuilder<'_, Postgres> = sqlx::QueryBuilder::new(checked_sql);

    let mut args = statically_checked_query
        .take_arguments()
        .map_err(|e| {
            ErrorModel::internal("Failed to build dynamic query", "DatabaseError", Some(e))
        })?
        .unwrap_or_default();

    append_dynamic_filters(batch_tables.as_slice(), &mut query_builder, &mut args)?;

    let query = query_builder.build();

    let rows: Vec<TabularRow> = sqlx::query_as_with(query.sql(), args)
        .fetch_all(catalog_state)
        .await
        .map_err(|e| e.into_error_model("Error fetching tables or views".to_string()))?;

    let mut table_map = HashMap::with_capacity(tables.len());
    for TabularRow {
        tabular_id,
        namespace,
        tabular_name: name,
        metadata_location,
        typ,
    } in rows
    {
        let namespace = try_parse_namespace_ident(namespace)?;

        let staged = metadata_location.is_none();
        if !staged || list_flags.include_staged {
            match typ {
                TabularType::Table => {
                    table_map.insert(
                        TabularIdentOwned::Table(TableIdent { namespace, name }),
                        Some(TabularIdentUuid::Table(tabular_id)),
                    );
                }
                TabularType::View => {
                    table_map.insert(
                        TabularIdentOwned::View(TableIdent { namespace, name }),
                        Some(TabularIdentUuid::View(tabular_id)),
                    );
                }
            }
        }
    }

    // Missing tables are added with None
    for table in tables {
        table_map.entry(table.into()).or_insert(None);
    }

    Ok(table_map)
}

fn append_dynamic_filters(
    batch_tables: &[(&NamespaceIdent, &String, TabularType)],
    query_builder: &mut QueryBuilder<'_, Postgres>,
    args: &mut PgArguments,
) -> Result<()> {
    query_builder.push(r" AND (n.namespace_name, t.name, t.typ) IN ");
    query_builder.push("(");

    let mut arg_idx = args.len() + 1;
    for (i, (ns_ident, name, typ)) in batch_tables.iter().enumerate() {
        query_builder.push(format!("(${arg_idx}"));
        arg_idx += 1;
        args.add(ns_ident.as_ref()).map_err(|e| {
            ErrorModel::internal("Failed to add namespace to query", "DatabaseError", Some(e))
        })?;

        query_builder.push(", ");

        query_builder.push(format!("${arg_idx}"));
        arg_idx += 1;
        args.add(name).map_err(|e| {
            ErrorModel::internal("Failed to add name to query", "DatabaseError", Some(e))
        })?;
        query_builder.push(", ");

        query_builder.push(format!("${arg_idx}"));
        arg_idx += 1;
        args.add(*typ).map_err(|e| {
            ErrorModel::internal("Failed to add type to query", "DatabaseError", Some(e))
        })?;

        query_builder.push(")");
        if i != batch_tables.len() - 1 {
            query_builder.push(", ");
        }
    }
    query_builder.push(")");
    Ok(())
}

pub(crate) struct CreateTabular<'a> {
    pub(crate) id: Uuid,
    pub(crate) name: &'a str,
    pub(crate) namespace_id: Uuid,
    pub(crate) typ: TabularType,
    pub(crate) metadata_location: Option<&'a str>,
    pub(crate) location: &'a str,
}

pub(crate) async fn create_tabular<'a>(
    CreateTabular {
        id,
        name,
        namespace_id,
        typ,
        metadata_location,
        location,
    }: CreateTabular<'a>,
    conn: &mut PgConnection,
) -> Result<Uuid> {
    // Tables with `metadata_location is NULL` are staged and not yet committed.
    // They can be overwritten in a new create statement as if they wouldn't exist yet.
    // Views do not require this distinction, as `metadata_location` is always set for them
    // (validated by constraint).
    Ok(sqlx::query_scalar!(
        r#"
        INSERT INTO tabular (tabular_id, name, namespace_id, typ, metadata_location, location)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT ON CONSTRAINT unique_name_per_namespace_id
        DO UPDATE SET tabular_id = $1, metadata_location = $5, location = $6
        WHERE tabular.metadata_location IS NULL AND tabular.typ = 'table'
        RETURNING tabular_id
        "#,
        id,
        name,
        namespace_id,
        typ as _,
        metadata_location,
        location
    )
    .fetch_one(conn)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => {
            tracing::debug!("conflicted out {id}, {namespace_id}, {typ}");
            ErrorModel::builder()
                .code(StatusCode::CONFLICT.into())
                .message("Table or View with same name already exists in Namespace".to_string())
                .r#type("TableOrViewAlreadyExists".to_string())
                .build()
        }
        _ => e.into_error_model(format!("Error creating {typ}")),
    })?)
}

pub(crate) async fn list_tabulars(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
    typ: Option<TabularType>,
    pagination_query: PaginationQuery,
) -> Result<PaginatedTabulars<TabularIdentUuid, TabularIdentOwned>> {
    let page_size = pagination_query
        .page_size
        .map(i64::from)
        .map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

    let token = pagination_query
        .page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(|PaginateToken::V1(V1PaginateToken { created_at, id })| (created_at, id))
        .unzip();

    let tables = sqlx::query!(
        r#"
        SELECT
            t.tabular_id,
            t.name as "tabular_name",
            namespace_name,
            typ as "typ: TabularType",
            t.created_at
        FROM tabular t
        INNER JOIN namespace n ON t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1
            AND namespace_name = $2
            AND w.status = 'active'
            AND (t.deleted_at IS NULL OR $8)
            AND (t."metadata_location" IS NOT NULL OR $3)
            AND (t.typ = $4 OR $4 IS NULL)
            AND ((t.created_at > $5 OR $5 IS NULL) OR (t.created_at = $5 AND t.tabular_id > $6))
            ORDER BY t.created_at, t.tabular_id ASC
            LIMIT $7
        "#,
        *warehouse_id,
        &**namespace,
        list_flags.include_staged,
        typ as _,
        token_ts,
        token_id,
        page_size,
        list_flags.include_deleted
    )
    .fetch_all(&catalog_state.read_pool())
    .await
    .map_err(|e| e.into_error_model("Error fetching tables or views".to_string()))?;

    let next_page_token = tables.last().map(|r| {
        PaginateToken::V1(V1PaginateToken {
            created_at: r.created_at,
            id: r.tabular_id,
        })
        .to_string()
    });

    let mut tabulars = HashMap::new();
    for table in tables {
        let namespace = try_parse_namespace_ident(table.namespace_name)?;
        let name = table.tabular_name;

        match table.typ {
            TabularType::Table => {
                tabulars.insert(
                    TabularIdentUuid::Table(table.tabular_id),
                    TabularIdentOwned::Table(TableIdent { namespace, name }),
                );
            }
            TabularType::View => {
                tabulars.insert(
                    TabularIdentUuid::View(table.tabular_id),
                    TabularIdentOwned::View(TableIdent { namespace, name }),
                );
            }
        };
    }

    Ok(PaginatedTabulars {
        tabulars,
        next_page_token,
    })
}

/// Rename a tabular. Tabulars may be moved across namespaces.
pub(crate) async fn rename_tabular(
    warehouse_id: WarehouseIdent,
    source_id: TabularIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let TableIdent {
        namespace: source_namespace,
        name: source_name,
    } = source;
    let TableIdent {
        namespace: dest_namespace,
        name: dest_name,
    } = destination;

    if source_namespace == dest_namespace {
        let _ = sqlx::query_scalar!(
            r#"
            UPDATE tabular ti
            SET name = $1
            WHERE tabular_id = $2 AND typ = $3
                AND metadata_location IS NOT NULL
                AND ti.deleted_at IS NULL
                AND $4 IN (
                    SELECT warehouse_id FROM warehouse WHERE status = 'active'
                )
            RETURNING tabular_id
            "#,
            &**dest_name,
            *source_id,
            TabularType::from(source_id) as _,
            *warehouse_id,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("ID of {} to rename not found", source_id.typ_str()))
                .r#type(format!("Rename{}IdNotFound", source_id.typ_str()))
                .build(),
            _ => e.into_error_model(format!("Error renaming {}", source_id.typ_str())),
        })?;
    } else {
        let _ = sqlx::query_scalar!(
            r#"
            UPDATE tabular ti
            SET name = $1, "namespace_id" = (
                SELECT namespace_id
                FROM namespace
                WHERE warehouse_id = $2 AND namespace_name = $3
            )
            WHERE tabular_id = $4 AND typ = $5 AND metadata_location IS NOT NULL
                AND ti.name = $6
                AND ti.deleted_at IS NULL
                AND $2 IN (
                    SELECT warehouse_id FROM warehouse WHERE status = 'active'
                )
            RETURNING tabular_id
            "#,
            &**dest_name,
            *warehouse_id,
            &**dest_namespace,
            *source_id,
            TabularType::from(source_id) as _,
            &**source_name,
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!(
                    "ID of {} to rename not found or destination namespace not found",
                    source_id.typ_str()
                ))
                .r#type(format!(
                    "Rename{}IdOrNamespaceNotFound",
                    source_id.typ_str()
                ))
                .build(),
            _ => e.into_error_model(format!("Error renaming {}", source_id.typ_str())),
        })?;
    };

    Ok(())
}

// ToDo: Switch to a soft delete
pub(crate) async fn drop_tabular<'a>(
    tabular_id: TabularIdentUuid,
    hard_delete: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    // Set deleted_at to now() for rows that have metadata_location
    // Drop the row if it doesn't have metadata_location

    if hard_delete {
        let _ = sqlx::query!(
            r#"DELETE FROM tabular
                WHERE tabular_id = $1
                    AND typ = $2
                    AND tabular_id IN (SELECT tabular_id FROM active_tabulars)
               RETURNING tabular_id"#,
            *tabular_id,
            TabularType::from(tabular_id) as _
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| {
            if let sqlx::Error::RowNotFound = e {
                ErrorModel::builder()
                    .code(StatusCode::NOT_FOUND.into())
                    .message(format!("{} not found", tabular_id.typ_str()))
                    .r#type("NoSuchTabularError".to_string())
                    .build()
            } else {
                tracing::warn!("Error dropping tabular: {}", e);
                e.into_error_model(format!("Error dropping {}", tabular_id.typ_str()))
            }
        })?;
        return Ok(());
    }

    // Soft delete, sets deleted_at to now and appends a uuid to the name to avoid conflicts of
    // new tables with the same name. Staged tables are permanently deleted.
    let _ = sqlx::query!(
        r#"
    WITH updated AS (
        UPDATE tabular
        -- Append a suffix to the name to avoid conflicts with new tables
        SET deleted_at = now(), name = name || $3
        WHERE tabular_id = $1
        AND typ = $2
        AND metadata_location IS NOT NULL
        AND tabular_id IN (SELECT tabular_id FROM active_tabulars)
        RETURNING tabular_id
    ),
    deleted AS (
        DELETE FROM tabular
        WHERE tabular_id = $1
        AND typ = $2
        AND metadata_location IS NULL
        AND tabular_id IN (SELECT tabular_id FROM active_tabulars)
        RETURNING tabular_id
    )
    SELECT tabular_id FROM updated
    UNION ALL
    SELECT tabular_id FROM deleted
    "#,
        *tabular_id,
        TabularType::from(tabular_id) as _,
        Uuid::now_v7().to_string()
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message(format!("{} not found", tabular_id.typ_str()))
                .r#type("NoSuchTabularError".to_string())
                .build()
        } else {
            tracing::warn!("Error dropping tabular: {}", e);
            e.into_error_model(format!("Error dropping {}", tabular_id.typ_str()))
        }
    })?;

    Ok(())
}

fn try_parse_namespace_ident(namespace: Vec<String>) -> Result<NamespaceIdent> {
    NamespaceIdent::from_vec(namespace).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error parsing namespace".to_string())
            .r#type("NamespaceParseError".to_string())
            .source(Some(Box::new(e)))
            .build()
            .into()
    })
}

impl<'a, 'b> From<&'b TabularIdentBorrowed<'a>> for TabularType {
    fn from(ident: &'b TabularIdentBorrowed<'a>) -> Self {
        match ident {
            TabularIdentBorrowed::Table(_) => TabularType::Table,
            TabularIdentBorrowed::View(_) => TabularType::View,
        }
    }
}

impl<'a> From<&'a TabularIdentUuid> for TabularType {
    fn from(ident: &'a TabularIdentUuid) -> Self {
        match ident {
            TabularIdentUuid::Table(_) => TabularType::Table,
            TabularIdentUuid::View(_) => TabularType::View,
        }
    }
}

impl From<TabularIdentUuid> for TabularType {
    fn from(ident: TabularIdentUuid) -> Self {
        match ident {
            TabularIdentUuid::Table(_) => TabularType::Table,
            TabularIdentUuid::View(_) => TabularType::View,
        }
    }
}
