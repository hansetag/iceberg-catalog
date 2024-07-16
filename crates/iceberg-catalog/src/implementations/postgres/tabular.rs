pub(crate) mod table;
pub(crate) mod view;

use super::{dbutils::DBErrorHandler as _, CatalogState};
use crate::{
    service::{ErrorModel, Result, TableIdent},
    WarehouseIdent,
};
use http::StatusCode;
use iceberg_ext::NamespaceIdent;

use crate::service::tabular_idents::{TabularIdentBorrowed, TabularIdentOwned, TabularIdentUuid};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, Execute, FromRow, PgConnection, Postgres, QueryBuilder};
use std::collections::{HashMap, HashSet};
use std::default::Default;
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
    include_staged: bool,
    catalog_state: E,
) -> Result<Option<TabularIdentUuid>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let t = table.to_table_ident_tuple();
    let typ: TabularType = table.into();

    let rows = sqlx::query!(
        r#"
        SELECT t.tabular_id, t.metadata_location, typ AS "typ: TabularType"
        FROM tabular t
        INNER JOIN namespace n ON t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.namespace_name = $1 AND t.name = $2
        AND n.warehouse_id = $3
        AND w.status = 'active'
        AND t.typ = $4
        "#,
        t.namespace.as_ref(),
        t.name,
        *warehouse_id,
        typ as _
    )
    .fetch_one(catalog_state)
    .await
    .map(|r| {
        Some((
            match r.typ {
                TabularType::Table => TabularIdentUuid::Table(r.tabular_id),
                TabularType::View => TabularIdentUuid::View(r.tabular_id),
            },
            r.metadata_location.is_none(),
        ))
    });

    match rows {
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(None),
            _ => Err(e
                .into_internal_error(format!("Error fetching {}", table.typ_str()))
                .into()),
        },
        Ok(Some((table_id, staged))) => {
            if staged && !include_staged {
                return Ok(None);
            }
            Ok(Some(table_id))
        }
        Ok(None) => Ok(None),
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
    include_staged: bool,
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
        WHERE w.status = 'active' and n."warehouse_id" = $1"#,
        *warehouse_id
    );
    let checked_sql = statically_checked_query.sql();

    let mut query_builder: QueryBuilder<'_, Postgres> = sqlx::QueryBuilder::new(checked_sql);

    let mut args = statically_checked_query
        .take_arguments()
        .unwrap_or_default();

    append_dynamic_filters(batch_tables.as_slice(), &mut query_builder, &mut args);

    let query = query_builder.build();

    let rows: Vec<TabularRow> = sqlx::query_as_with(query.sql(), args)
        .fetch_all(catalog_state)
        .await
        .map_err(|e| e.into_internal_error("Error fetching tables or views".to_string()))?;

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
        if !staged || include_staged {
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
) {
    query_builder.push(r" AND (n.namespace_name, t.name, t.typ) IN ");
    query_builder.push("(");

    let mut arg_idx = 2;
    for (i, (ns_ident, name, typ)) in batch_tables.iter().enumerate() {
        query_builder.push(format!("(${arg_idx}"));
        arg_idx += 1;
        args.add(ns_ident.as_ref());

        query_builder.push(", ");

        query_builder.push(format!("${arg_idx}"));
        arg_idx += 1;
        args.add(name);
        query_builder.push(", ");

        query_builder.push(format!("${arg_idx}"));
        arg_idx += 1;
        args.add(*typ);

        query_builder.push(")");
        if i != batch_tables.len() - 1 {
            query_builder.push(", ");
        }
    }
    query_builder.push(")");
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
        _ => e.into_internal_error(format!("Error creating {typ}")),
    })?)
}

pub(crate) async fn list_tabulars(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    include_staged: bool,
    catalog_state: CatalogState,
    typ: Option<TabularType>,
) -> Result<HashMap<TabularIdentUuid, TabularIdentOwned>> {
    let tables = sqlx::query!(
        r#"
        SELECT
            t.tabular_id,
            t.name as "tabular_name",
            namespace_name,
            typ as "typ: TabularType"
        FROM tabular t
        INNER JOIN namespace n ON t.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1
            AND namespace_name = $2
            AND w.status = 'active'
            AND (t."metadata_location" IS NOT NULL OR $3)
            AND (t.typ = $4 OR $4 IS NULL)
        "#,
        *warehouse_id,
        &**namespace,
        include_staged,
        typ as _
    )
    .fetch_all(&catalog_state.read_pool)
    .await
    .map_err(|e| e.into_internal_error("Error fetching tables or views".to_string()))?;

    let mut table_map = HashMap::new();
    for table in tables {
        let namespace = try_parse_namespace_ident(table.namespace_name)?;
        let name = table.tabular_name;

        match table.typ {
            TabularType::Table => {
                table_map.insert(
                    TabularIdentUuid::Table(table.tabular_id),
                    TabularIdentOwned::Table(TableIdent { namespace, name }),
                );
            }
            TabularType::View => {
                table_map.insert(
                    TabularIdentUuid::View(table.tabular_id),
                    TabularIdentOwned::View(TableIdent { namespace, name }),
                );
            }
        };
    }

    Ok(table_map)
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
            WHERE tabular_id = $2 AND typ = $3 AND metadata_location IS NOT NULL
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
            _ => e.into_internal_error(format!("Error renaming {}", source_id.typ_str())),
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
            _ => e.into_internal_error(format!("Error renaming {}", source_id.typ_str())),
        })?;
    };

    Ok(())
}

// ToDo: Switch to a soft delete
pub(crate) async fn drop_tabular<'a>(
    tabular_id: TabularIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
        DELETE FROM tabular
        WHERE tabular_id = $1 AND typ = $2
        AND tabular_id IN (select tabular_id from active_tabulars)
        RETURNING "tabular_id"
        "#,
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
                .r#type("NoSuchTableError".to_string())
                .build()
        } else {
            tracing::warn!("Error dropping tabular: {}", e);
            e.into_internal_error(format!("Error dropping {}", tabular_id.typ_str()))
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
