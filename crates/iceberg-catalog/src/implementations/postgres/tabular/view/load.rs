use crate::api::Result;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::view::{ViewFormatVersion, ViewRepresentationType};
use crate::service::TableIdentUuid;
use iceberg::spec::{
    Schema, SchemaRef, SqlViewRepresentation, ViewMetadata, ViewRepresentation, ViewVersion,
    ViewVersionLog, ViewVersionRef,
};
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::{FromRow, PgConnection};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

pub(crate) async fn load_view(
    view: &TableIdentUuid,
    conn: &mut PgConnection,
) -> Result<ViewMetadata> {
    let fetcher: MetadataFetcher = MetadataFetcher::new(view.into_uuid());
    fetcher.fetch_metadata(conn).await
}

#[allow(clippy::struct_field_names)]
#[derive(FromRow, Debug)]
pub(crate) struct View {
    view_id: Uuid,
    #[sqlx(rename = "view_format_version: ViewFormatVersion")]
    view_format_version: ViewFormatVersion,
    // TODO: this is not part of view metadata as it seems
    #[allow(dead_code)]
    view_location: String,
    // TODO: We can construct this from uuid + location, why are we storing this?
    metadata_location: String,
    current_version_id: i64,
}

pub(crate) struct MetadataFetcher {
    view_id: Uuid,
}

impl MetadataFetcher {
    pub(crate) fn new(view_id: Uuid) -> Self {
        Self { view_id }
    }

    pub(crate) async fn fetch_metadata(
        self,
        conn: &mut PgConnection,
    ) -> crate::api::Result<ViewMetadata> {
        let View {
            view_id,
            view_format_version,
            view_location: _,
            metadata_location: location,
            current_version_id,
        } = self.fetch_view(&mut *conn).await?;

        Ok(ViewMetadata {
            format_version: match view_format_version {
                ViewFormatVersion::V1 => iceberg::spec::ViewFormatVersion::V1,
            },
            view_uuid: view_id,
            location,
            current_version_id,
            versions: self.fetch_versions(&mut *conn, view_id).await?,
            version_log: self.fetch_version_log(&mut *conn, view_id).await?,
            schemas: self.fetch_schemas(&mut *conn, view_id).await?,
            properties: self.fetch_properties(&mut *conn, view_id).await?,
        })
    }

    pub(crate) async fn fetch_view(&self, conn: &mut PgConnection) -> crate::api::Result<View> {
        Ok(sqlx::query_as!(
            View,
            r#"select
                    v.view_id,
                    view_format_version as "view_format_version: ViewFormatVersion",
                    ta.location as view_location,
                    ta.metadata_location as "metadata_location!",
                    cvv.version_id as current_version_id
                 from view v
                    JOIN view_version vv on v.view_id = vv.view_id
                    JOIN tabular ta ON v.view_id=ta.tabular_id
                    JOIN namespace n ON ta.namespace_id = n.namespace_id
                    JOIN warehouse w ON n.warehouse_id = w.warehouse_id
                    JOIN current_view_metadata_version cvv ON v.view_id = cvv.view_id
                WHERE v.view_id = $1 AND w.status = 'active' AND ta.typ = 'view'"#,
            self.view_id
        )
        .fetch_one(conn)
        .await
        .map_err(|e| {
            let message = "Error fetching base view".to_string();
            tracing::warn!(?e, "{}", message);
            e.into_error_model(message)
        })?)
    }

    pub(crate) async fn fetch_version_log(
        &self,
        conn: &mut PgConnection,
        view_id: Uuid,
    ) -> Result<Vec<ViewVersionLog>> {
        Ok(sqlx::query!(
            r#"
                SELECT version_id, timestamp
                FROM view_version_log
                WHERE view_id = $1
                "#,
            view_id
        )
        .fetch_all(conn)
        .await
        .map_err(|e| {
            let message = "Error fetching version log".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?
        .into_iter()
        .map(|r| ViewVersionLog {
            version_id: r.version_id,
            timestamp_ms: r.timestamp.timestamp_millis(),
        })
        .collect())
    }

    pub(crate) async fn fetch_schemas(
        &self,
        conn: &mut PgConnection,
        view_id: Uuid,
    ) -> crate::api::Result<HashMap<i32, SchemaRef>> {
        sqlx::query!(
            r#"
            SELECT schema_id, schema
            FROM view_schema
            WHERE view_id = $1
            "#,
            view_id
        )
        .fetch_all(conn)
        .await
        .map_err(|e| {
            let message = "Error fetching schemas".to_string();
            tracing::warn!(?e, "{}", message);
            e.into_error_model(message)
        })?
        .into_iter()
        .map(|r| {
            let schema: Schema = serde_json::from_value(r.schema).map_err(|e| {
                let message = "Error deserializing schema".to_string();
                tracing::warn!(?e, message);
                ErrorModel::builder()
                    .code(500)
                    .message(message)
                    .r#type("DatabaseError".to_string())
                    .stack(Some(vec![e.to_string()]))
                    .build()
            })?;
            Ok((r.schema_id, Arc::new(schema)))
        })
        .collect::<Result<HashMap<_, _>>>()
    }

    pub(crate) async fn fetch_properties(
        &self,
        conn: &mut PgConnection,
        view_id: Uuid,
    ) -> Result<HashMap<String, String>> {
        Ok(sqlx::query!(
            r#"
            SELECT key, value
            FROM view_properties
            WHERE view_id = $1
            "#,
            view_id
        )
        .fetch_all(conn)
        .await
        .map_err(|e| {
            let message = "Error fetching view properties".to_string();
            tracing::warn!(?e, "{}", message);
            e.into_error_model(message)
        })?
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect())
    }

    #[instrument(skip_all)]
    pub(crate) async fn fetch_versions(
        &self,
        conn: &mut PgConnection,
        view_id: Uuid,
    ) -> crate::api::Result<HashMap<i64, ViewVersionRef>> {
        let mut versions = HashMap::new();
        let rows = sqlx::query!(
                r#"SELECT version_id, schema_id, timestamp, default_namespace_id as "default_namespace_id?", default_catalog, view_version_uuid
                FROM view_version
                LEFT JOIN namespace ns on ns.namespace_id = view_version.default_namespace_id
                WHERE view_id = $1
                "#,
            view_id
        )
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                let message = "Error fetching versions".to_string();
                tracing::warn!("{}", message);
                e.into_error_model(message)
            })?;

        for r in rows {
            let timestamp = r.timestamp;
            let dni: Option<Uuid> = r.default_namespace_id;
            let namespace_name = Self::get_namespace_ident_with_empty_support(conn, dni).await?;
            let view_version_uuid = r.view_version_uuid;
            let version_id = r.version_id;

            let builder = ViewVersion::builder()
                .with_timestamp_ms(timestamp.timestamp_millis())
                .with_version_id(version_id)
                .with_default_namespace(namespace_name)
                .with_default_catalog(r.default_catalog)
                .with_schema_id(r.schema_id)
                .with_summary(Self::fetch_metadata_summary(conn, view_version_uuid).await?)
                .with_representations(
                    Self::fetch_view_representations(conn, view_version_uuid).await?,
                )
                .build();

            versions.insert(version_id, Arc::new(builder));
        }
        Ok(versions)
    }

    async fn fetch_view_representations(
        conn: &mut PgConnection,
        view_version_uuid: Uuid,
    ) -> Result<Vec<ViewRepresentation>> {
        Ok(sqlx::query!(
            r#"
                SELECT typ as "typ: ViewRepresentationType", sql, dialect
                FROM view_representation
                WHERE view_version_uuid = $1
                "#,
            view_version_uuid
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| {
            let message = "Error fetching view representations".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?
        .into_iter()
        .map(|r| match r.typ {
            ViewRepresentationType::Sql => {
                ViewRepresentation::SqlViewRepresentation(SqlViewRepresentation {
                    sql: r.sql,
                    dialect: r.dialect,
                })
            }
        })
        .collect())
    }

    async fn fetch_metadata_summary(
        conn: &mut PgConnection,
        view_version_uuid: Uuid,
    ) -> Result<HashMap<String, String>> {
        Ok(sqlx::query!(
            r#"
                SELECT key, value
                FROM metadata_summary
                WHERE view_version_uuid = $1
                "#,
            view_version_uuid
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| {
            let message = "Error fetching metadata summary".to_string();
            tracing::warn!("{}", message);
            e.into_error_model(message)
        })?
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect())
    }

    // this is a horrible function, and it's only here because NamespaceIdent doesn't allow constructing
    // from empty vecs which is something that spark is handing to us.
    async fn get_namespace_ident_with_empty_support(
        conn: &mut PgConnection,
        dni: Option<Uuid>,
    ) -> crate::api::Result<NamespaceIdent> {
        let namespace_name: NamespaceIdent = serde_json::from_value(if let Some(dni) = dni {
            serde_json::Value::Array(
                sqlx::query_scalar!(
                    r#"
                    SELECT namespace_name
                    FROM namespace
                    WHERE namespace_id = $1
                "#,
                    &dni
                )
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| {
                    let message = "Error fetching namespace_name".to_string();
                    tracing::warn!("{}", message);
                    e.into_error_model(message)
                })?
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
            )
        } else {
            // TODO: NamespaceIdent doesn't allow empty vecs, otoh, spark is happily handing those to us
            serde_json::Value::Array(vec![])
        })
        .map_err(|e| {
            let message =
                "Error fetching namespace_name, failed to deserialize namespace ident.".to_string();
            tracing::warn!("{}", message);
            ErrorModel::builder()
                .code(500)
                .message(message)
                .r#type("DatabaseError".to_string())
                .stack(Some(vec![e.to_string()]))
                .build()
        })?;

        Ok(namespace_name)
    }
}
