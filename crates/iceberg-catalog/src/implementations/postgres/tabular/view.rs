use crate::{
    service::{ErrorModel, Result, TableIdent, TableIdentUuid},
    WarehouseIdent,
};

use http::StatusCode;

use crate::implementations::postgres::tabular::{TabularIdentRef, TabularIdentUuid};

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
