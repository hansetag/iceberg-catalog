use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + std::error::Error + 'static + Send + Sync,
{
    fn into_internal_error(self, message: String) -> ErrorModel {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(message)
            .r#type("DatabaseError".to_string())
            .source(Some(Box::new(self)))
            .build()
    }
}

impl DBErrorHandler for sqlx::Error {}
