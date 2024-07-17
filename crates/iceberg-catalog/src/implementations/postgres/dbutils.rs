use iceberg_ext::catalog::rest::ErrorModel;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + std::error::Error + 'static + Send + Sync,
{
    fn into_internal_error(self, message: String) -> ErrorModel {
        ErrorModel::internal(message, "DatabaseError", Some(Box::new(self)))
    }
}

impl DBErrorHandler for sqlx::Error {}
