use crate::api::ErrorModel;
use http::StatusCode;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + Send + Sync + std::error::Error + 'static,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(message)
            .r#type("DatabaseError".to_string())
            .source(Some(Box::new(self)))
            .build()
    }
}

impl DBErrorHandler for sqlx::Error {}
