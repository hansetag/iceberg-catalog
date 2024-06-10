use crate::api::ErrorModel;
use http::StatusCode;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(message)
            .r#type("DatabaseError".to_string())
            .stack(Some(vec![self.to_string()]))
            .build()
    }

    fn as_error_model(&self, message: String) -> ErrorModel {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message(message)
            .r#type("DatabaseError".to_string())
            .stack(Some(vec![self.to_string()]))
            .build()
    }
}

impl DBErrorHandler for sqlx::Error {}
