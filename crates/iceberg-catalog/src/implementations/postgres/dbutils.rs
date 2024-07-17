use crate::api::ErrorModel;
use sqlx::Error;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized + Send + Sync + std::error::Error + 'static,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        match self {
            Error::Database(ref db) => match db.code().as_deref() {
                Some("25P01" | "25P02") => {
                    ErrorModel::conflict(
                        "Concurrent modification failed.",
                        "TransactionFailed",
                        Some(Box::new(self))
                    )
                }
                }
            }
        ErrorModel::internal(message, "DatabaseError", Some(Box::new(self)))

    }
}

impl DBErrorHandler for sqlx::Error {}
