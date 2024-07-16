use crate::api::ErrorModel;
use sqlx::Error;

pub(crate) trait DBErrorHandler
where
    Self: ToString + Sized,
{
    fn into_error_model(self, message: String) -> ErrorModel {
        internal_db_error(message, self.to_string())
    }

    fn as_error_model(&self, message: String) -> ErrorModel {
        internal_db_error(message, self.to_string())
    }
}

impl DBErrorHandler for sqlx::Error {
    fn into_error_model(self, message: String) -> ErrorModel {
        match self {
            Error::Database(ref db) => match db.code().as_deref() {
                Some("25P01" | "25P02") => {
                    let mut model = ErrorModel::conflict(
                        "Concurrent modification failed.",
                        "TransactionFailed",
                    );
                    model.push_to_stack(format!("{self:?}"));
                    model
                }
                _ => internal_db_error(message, self.to_string()),
            },
            _ => internal_db_error(message, self.to_string()),
        }
    }
}

fn internal_db_error(message: String, stack_item: String) -> ErrorModel {
    let mut m = ErrorModel::internal(message, "DatabaseError");
    m.push_to_stack(stack_item);
    m
}
