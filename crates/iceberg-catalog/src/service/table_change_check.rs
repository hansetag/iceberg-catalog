use crate::service::TableIdentUuid;
use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use iceberg::TableUpdate;
use iceberg_ext::catalog::rest::ErrorModel;
use std::fmt::Debug;
use std::sync::Arc;

/// A trait for checking if a table change is allowed.
///
/// This trait is used to implement custom logic for checking if a table change is allowed. One
/// possible application is the enforcement of data contracts. For example, an external system could
/// be contacted to check if the changed table columns are part of an existing contract.
///
/// # Example
///
/// ```rust
///     use async_trait::async_trait;
///     use iceberg::spec::TableMetadata;
///     use iceberg::TableUpdate;
///     use iceberg_catalog::service::table_change_check::{TableChangeCheck, TableCheckResult};
///     use iceberg_catalog::service::TableIdentUuid;
///     use iceberg_ext::catalog::rest::ErrorModel;
///
///     #[derive(Debug)]
///     pub struct AllowAllChecker;
///     #[async_trait]
///     impl TableChangeCheck for AllowAllChecker {
///         fn name(&self) -> &'static str {
///             "AllowAllChecker"
///         }
///         async fn check(
///             &self,
///             _table_updates: &[TableUpdate],
///             _table_ident_uuid: TableIdentUuid,
///             _current_metadata: &TableMetadata,
///         ) -> Result<TableCheckResult, ErrorModel> {
///             Ok(TableCheckResult::Clear {})
///         }
///
///         async fn check_drop(
///             &self,
///             _table_ident_uuid: TableIdentUuid,
///         ) -> Result<TableCheckResult, ErrorModel> {
///             Ok(TableCheckResult::Clear {})
///         }
///     }
///
///     #[derive(Debug)]
///     pub struct DenyAllChecker;
///
///     #[async_trait]
///     impl TableChangeCheck for DenyAllChecker {
///         fn name(&self) -> &'static str {
///             "DenyAllChecker"
///         }
///         async fn check(
///             &self,
///             _table_updates: &[TableUpdate],
///             _table_ident_uuid: TableIdentUuid,
///             _current_metadata: &TableMetadata,
///         ) -> Result<TableCheckResult, ErrorModel> {
///             Ok(TableCheckResult::Block {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .message("Denied")
///                     .r#type("ContractViolation".to_string())
///                     .build()
///                     .into(),
///             })
///         }
///
///         async fn check_drop(
///             &self,
///             _table_ident_uuid: TableIdentUuid,
///         ) -> Result<TableCheckResult, ErrorModel> {
///             Ok(TableCheckResult::Block {
///                 error_model: ErrorModel::builder()
///                     .code(409)
///                     .r#type("ContractViolation".to_string())
///                     .message("Denied")
///                     .build()
///                     .into(),
///             })
///         }
///     }
/// ```
#[async_trait]
pub trait TableChangeCheck: Debug {
    fn name(&self) -> &'static str;

    async fn check(
        &self,
        table_updates: &[TableUpdate],
        table_ident_uuid: TableIdentUuid,
        current_metadata: &TableMetadata,
    ) -> Result<TableCheckResult, ErrorModel>;

    async fn check_drop(
        &self,
        table_ident_uuid: TableIdentUuid,
    ) -> Result<TableCheckResult, ErrorModel>;
}

#[derive(Debug)]
pub enum TableCheckResult {
    Clear {},
    Block { error_model: ErrorModel },
}

impl TableCheckResult {
    /// Converts `self` into a `Result<(), ErrorModel>`.
    ///
    /// When using `TableChangeCheck`, we are presented with a `Result<TableCheckResult, ErrorModel>`
    /// where the outer `ErrorModel` indicates that a checker failed, that would indicate a problem
    /// with the checker itself and may be returned as an internal server error. This function here
    /// offers convenience to go from the `TableCheckResult` to a `Result<(), ErrorModel>` which can
    /// be short-circuited using the `?` operator in the handler.
    ///
    /// # Example
    ///
    /// ```rust
    ///     use iceberg_catalog::service::table_change_check::TableCheckResult;
    ///     use iceberg_ext::catalog::rest::ErrorModel;
    ///     let result: Result<TableCheckResult, ErrorModel> = Ok(TableCheckResult::Clear {});
    ///     // no need to match on the result
    ///     result?.into_result()?;
    /// ```
    ///
    /// # Errors
    ///
    /// - extracts `error_model` from `TableCheckResult::Block` and returns it as an `Err` for
    ///   convenience.
    pub fn into_result(self) -> Result<(), ErrorModel> {
        match self {
            TableCheckResult::Clear {} => Ok(()),
            TableCheckResult::Block { error_model } => Err(error_model),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableChangeCheckers {
    checkers: Vec<Arc<dyn TableChangeCheck + Sync + Send>>,
}

impl TableChangeCheckers {
    #[must_use]
    pub fn new(checkers: Vec<Arc<dyn TableChangeCheck + Sync + Send>>) -> Self {
        Self { checkers }
    }
}

#[async_trait]
impl TableChangeCheck for TableChangeCheckers {
    fn name(&self) -> &'static str {
        "TableChangeCheckers"
    }

    async fn check(
        &self,
        table_updates: &[TableUpdate],
        table_ident_uuid: TableIdentUuid,
        current_metadata: &TableMetadata,
    ) -> Result<TableCheckResult, ErrorModel> {
        for checker in &self.checkers {
            match checker
                .check(table_updates, table_ident_uuid, current_metadata)
                .await
            {
                Ok(TableCheckResult::Clear {}) => {}
                Ok(block_result @ TableCheckResult::Block { error_model: _ }) => {
                    tracing::info!(
                        "Checker {} blocked change on table '{}'",
                        checker.name(),
                        table_ident_uuid
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("Checker {} failed", checker.name());
                    return Err(error);
                }
            }
        }

        Ok(TableCheckResult::Clear {})
    }
    async fn check_drop(
        &self,
        table_ident_uuid: TableIdentUuid,
    ) -> Result<TableCheckResult, ErrorModel> {
        for checker in &self.checkers {
            match checker.check_drop(table_ident_uuid).await {
                Ok(TableCheckResult::Clear {}) => {}
                Ok(block_result @ TableCheckResult::Block { error_model: _ }) => {
                    tracing::info!(
                        "Checker {} blocked drop on table '{}'",
                        checker.name(),
                        table_ident_uuid
                    );
                    return Ok(block_result);
                }
                Err(error) => {
                    tracing::warn!("Checker {} failed", checker.name());
                    return Err(error);
                }
            }
        }
        Ok(TableCheckResult::Clear {})
    }
}
