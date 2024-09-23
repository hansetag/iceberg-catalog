// TODO: move this constant elsewhere, maybe some constants module?
/// Default project ID used for single-project deployments.
pub const DEFAULT_PROJECT_ID: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-000000000000");

#[cfg(feature = "sqlx-postgres")]
pub mod postgres;

mod authz;
pub mod kv2;

pub use authz::{AllowAllAuthState, AllowAllAuthZHandler};
