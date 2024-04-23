use crate::auth::AuthState;
use iceberg_rest_service::State as ServiceState;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct DBState {
    #[cfg(feature = "sqlx-postgres")]
    pub pool: sqlx::PgPool,
}

#[derive(Clone, Debug)]
pub struct State<A: AuthState> {
    pub auth_state: A,
    pub db_state: DBState,
}

impl<A: AuthState> ServiceState for State<A> {}
