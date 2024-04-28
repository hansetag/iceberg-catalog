pub mod config;
pub mod namespace;

use crate::service::{auth::AuthHandler, AuthState, Catalog, CatalogState};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CatalogServer<C: Catalog<CS>, CS: CatalogState, A: AuthHandler<AS>, AS: AuthState> {
    auth_handler: PhantomData<A>,
    auth_state: PhantomData<AS>,
    config_server: PhantomData<C>,
    catalog_state: PhantomData<CS>,
}
