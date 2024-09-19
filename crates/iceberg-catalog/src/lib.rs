#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions)]
#![forbid(unsafe_code)]

pub mod rest;

mod config;
pub mod modules;
pub mod service;
pub use modules::{ProjectIdent, SecretIdent, WarehouseIdent};

pub use config::{SecretBackend, CONFIG};

mod request_metadata;

#[cfg(feature = "router")]
pub mod metrics;
#[cfg(feature = "router")]
pub(crate) mod tracing;
