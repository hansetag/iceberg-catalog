#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]

pub mod api;

pub mod catalog;
mod config;
pub mod service;
pub use service::{ProjectIdent, SecretIdent, WarehouseIdent};

pub use config::CONFIG;

pub mod implementations;
mod request_metadata;
#[cfg(feature = "router")]
pub(crate) mod tracing;
