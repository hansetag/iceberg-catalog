#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]

pub mod auth;
pub mod catalog;
mod models;
pub mod state;

pub use models::*;
