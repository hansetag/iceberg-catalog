#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]

// #[macro_use]
// extern crate serde_derive;

mod service;
pub mod types;

pub use service::*;
