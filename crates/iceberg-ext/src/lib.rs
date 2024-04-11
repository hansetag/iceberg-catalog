#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]

pub mod catalog;
pub mod spec;
pub mod validation;

#[macro_use]
extern crate serde_derive;
