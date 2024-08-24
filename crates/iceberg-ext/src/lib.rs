#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod catalog;
pub mod configs;
pub mod spec;
pub mod validation;

pub use iceberg::{
    NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};

#[macro_use]
extern crate serde_derive;
