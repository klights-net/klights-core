//! Bootstrap phases module.
//!
//! Each module handles one phase of the boot sequence. Phases are pure
//! functions taking typed inputs and returning typed outputs — testable
//! in isolation without constructing the full runtime.

pub mod bootstrap;
pub mod config;
pub mod datastore;
pub mod env;
pub mod identity;
pub mod leader;
pub mod network;
pub mod recovery;
pub mod server;
