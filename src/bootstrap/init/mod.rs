//! Bootstrap init helpers extracted from `runtime.rs` (R3 refactor).
//!
//! Each sub-module holds a cohesive group of helpers that were previously
//! inlined in the 3800+ LOC runtime file.  No behaviour change — pure
//! mechanical extraction.

pub mod cleanup;
pub mod dataplane;
pub mod host;
pub mod leader_control_stream;
pub mod predicates;
pub mod recovery;
pub mod tls;
