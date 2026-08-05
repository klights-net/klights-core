//! Passive cluster datastore adapters for klights.
//!
//! This package owns concrete SQLite/Redb open policy, current schemas and
//! migrations, and the supervised database-call boundary. Backend selection
//! and composition remain the responsibility of the root `klights` package.

pub mod diagnostics;
pub mod errors;
mod position_membership;
pub mod redb;
pub mod signing_key_state;
pub mod sqlite;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
pub mod test_fixtures;
