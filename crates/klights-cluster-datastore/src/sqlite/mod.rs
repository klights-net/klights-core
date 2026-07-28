//! SQLite cluster schema and explicit open adapters.

mod fingerprint;
mod open;
mod schema;

pub use fingerprint::{META_INSERT, META_SELECT};
pub use open::{
    check_db_health, init_schema, open_in_memory, open_read_only_with_opts, open_with_opts,
};
pub use schema::init_schema_in_conn;

#[cfg(test)]
pub use open::open_in_memory_with_default_supervisor;
