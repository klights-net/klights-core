//! Schema-check marker for the redb backend.
//!
//! Every typed table defined in `tables.rs` is opened inside a `read_txn`.
//! redb returns a `TableTypeError` if the stored type signature doesn't
//! match the current `TableDefinition` — that is our schema-mismatch detector.

use ::redb::{Database, ReadableDatabase, TableHandle};

use crate::errors::OpenError;

use super::tables;

/// Reject an incompatible existing node-dataplane table while allowing older
/// databases that predate this table to be initialized by the open boundary.
pub(super) fn schema_check_existing(db: &Database) -> Result<(), OpenError> {
    let r = db.begin_read().map_err(|e| OpenError::Corrupt {
        path: String::new(),
        details: format!("failed to begin read transaction: {e}"),
    })?;

    match r.open_table(tables::NODE_DATAPLANE) {
        Ok(_) => Ok(()),
        Err(error) if error.to_string().contains("does not exist") => Ok(()),
        Err(error) => Err(schema_mismatch(tables::NODE_DATAPLANE.name(), error)),
    }
}

fn schema_mismatch(name: &str, error: impl std::fmt::Display) -> OpenError {
    OpenError::SchemaMismatch {
        path: String::new(),
        expected: format!("table `{name}` with compiled type signature"),
        actual: error.to_string(),
        hint: "redb table type mismatch — delete state.redb and restart".to_string(),
    }
}

/// Verify that every expected table exists and has the correct type.
///
/// Returns `Ok(())` if all tables match.  Returns `Err(OpenError::SchemaMismatch)`
/// if any table has a mismatched type signature (operator must delete and restart).
pub(super) fn schema_check(db: &Database) -> Result<(), OpenError> {
    let r = db.begin_read().map_err(|e| OpenError::Corrupt {
        path: String::new(),
        details: format!("failed to begin read transaction: {e}"),
    })?;

    // Open each table — redb type-checks on open.
    macro_rules! check {
        ($table:expr_2021) => {
            r.open_table($table)
                .map_err(|error| schema_mismatch($table.name(), error))?;
        };
    }

    check!(tables::RES_CLUSTER);
    check!(tables::RES_NS);
    check!(tables::NAMESPACES);
    check!(tables::WATCH_EVENTS);
    check!(tables::RESOURCE_HISTORY_BY_IDENTITY);
    check!(tables::RESOURCE_CURRENT_BY_IDENTITY);
    check!(tables::WATCH_REPLAY_FLOORS);
    check!(tables::WATCH_REPLAY_POSITION_FLOORS);
    check!(tables::OUTBOX_STREAM_WATERMARKS);
    check!(tables::RESOURCES_BY_OWNER);
    check!(tables::RV_TO_KEY);
    check!(tables::NODE_SUBNETS);
    check!(tables::NODE_DATAPLANE);
    check!(tables::META);

    Ok(())
}
