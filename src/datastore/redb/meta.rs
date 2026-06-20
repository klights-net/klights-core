//! Schema-check marker for the redb backend.
//!
//! Every typed table defined in `tables.rs` is opened inside a `read_txn`.
//! redb returns a `TableTypeError` if the stored type signature doesn't
//! match the current `TableDefinition` — that is our schema-mismatch detector.

use ::redb::{Database, ReadableDatabase, TableHandle};

use crate::datastore::errors::OpenError;

use super::tables;

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
            r.open_table($table).map_err(|e| {
                let name = $table.name();
                OpenError::SchemaMismatch {
                    path: String::new(),
                    expected: format!("table `{name}` with compiled type signature"),
                    actual: format!("{e}"),
                    hint: "redb table type mismatch — delete state.redb and restart".to_string(),
                }
            })?;
        };
    }

    check!(tables::RES_CLUSTER);
    check!(tables::RES_NS);
    check!(tables::NAMESPACES);
    check!(tables::WATCH_EVENTS);
    check!(tables::WATCH_REPLAY_FLOORS);
    check!(tables::RESOURCES_BY_OWNER);
    check!(tables::RV_TO_KEY);
    check!(tables::POD_SANDBOXES);
    check!(tables::POD_NETWORKS);
    check!(tables::NODE_SUBNETS);
    check!(tables::POD_SLOT_ADMISSIONS);
    check!(tables::POD_ENDPOINTS);
    check!(tables::POD_WORKQUEUE);
    check!(tables::META);

    Ok(())
}
