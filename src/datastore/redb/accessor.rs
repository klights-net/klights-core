//! Shared redb DB accessor with supervised `db_call` helpers.
//! All domain stores compose this instead of reaching into RedbDatastore.

use std::sync::{Arc, Mutex};

use ::redb::Database;
use anyhow::{Result, anyhow};

use crate::task_supervisor::TaskSupervisor;

/// Shared synchronous-redb access through the TaskSupervisor DB pool.
///
/// Each domain store holds its own `Arc<RedbAccessor>`. When
/// RedbDatastore composes the stores it creates one accessor and
/// shares it across all of them.
pub struct RedbAccessor {
    db: Mutex<Option<Arc<Database>>>,
    supervisor: Arc<TaskSupervisor>,
}

impl RedbAccessor {
    pub fn new(db: Arc<Database>, supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            db: Mutex::new(Some(db)),
            supervisor,
        }
    }

    /// Release the underlying database handle.
    pub fn close(&self) {
        self.db.lock().unwrap().take();
    }

    /// Direct access to the underlying DB for snapshot/restore operations.
    /// Caller must ensure the DB is not closed and that access is properly
    /// serialized (snapshot/restore are infrequent, heavyweight ops).
    pub fn db(&self) -> anyhow::Result<Arc<Database>> {
        self.db
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("redb datastore closed"))
    }

    /// Run a synchronous redb closure on the DB-category blocking pool.
    pub async fn call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        let db = self
            .db
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| anyhow!("redb datastore closed"))?
            .clone();
        let label_owned = label.to_string();
        self.supervisor
            .run_db_blocking(label_owned, "redb", move || f(&db))
            .await
            .map_err(|e| anyhow!("supervisor error: {e}"))?
    }

    /// Execute a write `call` with automatic retry on RV conflict.
    pub async fn call_with_retry<T, F, C>(&self, base_label: &str, factory: C) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
        C: Fn(bool) -> F + Send + 'static,
    {
        const MAX_RV_RETRIES: u32 = 3;
        let mut attempt = 0u32;
        loop {
            let skip_rv_check = attempt > 0;
            let label = if skip_rv_check {
                format!("{}_rvretry{}", base_label, attempt)
            } else {
                base_label.to_string()
            };
            match self.call(&label, factory(skip_rv_check)).await {
                ok @ Ok(_) => return ok,
                Err(e)
                    if crate::datastore::errors::is_conflict_error(&e)
                        && attempt < MAX_RV_RETRIES =>
                {
                    attempt += 1;
                    tracing::debug!(
                        "{}: RV conflict, retry {}/{}",
                        base_label,
                        attempt,
                        MAX_RV_RETRIES,
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
