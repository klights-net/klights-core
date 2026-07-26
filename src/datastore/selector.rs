//! Passive cluster backend adapter dispatch.
//!
//! Returns the selected passive `DatastoreHandle` so no caller distinguishes
//! which backend was chosen. Replication sequencing is composed separately by
//! the root after Raft has been constructed from this passive handle. The
//! composition root owns environment, backend, mode, and path selection and
//! passes only the resulting typed request into this module.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::datastore::backend::DatastoreHandle;
use crate::datastore::sqlite;
use klights_supervisor::TaskSupervisor;

/// Fully selected passive cluster-store adapter request.
///
/// Root composition constructs this value after parsing and validating ambient
/// configuration. Variant-specific fields prevent persistence from receiving
/// unused root configuration or discovering a backend, mode, key, or path.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PassiveStoreOpenRequest<'a> {
    SqliteInMemory,
    SqlitePersistent {
        cluster_db_path: &'a Path,
        db_key_file: Option<&'a Path>,
    },
    RedbInMemory,
    RedbPersistent {
        cluster_db_path: &'a Path,
    },
}

/// Open the already-selected passive cluster datastore adapter.
///
/// Every variant returns the concrete backend behind a `DatastoreHandle`; this
/// dispatch never reads ambient configuration or installs replication behavior.
pub(crate) async fn open(
    request: PassiveStoreOpenRequest<'_>,
    supervisor: Arc<TaskSupervisor>,
) -> Result<DatastoreHandle> {
    match request {
        PassiveStoreOpenRequest::SqliteInMemory => {
            tracing::info!(backend = "sqlite", mode = "in-memory", "opening datastore");
            let executor = crate::sqlite_boundary::DbExecutor::open_in_memory(
                supervisor,
                "sqlite:selector-in-memory",
            )
            .await?;
            let ds = sqlite::Datastore::new_in_memory_with_watch_and_executor(executor).await?;
            Ok(Arc::new(ds))
        }
        PassiveStoreOpenRequest::SqlitePersistent {
            cluster_db_path,
            db_key_file,
        } => {
            tracing::info!(backend = "sqlite", mode = "persistent", "opening datastore");
            let ds =
                sqlite::Datastore::new_persistent_paths(cluster_db_path, supervisor, db_key_file)
                    .await?;
            Ok(Arc::new(ds))
        }
        PassiveStoreOpenRequest::RedbInMemory => {
            tracing::info!(backend = "redb", mode = "in-memory", "opening datastore");
            let ds =
                crate::datastore::redb::RedbDatastore::new_in_memory_with_supervisor(supervisor)
                    .await?;
            Ok(Arc::new(ds))
        }
        PassiveStoreOpenRequest::RedbPersistent { cluster_db_path } => {
            tracing::info!(backend = "redb", mode = "persistent", "opening datastore");
            let ds =
                crate::datastore::redb::RedbDatastore::new_persistent(cluster_db_path, supervisor)
                    .await?;
            Ok(Arc::new(ds))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PassiveStoreOpenRequest, open};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::sync::Arc;

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    #[tokio::test]
    async fn typed_in_memory_requests_open_both_passive_adapters() {
        for request in [
            PassiveStoreOpenRequest::SqliteInMemory,
            PassiveStoreOpenRequest::RedbInMemory,
        ] {
            let store = open(request, supervisor()).await.expect("open adapter");
            assert_eq!(store.get_current_resource_version().await.unwrap(), 0);
            store.close();
        }
    }

    #[tokio::test]
    async fn typed_persistent_requests_preserve_adapter_paths() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_cluster = dir.path().join("sqlite/cluster.db");
        let sqlite_node = dir.path().join("sqlite/node.db");
        let sqlite = open(
            PassiveStoreOpenRequest::SqlitePersistent {
                cluster_db_path: &sqlite_cluster,
                db_key_file: None,
            },
            supervisor(),
        )
        .await
        .expect("open persistent sqlite");
        assert_eq!(sqlite.get_current_resource_version().await.unwrap(), 0);
        sqlite.close();
        assert!(sqlite_cluster.is_file());
        assert!(
            !sqlite_node.exists(),
            "passive cluster-store open must not create node.db"
        );

        let redb_path = dir.path().join("redb/cluster.redb");
        let redb = open(
            PassiveStoreOpenRequest::RedbPersistent {
                cluster_db_path: &redb_path,
            },
            supervisor(),
        )
        .await
        .expect("open persistent redb");
        assert_eq!(redb.get_current_resource_version().await.unwrap(), 0);
        redb.close();
        assert!(redb_path.is_file());
    }
}
