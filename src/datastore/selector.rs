//! Cluster backend selection from parsed bootstrap config.
//!
//! Returns `DatastoreHandle` (Arc<dyn DatastoreBackend>) so no caller
//! distinguishes which backend was chosen.

use std::sync::Arc;

use anyhow::Result;

use crate::bootstrap::config::KlightsConfig;
use crate::datastore::backend::DatastoreHandle;
use crate::datastore::backend_kind::BackendKind;
use crate::datastore::replicated::{ReplicatedDatastore, ReplicationMode, ReplicationObserver};
use crate::datastore::sqlite;
use crate::task_supervisor::TaskSupervisor;

/// Open the cluster datastore. Dispatches on `config.in_memory` and
/// `config.datastore_backend`.
///
/// Both in-memory and persistent paths return a `DatastoreHandle` backed by
/// `ReplicatedDatastore`.  No caller knows which concrete backend was selected.
pub async fn open(cfg: &KlightsConfig, supervisor: Arc<TaskSupervisor>) -> Result<DatastoreHandle> {
    open_raft_cluster(cfg, supervisor, None).await
}

pub async fn open_raft_cluster(
    cfg: &KlightsConfig,
    supervisor: Arc<TaskSupervisor>,
    observer: Option<ReplicationObserver>,
) -> Result<DatastoreHandle> {
    open_with_mode(
        cfg,
        supervisor,
        ReplicationMode::Raft {
            node_name: cfg.node_name.clone(),
        },
        observer,
    )
    .await
}

async fn open_with_mode(
    cfg: &KlightsConfig,
    supervisor: Arc<TaskSupervisor>,
    replication_mode: ReplicationMode,
    observer: Option<ReplicationObserver>,
) -> Result<DatastoreHandle> {
    let kind = cfg.datastore_backend;
    let mode = if cfg.in_memory {
        "in-memory"
    } else {
        "persistent"
    };
    tracing::info!(backend = kind.as_str(), mode, "opening datastore");

    if cfg.in_memory {
        open_in_memory(kind, supervisor.clone(), cfg, replication_mode, observer).await
    } else {
        open_persistent(kind, cfg, supervisor, replication_mode, observer).await
    }
}

async fn open_persistent(
    kind: BackendKind,
    cfg: &KlightsConfig,
    supervisor: Arc<TaskSupervisor>,
    replication_mode: ReplicationMode,
    observer: Option<ReplicationObserver>,
) -> Result<DatastoreHandle> {
    match kind {
        BackendKind::Sqlite => {
            let ds = sqlite::Datastore::new_persistent_paths(
                &cfg.cluster_db_path,
                &cfg.node_db_path,
                supervisor,
                cfg.db_key_file.as_deref(),
            )
            .await?;
            Ok(Arc::new(ReplicatedDatastore::with_observer(
                Arc::new(ds),
                replication_mode,
                observer,
            )))
        }
        BackendKind::Redb => {
            let ds = crate::datastore::redb::RedbDatastore::new_persistent(
                &cfg.cluster_db_path,
                supervisor,
            )
            .await?;
            Ok(Arc::new(ReplicatedDatastore::with_observer(
                Arc::new(ds),
                replication_mode,
                observer,
            )))
        }
    }
}

async fn open_in_memory(
    kind: BackendKind,
    supervisor: Arc<TaskSupervisor>,
    _cfg: &KlightsConfig,
    replication_mode: ReplicationMode,
    observer: Option<ReplicationObserver>,
) -> Result<DatastoreHandle> {
    match kind {
        BackendKind::Sqlite => {
            let executor =
                sqlite::DbExecutor::open_in_memory(supervisor, "sqlite:selector-in-memory").await?;
            let ds = sqlite::Datastore::new_in_memory_with_watch_and_executor(executor).await?;
            Ok(Arc::new(ReplicatedDatastore::with_observer(
                Arc::new(ds),
                replication_mode,
                observer,
            )))
        }
        BackendKind::Redb => {
            let ds =
                crate::datastore::redb::RedbDatastore::new_in_memory_with_supervisor(supervisor)
                    .await?;
            Ok(Arc::new(ReplicatedDatastore::with_observer(
                Arc::new(ds),
                replication_mode,
                observer,
            )))
        }
    }
}
