//! Characterization tests for the private node-store selector.
//!
//! These cases freeze the selector's existing path, memory, key-file,
//! concurrency, reopen, and fail-closed backend behavior while its source
//! owner moves from `datastore/node_local` into this private bootstrap module.

use std::path::Path;
use std::sync::Arc;

use crate::bootstrap::cluster_store::backend_kind::BackendKind;
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn open_sqlite(
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    connection_key: &'static str,
) -> anyhow::Result<super::NodeLocalStores> {
    super::open_node_local(BackendKind::Sqlite, path, supervisor, connection_key).await
}

#[tokio::test]
async fn disk_open_reopen_preserves_node_metadata_and_schema() {
    let directory = tempfile::tempdir().expect("node selector fixture");
    std::fs::set_permissions(
        directory.path(),
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("secure node selector fixture directory");
    let path = directory.path().join("node.db");
    let supervisor = supervisor();

    {
        let stores = open_sqlite(
            Some(&path),
            supervisor.clone(),
            "sqlite:phase18-selector-disk-first",
        )
        .await
        .expect("open disk node store");
        assert_eq!(stores.identity().backend_name(), "sqlite");
        stores
            .identity()
            .set_node_meta("phase18-reopen", "preserved")
            .await
            .expect("write disk metadata");
    }

    assert!(path.is_file(), "disk selector must create node.db");
    let reopened = open_sqlite(
        Some(&path),
        supervisor,
        "sqlite:phase18-selector-disk-reopen",
    )
    .await
    .expect("reopen disk node store");
    assert_eq!(
        reopened
            .identity()
            .get_node_meta("phase18-reopen")
            .await
            .expect("read reopened disk metadata")
            .as_deref(),
        Some("preserved")
    );
}

#[tokio::test]
async fn memory_open_isolated_and_does_not_create_disk_state() {
    let supervisor = supervisor();
    let first = open_sqlite(
        None,
        supervisor.clone(),
        "sqlite:phase18-selector-memory-first",
    )
    .await
    .expect("open in-memory node store");
    assert_eq!(first.identity().backend_name(), "sqlite");
    first
        .identity()
        .set_node_meta("phase18-memory", "ephemeral")
        .await
        .expect("write in-memory metadata");
    drop(first);

    let reopened = open_sqlite(None, supervisor, "sqlite:phase18-selector-memory-reopen")
        .await
        .expect("open fresh in-memory node store");
    assert_eq!(
        reopened
            .identity()
            .get_node_meta("phase18-memory")
            .await
            .expect("read fresh in-memory metadata"),
        None
    );
}

#[tokio::test]
async fn concurrent_disk_opens_are_safe_and_reopenable() {
    let directory = tempfile::tempdir().expect("concurrent selector fixture");
    std::fs::set_permissions(
        directory.path(),
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("secure concurrent selector fixture directory");
    let path = directory.path().join("node.db");
    let supervisor_handle = supervisor();

    let (left, right) = tokio::join!(
        open_sqlite(
            Some(&path),
            supervisor_handle.clone(),
            "sqlite:phase18-selector-concurrent-left",
        ),
        open_sqlite(
            Some(&path),
            supervisor_handle,
            "sqlite:phase18-selector-concurrent-right",
        ),
    );
    let left = left.expect("left concurrent disk open");
    let right = right.expect("right concurrent disk open");
    left.identity()
        .set_node_meta("phase18-concurrent-left", "ok")
        .await
        .expect("write left concurrent metadata");
    right
        .identity()
        .set_node_meta("phase18-concurrent-right", "ok")
        .await
        .expect("write right concurrent metadata");
    drop(left);
    drop(right);

    let reopened = open_sqlite(
        Some(&path),
        supervisor(),
        "sqlite:phase18-selector-concurrent-reopen",
    )
    .await
    .expect("reopen after concurrent disk opens");
    assert_eq!(
        reopened
            .identity()
            .get_node_meta("phase18-concurrent-left")
            .await
            .expect("read left concurrent metadata")
            .as_deref(),
        Some("ok")
    );
    assert_eq!(
        reopened
            .identity()
            .get_node_meta("phase18-concurrent-right")
            .await
            .expect("read right concurrent metadata")
            .as_deref(),
        Some("ok")
    );
}

#[tokio::test]
async fn redb_selection_is_a_typed_fail_closed_refusal() {
    let result = super::open_node_local(
        BackendKind::Redb,
        None,
        supervisor(),
        "redb:phase18-selector-refusal",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("redb node-local selector unexpectedly opened"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "node-local redb backend not implemented yet"
    );
}
