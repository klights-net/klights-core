//! Characterization tests for the private node-store selector.
//!
//! These cases freeze the selector's existing path, memory, key-file,
//! concurrency, reopen, and fail-closed backend behavior while its source
//! owner moves from `datastore/node_local` into this private bootstrap module.

use std::path::Path;
use std::sync::Arc;

use crate::datastore::backend_kind::BackendKind;
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn open_sqlite(
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> anyhow::Result<super::NodeLocalStores> {
    super::open_node_local(
        BackendKind::Sqlite,
        path,
        supervisor,
        key_file,
        connection_key,
    )
    .await
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
            None,
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
        None,
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
        None,
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

    let reopened = open_sqlite(
        None,
        supervisor,
        None,
        "sqlite:phase18-selector-memory-reopen",
    )
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
            None,
            "sqlite:phase18-selector-concurrent-left",
        ),
        open_sqlite(
            Some(&path),
            supervisor_handle,
            None,
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
        None,
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

#[cfg(not(feature = "sqlcipher"))]
#[tokio::test]
async fn key_file_is_rejected_without_sqlcipher_instead_of_falling_back() {
    let directory = tempfile::tempdir().expect("key selector fixture");
    let key_path = directory.path().join("node.key");
    std::fs::write(&key_path, b"k").expect("write selector key");

    let result = open_sqlite(
        None,
        supervisor(),
        Some(&key_path),
        "sqlite:phase18-selector-key-rejected",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("key file must not silently select plaintext SQLite"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "SQLCipher encryption requested but the 'sqlcipher' cargo feature is not enabled. \
Rebuild with --features sqlcipher"
    );
}

#[cfg(feature = "sqlcipher")]
#[tokio::test]
async fn key_file_preserves_existing_sqlcipher_open_refusal() {
    let directory = tempfile::tempdir().expect("SQLCipher selector fixture");
    std::fs::set_permissions(
        directory.path(),
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("secure SQLCipher selector fixture directory");
    let key_path = directory.path().join("node.key");
    let db_path = directory.path().join("node.db");
    std::fs::write(&key_path, b"k").expect("write selector key");
    let result = open_sqlite(
        Some(&db_path),
        supervisor(),
        Some(&key_path),
        "sqlite:phase18-selector-key-refusal",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("the existing SQLCipher opener unexpectedly accepted a byte key"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        r#"Rusqlite("Unsupported value "Blob([107])"")"#
    );
}

#[tokio::test]
async fn redb_selection_is_a_typed_fail_closed_refusal() {
    let result = super::open_node_local(
        BackendKind::Redb,
        None,
        supervisor(),
        None,
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
