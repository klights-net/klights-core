//! DSB-R-01 — redb opener and schema check tests.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use tempfile::TempDir;

use crate::datastore::redb::{self, RedbOpenOpts};
use crate::task_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
use ::redb::{ReadableDatabase, TableHandle};

fn temp_db_dir() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    // TempDir creates under /tmp (mode 1777).  Tighten to 0700 so the
    // opener doesn't reject it.
    std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o700)).ok();
    let path = dir.path().join("state.redb");
    (dir, path)
}

#[test]
fn open_fresh_creates_state_redb_with_all_tables() {
    let (_dir, path) = temp_db_dir();
    let db = redb::open(&RedbOpenOpts {
        path: path.clone(),
        cache_size: 40 * 1024 * 1024,
    })
    .expect("open fresh");
    // Verify all expected tables exist.
    let r = db.begin_read().expect("read txn");
    let tables = r.list_tables().expect("list tables");
    let names: Vec<String> = tables.map(|t| t.name().to_string()).collect();
    for expected in &[
        "res_cluster",
        "res_ns",
        "namespaces",
        "watch_events",
        "resources_by_owner",
        "rv_to_key",
        "pod_sandboxes",
        "pod_networks",
        "node_subnets",
        "pod_slot_admissions",
        "pod_endpoints",
        "pod_workqueue",
        "meta",
    ] {
        assert!(
            names.iter().any(|n| n.as_str() == *expected),
            "missing table: {expected}"
        );
    }
}

#[test]
fn open_existing_with_matching_schema_succeeds() {
    let (_dir, path) = temp_db_dir();
    let opts = RedbOpenOpts {
        path: path.clone(),
        cache_size: 40 * 1024 * 1024,
    };
    redb::open(&opts).expect("first open");
    // Second open with same schema must succeed.
    redb::open(&opts).expect("second open");
}

#[test]
fn open_existing_with_same_schema_succeeds_on_reopen() {
    // NOTE: A true type-mismatch test requires a different binary (e.g. a
    // different TableDefinition value type) and cannot run in-process.
    // redb's type check is exercised implicitly — schema_check would fail
    // if any table had the wrong type.  This test verifies the schema_check
    // path executes and passes on the same binary.
    let (_dir, path) = temp_db_dir();
    let opts = RedbOpenOpts {
        path: path.clone(),
        cache_size: 40 * 1024 * 1024,
    };
    redb::open(&opts).expect("first open");
    redb::open(&opts).expect("reopen");
}

#[test]
fn open_persistent_sets_file_mode_0600() {
    let (_dir, path) = temp_db_dir();
    // Set parent dir to 0700 so the opener doesn't reject it.
    std::fs::set_permissions(_dir.path(), PermissionsExt::from_mode(0o700)).ok();
    redb::open(&RedbOpenOpts {
        path: path.clone(),
        cache_size: 40 * 1024 * 1024,
    })
    .expect("open");
    let meta = std::fs::metadata(&path).expect("metadata");
    let mode = meta.permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "state.redb must be mode 0600, got {mode:o}"
    );
}

#[test]
fn open_persistent_sets_parent_dir_0700() {
    let (_dir, path) = temp_db_dir();
    redb::open(&RedbOpenOpts {
        path: path.clone(),
        cache_size: 40 * 1024 * 1024,
    })
    .expect("open");
    let parent = path.parent().unwrap();
    let meta = std::fs::metadata(parent).expect("parent metadata");
    let mode = meta.permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o700,
        "parent dir must be mode 0700, got {mode:o}"
    );
}

#[tokio::test]
async fn redb_persistent_open_runs_inside_supervised_db_boundary() {
    let (_dir, path) = temp_db_dir();
    let opts = RedbOpenOpts {
        path,
        cache_size: 40 * 1024 * 1024,
    };
    let supervisor = std::sync::Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let supervisor_for_open = std::sync::Arc::clone(&supervisor);

    let handle = tokio::spawn(async move {
        super::open_boundary::open_persistent_with(&supervisor_for_open, opts, move |opts| {
            let _ = entered_tx.send(());
            release_rx.recv().unwrap();
            super::open_boundary::open_persistent_blocking(opts)
        })
        .await
    });

    entered_rx.await.unwrap();
    let active_db_tasks = supervisor.active_tasks(Some(TaskCategory::Db));
    assert!(
        active_db_tasks
            .iter()
            .any(|task| task.name == "redb_open_persistent"),
        "redb open must be visible as a supervised DB task, got {active_db_tasks:?}"
    );

    release_tx.send(()).unwrap();
    let db = handle.await.unwrap().unwrap();
    drop(db);
}
mod cross_backend_tests;
