//! Root-owned node-local selection and focused-port composition tests.

use std::sync::Arc;

use crate::bootstrap::node_store::NodeLocalStores;
use crate::datastore::backend_kind::BackendKind;
use crate::datastore::node_local::selector;
use klights_node_store::NodeIdentity;
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn open_sqlite_node_local_store() -> Arc<NodeLocalStores> {
    let executor = klights_node_datastore::open::open_with_opts(
        klights_node_datastore::open::in_memory_opts(),
        supervisor(),
        "sqlite:node-local-backend-test",
    )
    .await
    .expect("open node-local executor");
    Arc::new(NodeLocalStores::from_executor(executor).expect("create sqlite node-local db"))
}

#[tokio::test]
async fn sqlite_store_implements_focused_node_identity() {
    let handle = open_sqlite_node_local_store().await;
    fn assert_identity_trait(_: &dyn NodeIdentity) {}
    let identity = handle.identity();
    assert_identity_trait(identity.as_ref());
    assert_eq!(identity.backend_name(), "sqlite");

    identity
        .set_node_meta("node_uid", "node-a")
        .await
        .expect("write meta through trait object");
    assert_eq!(
        identity.get_node_meta("node_uid").await.expect("read meta"),
        Some("node-a".to_string())
    );
}

#[tokio::test]
async fn selector_creates_sqlite_node_db_and_node_local_schema() {
    let directory = tempfile::tempdir().expect("node-local selector fixture");
    std::fs::set_permissions(
        directory.path(),
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("secure node-local fixture directory");
    let path = directory.path().join("node.db");
    let handle = selector::open_node_local(
        BackendKind::Sqlite,
        Some(&path),
        supervisor(),
        None,
        "sqlite:node-local-selector-test",
    )
    .await
    .expect("open sqlite node-local");

    let identity = handle.identity();
    assert_eq!(identity.backend_name(), "sqlite");
    assert!(path.is_file(), "node-local selector must create node.db");
    identity
        .set_node_meta("schema-owner", "node-local")
        .await
        .expect("node-local selector must initialize the node metadata table");
    assert_eq!(
        identity
            .get_node_meta("schema-owner")
            .await
            .expect("read initialized node metadata table")
            .as_deref(),
        Some("node-local")
    );
}

#[tokio::test]
async fn redb_node_local_selector_fails_fast_until_backend_lands() {
    let result = selector::open_node_local(
        BackendKind::Redb,
        None,
        supervisor(),
        None,
        "redb:node-local-selector-test",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("redb node-local unexpectedly opened"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("node-local redb backend not implemented yet"),
        "unexpected error: {error}"
    );
}
