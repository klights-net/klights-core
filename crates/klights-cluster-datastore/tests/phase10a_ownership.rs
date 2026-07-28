use std::sync::Arc;

use klights_cluster_datastore::{
    errors::{DatastoreError, OpenError},
    redb::{self as cluster_redb, RedbAccessor, tables},
    sqlite,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use redb::ReadableDatabase;

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

#[test]
fn concrete_cluster_persistence_errors_are_owned_by_the_adapter_package() {
    let conflict = DatastoreError::conflict("stale resource version");
    assert!(conflict.is_conflict());

    let mismatch = OpenError::SchemaMismatch {
        path: "cluster.db".to_string(),
        expected: "expected".to_string(),
        actual: "actual".to_string(),
        hint: "restore the matching schema".to_string(),
    };
    assert_eq!(mismatch.path_hint(), "cluster.db");
}

#[tokio::test]
async fn explicit_sqlite_open_owns_the_current_cluster_schema() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10a:sqlite")
        .await
        .expect("open supervised SQLite adapter");
    let tables = executor
        .call_raw("phase10a:schema-owner", |connection| {
            let mut statement = connection.prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                 ORDER BY name",
            )?;
            let tables = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(tables)
        })
        .await
        .expect("read current cluster schema");

    assert_eq!(tables.len(), 15);
    assert!(tables.iter().any(|name| name == "outbox_stream_watermarks"));
    assert!(tables.iter().any(|name| name == "watch_events"));
    assert!(!tables.iter().any(|name| name == "pod_sandboxes"));
}

#[tokio::test]
async fn explicit_redb_open_owns_current_tables_and_supervised_calls() {
    let supervisor = supervisor();
    let database = cluster_redb::open_in_memory(supervisor.as_ref())
        .await
        .expect("open supervised Redb adapter");
    let accessor = RedbAccessor::new(Arc::new(database), supervisor);

    accessor
        .call("phase10a:redb-schema-owner", |database| {
            let read = database.begin_read()?;
            let metadata = read.open_table(tables::META)?;
            assert!(metadata.get("resource_version")?.is_none());
            read.open_table(tables::OUTBOX_STREAM_WATERMARKS)?;
            read.open_table(tables::WATCH_EVENTS)?;
            Ok(())
        })
        .await
        .expect("inspect current Redb schema through supervised boundary");
    accessor.close();
}
