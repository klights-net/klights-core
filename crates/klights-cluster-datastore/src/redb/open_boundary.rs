//! Supervised blocking boundary for redb database open and table setup.

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use ::redb::{Database, ReadableTable};

use crate::errors::OpenError;
use klights_supervisor::TaskSupervisor;

use super::meta;
use super::opener::RedbOpenOpts;
use super::tables;

const REDB_OPEN_RETRY_ATTEMPTS: usize = 50;
const REDB_OPEN_RETRY_DELAY: Duration = Duration::from_millis(500);

pub async fn open_persistent(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
) -> Result<Database, OpenError> {
    let mut last_retry_error = String::new();

    for attempt in 0..REDB_OPEN_RETRY_ATTEMPTS {
        match open_persistent_once(supervisor, opts.clone(), open_persistent_blocking).await {
            Ok(db) => return Ok(db),
            Err(err)
                if is_retryable_already_open(&err) && attempt + 1 < REDB_OPEN_RETRY_ATTEMPTS =>
            {
                last_retry_error = err.to_string();
                supervisor
                    .sleep("redb_open_retry_delay", REDB_OPEN_RETRY_DELAY)
                    .await
                    .map_err(|sleep_err| OpenError::Corrupt {
                        path: opts.path.display().to_string(),
                        details: format!("redb open retry timer failed: {sleep_err}"),
                    })?;
            }
            Err(err) => return Err(err),
        }
    }

    Err(OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to open redb database after retries: {last_retry_error}"),
    })
}

#[cfg(test)]
pub(super) async fn open_persistent_with<F>(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
    opener: F,
) -> Result<Database, OpenError>
where
    F: FnOnce(&RedbOpenOpts) -> Result<Database, OpenError> + Send + 'static,
{
    open_persistent_once(supervisor, opts, opener).await
}

async fn open_persistent_once<F>(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
    opener: F,
) -> Result<Database, OpenError>
where
    F: FnOnce(&RedbOpenOpts) -> Result<Database, OpenError> + Send + 'static,
{
    let path = opts.path.clone();
    supervisor
        .run_db_blocking("redb_open_persistent", "redb", move || opener(&opts))
        .await
        .map_err(|err| OpenError::Corrupt {
            path: path.display().to_string(),
            details: format!("supervised redb open task failed: {err}"),
        })?
}

pub async fn open_in_memory(supervisor: &TaskSupervisor) -> anyhow::Result<Database> {
    supervisor
        .run_db_blocking("redb_open_in_memory", "redb", open_in_memory_blocking)
        .await
        .map_err(|err| anyhow::anyhow!("supervised in-memory redb open task failed: {err}"))?
}

fn open_in_memory_blocking() -> anyhow::Result<Database> {
    let db = ::redb::Database::builder()
        .create_with_backend(::redb::backends::InMemoryBackend::new())
        .map_err(|e| anyhow::anyhow!("in-memory redb: {e}"))?;
    initialize_tables(&db).map_err(|e| anyhow::anyhow!("in-memory redb table init: {e}"))?;
    Ok(db)
}

fn open_persistent_blocking(opts: &RedbOpenOpts) -> Result<Database, OpenError> {
    ensure_parent_dir(&opts.path)?;
    let db = try_open_db(opts).map_err(|e| OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to create/open redb database: {e}"),
    })?;
    initialize_tables(&db).map_err(|e| OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to initialize redb tables: {e}"),
    })?;
    std::fs::set_permissions(&opts.path, PermissionsExt::from_mode(0o600)).map_err(|e| {
        OpenError::Filesystem {
            path: opts.path.clone(),
            source: e,
        }
    })?;
    meta::schema_check(&db).map_err(|e| attach_path(opts, e))?;
    Ok(db)
}

fn ensure_parent_dir(path: &std::path::Path) -> Result<(), OpenError> {
    if let Some(parent) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .create(parent)
            .map_err(|e| OpenError::Filesystem {
                path: parent.to_path_buf(),
                source: e,
            })?;
        std::fs::set_permissions(parent, PermissionsExt::from_mode(0o700)).map_err(|e| {
            OpenError::Filesystem {
                path: parent.to_path_buf(),
                source: e,
            }
        })?;
    }
    Ok(())
}

fn try_open_db(opts: &RedbOpenOpts) -> std::result::Result<Database, redb::DatabaseError> {
    if opts.path.exists() {
        Database::builder()
            .set_cache_size(opts.cache_size)
            .open(&opts.path)
    } else {
        Database::builder()
            .set_cache_size(opts.cache_size)
            .create(&opts.path)
    }
}

fn initialize_tables(db: &Database) -> anyhow::Result<()> {
    let w = db.begin_write()?;
    {
        let _ = w.open_table(tables::RES_CLUSTER);
        let _ = w.open_table(tables::RES_NS);
        let _ = w.open_table(tables::NAMESPACES);
        let _ = w.open_table(tables::WATCH_EVENTS_LEGACY);
        let _ = w.open_table(tables::WATCH_EVENTS);
        let _ = w.open_table(tables::WATCH_REPLAY_FLOORS);
        let _ = w.open_table(tables::WATCH_REPLAY_POSITION_FLOORS);
        let _ = w.open_table(tables::APPLIED_OUTBOX);
        let _ = w.open_table(tables::OUTBOX_STREAM_WATERMARKS);
        let _ = w.open_table(tables::RESOURCES_BY_OWNER);
        let _ = w.open_table(tables::RV_TO_KEY);
        let _ = w.open_table(tables::NODE_SUBNETS);
        let _ = w.open_table(tables::POD_CLEANUP_INTENTS);
        let _ = w.open_table(tables::META);
        let _ = w.open_table(tables::KLIGHTS_META);
    }
    migrate_watch_events_v2(&w)?;
    w.commit()?;
    Ok(())
}

fn migrate_watch_events_v2(w: &redb::WriteTransaction) -> anyhow::Result<()> {
    let mut high_water = {
        let current = w.open_table(tables::WATCH_EVENTS)?;
        current.last()?.map_or(0, |(key, _)| key.value())
    };
    if high_water == 0 {
        let legacy_rows: Vec<(u64, Vec<u8>)> = {
            let legacy = w.open_table(tables::WATCH_EVENTS_LEGACY)?;
            legacy
                .iter()?
                .map(|entry| entry.map(|(rv, value)| (rv.value(), value.value().to_vec())))
                .collect::<Result<_, _>>()?
        };
        if !legacy_rows.is_empty() {
            let mut current = w.open_table(tables::WATCH_EVENTS)?;
            for (resource_version, value) in legacy_rows {
                high_water += 1;
                let mut event: serde_json::Value = serde_json::from_slice(&value)?;
                if let Some(object) = event.as_object_mut() {
                    object.insert(
                        "resourceVersion".to_string(),
                        serde_json::Value::from(resource_version),
                    );
                }
                let encoded = serde_json::to_vec(&event)?;
                current.insert(high_water, encoded.as_slice())?;
            }
        }
    }
    let mut meta = w.open_table(tables::META)?;
    let persisted = meta
        .get("watch_event_id")?
        .and_then(|value| std::str::from_utf8(value.value()).ok()?.parse::<u64>().ok())
        .unwrap_or(0);
    high_water = high_water.max(persisted);
    meta.insert("watch_event_id", high_water.to_string().as_bytes())?;
    Ok(())
}

fn attach_path(opts: &RedbOpenOpts, err: OpenError) -> OpenError {
    match err {
        OpenError::SchemaMismatch {
            expected,
            actual,
            hint,
            ..
        } => OpenError::SchemaMismatch {
            path: opts.path.display().to_string(),
            expected,
            actual,
            hint,
        },
        other => other,
    }
}

fn is_retryable_already_open(err: &OpenError) -> bool {
    err.to_string().contains("already open")
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use klights_supervisor::{TaskCategory, TaskCategoryConfig};
    use redb::{ReadableDatabase, TableHandle};
    use tempfile::TempDir;

    fn temp_db_dir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().expect("tempdir");
        std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o700)).ok();
        let path = dir.path().join("state.redb");
        (dir, path)
    }

    fn insert_watch_event(
        write: &redb::WriteTransaction,
        resource_version: i64,
        event: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let event_id = {
            let mut metadata = write.open_table(tables::META)?;
            let current = metadata
                .get("watch_event_id")?
                .and_then(|value| std::str::from_utf8(value.value()).ok()?.parse::<u64>().ok())
                .unwrap_or(0);
            let next = current.saturating_add(1);
            metadata.insert("watch_event_id", next.to_string().as_bytes())?;
            next
        };
        let mut stored = event.clone();
        if let Some(object) = stored.as_object_mut() {
            object.insert(
                "resourceVersion".to_string(),
                serde_json::Value::from(resource_version),
            );
        }
        let encoded = serde_json::to_vec(&stored)?;
        write
            .open_table(tables::WATCH_EVENTS)?
            .insert(event_id, encoded.as_slice())?;
        Ok(())
    }

    #[test]
    fn migrates_legacy_rv_key_and_allows_same_rv_sibling() {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let legacy_event = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "namespace": "default",
            "name": "legacy",
            "eventType": "ADDED",
            "data": {}
        });
        let write = db.begin_write().unwrap();
        {
            let mut legacy = write.open_table(tables::WATCH_EVENTS_LEGACY).unwrap();
            let encoded = serde_json::to_vec(&legacy_event).unwrap();
            legacy.insert(7, encoded.as_slice()).unwrap();
        }
        write.commit().unwrap();

        initialize_tables(&db).unwrap();
        let write = db.begin_write().unwrap();
        insert_watch_event(
            &write,
            7,
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "namespace": "default",
                "name": "sibling",
                "eventType": "ADDED",
                "data": {}
            }),
        )
        .unwrap();
        write.commit().unwrap();

        let read = db.begin_read().unwrap();
        let table = read.open_table(tables::WATCH_EVENTS).unwrap();
        let rows: Vec<_> = table.iter().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(rows.len(), 2);
        let legacy: serde_json::Value = serde_json::from_slice(rows[0].1.value()).unwrap();
        assert_eq!(legacy["resourceVersion"], 7);
    }

    #[test]
    fn initializes_missing_allocator_metadata_from_existing_v2_rows() {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut events = write.open_table(tables::WATCH_EVENTS).unwrap();
            let encoded = serde_json::to_vec(&serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "namespace": "default",
                "name": "existing",
                "resourceVersion": 7,
                "eventType": "ADDED",
                "data": {}
            }))
            .unwrap();
            events.insert(7, encoded.as_slice()).unwrap();
        }
        write.commit().unwrap();

        initialize_tables(&db).unwrap();
        let write = db.begin_write().unwrap();
        insert_watch_event(
            &write,
            8,
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "namespace": "default",
                "name": "new",
                "eventType": "ADDED",
                "data": {}
            }),
        )
        .unwrap();
        write.commit().unwrap();

        let read = db.begin_read().unwrap();
        let events = read.open_table(tables::WATCH_EVENTS).unwrap();
        let ids = events
            .iter()
            .unwrap()
            .map(|entry| entry.map(|(key, _)| key.value()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(ids, vec![7, 8]);
        let meta = read.open_table(tables::META).unwrap();
        assert_eq!(meta.get("watch_event_id").unwrap().unwrap().value(), b"8");
    }

    #[test]
    fn open_fresh_creates_cluster_tables_without_node_local_tables() {
        let (_dir, path) = temp_db_dir();
        let db = open_persistent_blocking(&RedbOpenOpts {
            path,
            cache_size: 40 * 1024 * 1024,
        })
        .expect("open fresh");
        let read = db.begin_read().expect("read transaction");
        let names = read
            .list_tables()
            .expect("list tables")
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>();
        for expected in [
            "res_cluster",
            "res_ns",
            "namespaces",
            "watch_events",
            "resources_by_owner",
            "rv_to_key",
            "node_subnets",
            "pod_cleanup_intents",
            "meta",
        ] {
            assert!(
                names.iter().any(|name| name == expected),
                "missing table: {expected}"
            );
        }
        for node_local in [
            "pod_sandboxes",
            "pod_networks",
            "pod_slot_admissions",
            "pod_endpoints",
            "pod_workqueue",
        ] {
            assert!(
                !names.iter().any(|name| name == node_local),
                "cluster store must not create node-local table: {node_local}"
            );
        }
    }

    #[test]
    fn existing_database_with_current_schema_reopens() {
        let (_dir, path) = temp_db_dir();
        let opts = RedbOpenOpts {
            path,
            cache_size: 40 * 1024 * 1024,
        };
        open_persistent_blocking(&opts).expect("first open");
        open_persistent_blocking(&opts).expect("reopen");
    }

    #[test]
    fn persistent_open_sets_file_and_parent_permissions() {
        let (_dir, path) = temp_db_dir();
        open_persistent_blocking(&RedbOpenOpts {
            path: path.clone(),
            cache_size: 40 * 1024 * 1024,
        })
        .expect("open");
        assert_eq!(
            std::fs::metadata(&path)
                .expect("database metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(path.parent().expect("parent"))
                .expect("parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[tokio::test]
    async fn persistent_open_runs_inside_supervised_db_boundary() {
        let (_dir, path) = temp_db_dir();
        let opts = RedbOpenOpts {
            path,
            cache_size: 40 * 1024 * 1024,
        };
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let supervisor_for_open = Arc::clone(&supervisor);

        let handle = tokio::spawn(async move {
            open_persistent_with(&supervisor_for_open, opts, move |opts| {
                let _ = entered_tx.send(());
                release_rx.recv().unwrap();
                open_persistent_blocking(opts)
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
        drop(handle.await.unwrap().unwrap());
    }
}
