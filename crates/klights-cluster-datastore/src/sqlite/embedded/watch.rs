use serde_json::Value;

#[cfg(any(test, feature = "test-support"))]
use super::CommitObservationSink;
use super::{CatchUpResource, Datastore, StagedPostCommit};
use klights_cluster_core::Resource;

/// Free function to publish a pending watch event after DB commit.
///
/// This is the single entry point for post-commit watch broadcast.
/// Callable from:
/// - Request handler CRUD paths (Phase 1/2 SingleNode and Leader)
/// - Future Raft FSM apply hook (Phase 3 — every node, leader and
///   follower alike)
///
/// Per HA contract bullet #4, request handlers must never call the
/// watch bus directly — they hand a `StagedPostCommit` back
/// and this function publishes it. tests/source_guard_tests.py enforces this.
#[cfg(any(test, feature = "test-support"))]
pub fn publish_pending(pending: StagedPostCommit, sink: &dyn CommitObservationSink) {
    publish_pending_batch(std::iter::once(pending), sink);
}

#[cfg(any(test, feature = "test-support"))]
pub fn publish_pending_batch(
    pending: impl IntoIterator<Item = StagedPostCommit>,
    sink: &dyn CommitObservationSink,
) {
    let pending = pending.into_iter().collect::<Vec<_>>();
    sink.observe(&pending);
}

/// Map a DB-stored event_type string back to a `Cow<'static, str>` reusing
/// the canonical static literal where possible — most rows are one of the
/// three K8s event types, so the common case is allocation-free.
fn catchup_event_type_from_db(event_type: String) -> std::borrow::Cow<'static, str> {
    match event_type.as_str() {
        "ADDED" => std::borrow::Cow::Borrowed("ADDED"),
        "MODIFIED" => std::borrow::Cow::Borrowed("MODIFIED"),
        "DELETED" => std::borrow::Cow::Borrowed("DELETED"),
        _ => std::borrow::Cow::Owned(event_type),
    }
}

/// Create a StagedPostCommit from raw parameters.
///
/// Used by crud operations to stage a watch event inside the transaction,
/// then publish after commit via `Datastore::publish_watch_event`.
pub fn create_staged_post_commit(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    event_type: &str,
    data: impl Into<std::sync::Arc<Value>>,
) -> StagedPostCommit {
    #[cfg(not(test))]
    {
        let _ = (name, event_type, data);
        StagedPostCommit::new(api_version, kind, namespace, resource_version)
    }

    #[cfg(test)]
    {
        let mut value = data.into().as_ref().clone();
        let object = value
            .as_object_mut()
            .expect("staged test resource must be an object");
        object
            .entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version.to_string()));
        object
            .entry("kind".to_string())
            .or_insert_with(|| Value::String(kind.to_string()));
        let metadata = value
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .expect("staged test resource metadata");
        metadata
            .entry("name".to_string())
            .or_insert_with(|| Value::String(name.to_string()));
        if let Some(namespace) = namespace {
            metadata
                .entry("namespace".to_string())
                .or_insert_with(|| Value::String(namespace.to_string()));
        }
        metadata
            .entry("resourceVersion".to_string())
            .or_insert_with(|| Value::String(resource_version.to_string()));
        let resource =
            Resource::try_from_data(value.into()).expect("staged test resource identity");
        let encoded_json = serde_json::to_vec(&serde_json::json!({
            "type": event_type,
            "object": resource.data.as_ref(),
        }))
        .ok()
        .map(bytes::Bytes::from);
        StagedPostCommit::new(api_version, kind, namespace, resource_version).with_test_event(
            event_type,
            resource,
            encoded_json,
        )
    }
}

impl Datastore {
    pub(super) fn watch_row_to_catchup_resource(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<CatchUpResource> {
        let start = std::time::Instant::now();
        let data_bytes: Vec<u8> = row.get(6)?;
        let data_len = data_bytes.len();
        let data: serde_json::Value = serde_json::from_slice(&data_bytes)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let event_type: String = row.get(5)?;
        let resource = Resource {
            id: 0,
            api_version: row.get(0)?,
            kind: row.get(1)?,
            namespace: row.get(2)?,
            name: row.get(3)?,
            resource_version: row.get(4)?,
            uid: Resource::uid_from_data(&data),
            data: std::sync::Arc::new(data),
        };
        crate::diagnostics::log_slow_watch_replay_decode(
            crate::diagnostics::SlowWatchReplayDecode {
                elapsed: start.elapsed(),
                data_len,
                api_version: &resource.api_version,
                kind: &resource.kind,
                namespace: resource.namespace.as_deref(),
                name: &resource.name,
                resource_version: resource.resource_version,
                event_type: &event_type,
            },
        );
        Ok(CatchUpResource {
            resource,
            event_type: catchup_event_type_from_db(event_type),
        })
    }

    /// memory-improvement.md §10 P1: same mapping as
    /// `watch_row_to_catchup_resource`, but also surfaces the `watch_events.id`
    /// column (position 7) so the snapshot emitter can keyset-page the table
    /// without materializing it all at once.
    pub(super) fn watch_row_to_catchup_resource_with_id(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<(i64, CatchUpResource)> {
        let id: i64 = row.get(7)?;
        let catchup = Self::watch_row_to_catchup_resource(row)?;
        Ok((id, catchup))
    }

    /// Broadcast a watch event after the DB transaction has committed.
    /// Delegates to the free function `publish_pending` so the broadcast
    /// path is identical whether called from CRUD methods or a future
    /// Raft FSM apply hook.
    #[cfg(test)]
    pub fn publish_watch_event(&self, pending: StagedPostCommit) {
        if let Some(commit_sink) = self.commit_sink.as_deref() {
            publish_pending(pending, commit_sink);
        }
    }

    /// Broadcast a batch of watch events after the DB transaction has
    /// committed. Multi-event apply paths (raft/cluster replace) use this so
    /// the post-commit signals are grouped per `(topic, namespace)` through
    /// `publish_pending_batch` instead of emitting one signal per event.
    #[cfg(test)]
    pub fn publish_watch_events(&self, pending: impl IntoIterator<Item = StagedPostCommit>) {
        if let Some(commit_sink) = self.commit_sink.as_deref() {
            publish_pending_batch(pending, commit_sink);
        }
    }
}
