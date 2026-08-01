use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;
#[cfg(test)]
use tracing::Level;

#[cfg(test)]
use crate::watch::{WatchContentType, WatchEvent, WatchReceiver};
#[cfg(test)]
use klights_watch::WatchTopic;

#[cfg(test)]
use super::CommitObservationSink;
use super::{CatchUpResource, Datastore, StagedPostCommit};
use klights_cluster_core::Resource;

#[cfg(test)]
fn log_watch_event_broadcast(event: &WatchEvent) {
    if !tracing::enabled!(target: "klights::datastore::watch_event", Level::DEBUG) {
        return;
    }

    let object = event.object.as_ref();
    let metadata = object.get("metadata").unwrap_or(&Value::Null);
    tracing::debug!(
        target: "klights::datastore::watch_event",
        event_type = %event.event_type,
        api_version = value_str(object.get("apiVersion")),
        kind = value_str(object.get("kind")),
        namespace = value_str(metadata.get("namespace")),
        name = value_str(metadata.get("name")),
        uid = value_str(metadata.get("uid")),
        resource_version = value_str(metadata.get("resourceVersion")),
        generation = value_i64(metadata.get("generation")),
        status_phase = value_str(object.pointer("/status/phase")),
        status_observed_generation = value_i64(object.pointer("/status/observedGeneration")),
        "broadcasting datastore watch event"
    );
}

#[cfg(test)]
fn value_str(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or("")
}

#[cfg(test)]
fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}

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
#[cfg(test)]
pub fn publish_pending(pending: StagedPostCommit, sink: &dyn CommitObservationSink) {
    publish_pending_batch(std::iter::once(pending), sink);
}

#[cfg(test)]
pub fn publish_pending_batch(
    pending: impl IntoIterator<Item = StagedPostCommit>,
    sink: &dyn CommitObservationSink,
) {
    let pending = pending.into_iter().collect::<Vec<_>>();

    #[cfg(test)]
    {
        let events = pending
            .iter()
            .filter_map(staged_test_event)
            .collect::<Vec<_>>();
        for event in &events {
            log_watch_event_broadcast(event);
        }
        crate::watch_commit_observation_adapter::publish_test_events(sink, events);
    }
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
        crate::datastore::types::with_staged_test_resource_event(
            StagedPostCommit::new(api_version, kind, namespace, resource_version),
            event_type,
            name,
            data.into(),
        )
    }
}

#[cfg(test)]
pub fn staged_test_event(pending: &StagedPostCommit) -> Option<WatchEvent> {
    let staged = pending.test_event()?;
    let mut event =
        WatchEvent::from_type(staged.event_type(), staged.resource().data.as_ref().clone());
    event.encoded_payload =
        staged
            .encoded_json()
            .cloned()
            .map(|bytes| crate::watch::events::EncodedWatchPayload {
                content_type: WatchContentType::Json,
                bytes,
            });
    Some(event)
}

#[cfg(test)]
pub fn staged_post_commit_from_event(event: WatchEvent) -> StagedPostCommit {
    let resource = Resource::try_from_data(event.object.clone())
        .expect("test watch event must carry canonical resource identity");
    let encoded_json = event
        .encoded_payload
        .as_ref()
        .filter(|payload| payload.content_type == WatchContentType::Json)
        .map(|payload| payload.bytes.clone());
    StagedPostCommit::new(
        &resource.api_version,
        &resource.kind,
        resource.namespace.as_deref(),
        resource.resource_version,
    )
    .with_test_event(event.event_type.to_string(), resource, encoded_json)
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
        klights_cluster_datastore::diagnostics::log_slow_watch_replay_decode(
            klights_cluster_datastore::diagnostics::SlowWatchReplayDecode {
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

    #[cfg(test)]
    pub fn subscribe_watch(
        &self,
        topic: klights_watch::WatchTopic,
    ) -> broadcast::Receiver<WatchEvent> {
        crate::watch_commit_observation_adapter::subscribe_test_events(
            self.commit_sink.as_ref(),
            topic,
        )
    }

    #[cfg(test)]
    pub fn subscribe_watch_many(&self, topics: Vec<klights_watch::WatchTopic>) -> WatchReceiver {
        crate::watch_commit_observation_adapter::subscribe_test_events_many(
            self.commit_sink.as_ref(),
            topics,
        )
    }

    #[cfg(test)]
    pub fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        crate::watch_commit_observation_adapter::subscribe_from_sink(
            self.commit_sink.as_ref(),
            topic,
        )
    }

    /// Broadcast a watch event after the DB transaction has committed.
    /// Delegates to the free function `publish_pending` so the broadcast
    /// path is identical whether called from CRUD methods or a future
    /// Raft FSM apply hook.
    #[cfg(test)]
    pub fn publish_watch_event(&self, pending: StagedPostCommit) {
        publish_pending(pending, self.commit_sink.as_ref());
    }

    /// Broadcast a batch of watch events after the DB transaction has
    /// committed. Multi-event apply paths (raft/cluster replace) use this so
    /// the post-commit signals are grouped per `(topic, namespace)` through
    /// `publish_pending_batch` instead of emitting one signal per event.
    #[cfg(test)]
    pub fn publish_watch_events(&self, pending: impl IntoIterator<Item = StagedPostCommit>) {
        publish_pending_batch(pending, self.commit_sink.as_ref());
    }

    #[cfg(test)]
    pub fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        self.publish_watch_event(pending);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_supervisor::{DbExecutor, TaskCategoryConfig, TaskSupervisor};

    use super::*;

    async fn open_in_memory(connection_key: &'static str) -> DbExecutor {
        klights_cluster_datastore::sqlite::open_in_memory(
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            connection_key,
        )
        .await
        .expect("open in-memory cluster datastore")
    }

    #[tokio::test]
    async fn broadcast_watch_event_sends_to_subscribers() {
        let executor = open_in_memory("sqlite:memory:broadcast-test").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "Pod"));

        let pending = create_staged_post_commit(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            99,
            "ADDED",
            serde_json::json!({}),
        );
        ds.broadcast_watch_event(pending);

        let event = watch_rx.try_recv().expect("should receive broadcast event");
        assert_eq!(event.event_type, crate::watch::EventType::Added);
    }

    #[tokio::test]
    async fn resource_create_broadcasts_after_commit() {
        let executor = open_in_memory("sqlite:memory:create-broadcast-test").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "ConfigMap"));

        let _resource = ds
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "test-cm",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "test-cm", "namespace": "default"}
                }),
            )
            .await
            .unwrap();

        let event = watch_rx.try_recv().expect("should receive broadcast event");
        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(event.object["metadata"]["name"].as_str(), Some("test-cm"));
    }

    #[tokio::test]
    async fn resource_update_broadcasts_after_commit() {
        let executor = open_in_memory("sqlite:memory:update-broadcast-test").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "ConfigMap"));

        let created = ds
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "test-cm",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "test-cm", "namespace": "default"},
                    "data": {"key": "v1"}
                }),
            )
            .await
            .unwrap();
        let _ = watch_rx.try_recv(); // drain create event

        let _updated = ds
            .update_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "test-cm",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "test-cm", "namespace": "default"},
                    "data": {"key": "v2"}
                }),
                created.resource_version,
            )
            .await
            .unwrap();

        let event = watch_rx
            .try_recv()
            .expect("should receive update broadcast");
        assert_eq!(event.event_type, crate::watch::EventType::Modified);
    }

    #[tokio::test]
    async fn resource_delete_broadcasts_after_commit() {
        let executor = open_in_memory("sqlite:memory:delete-broadcast-test").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "ConfigMap"));

        ds.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "test-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "test-cm", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
        let _ = watch_rx.try_recv(); // drain create event

        ds.delete_resource("v1", "ConfigMap", Some("default"), "test-cm")
            .await
            .unwrap();

        let event = watch_rx
            .try_recv()
            .expect("should receive delete broadcast");
        assert_eq!(event.event_type, crate::watch::EventType::Deleted);
        assert_eq!(event.object["metadata"]["name"].as_str(), Some("test-cm"));
    }

    #[tokio::test]
    async fn status_update_broadcasts_after_commit() {
        let executor = open_in_memory("sqlite:memory:status-broadcast-test").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "Pod"));

        let created = ds
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "test-pod",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "test-pod", "namespace": "default"},
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();
        let _ = watch_rx.try_recv(); // drain create event

        let _updated = ds
            .update_status_only(
                "v1",
                "Pod",
                Some("default"),
                "test-pod",
                serde_json::json!({"phase": "Running"}),
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let event = watch_rx
            .try_recv()
            .expect("should receive status update broadcast");
        assert_eq!(event.event_type, crate::watch::EventType::Modified);
    }

    // -----------------------------------------------------------------------
    // DSB-04 audit and broadcast-mode tests
    // -----------------------------------------------------------------------

    /// DSB-04: persistent_create_emits_one_watch_event — proves exactly
    /// one event reaches a subscriber for a create operation.
    #[tokio::test]
    async fn persistent_create_emits_one_watch_event() {
        let executor = open_in_memory("sqlite:memory:dsb04-one-event").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "ConfigMap"));

        ds.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "single-event-cm",
            serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "single-event-cm"}}),
        )
        .await
        .unwrap();

        // Should get exactly one event
        let event = watch_rx
            .try_recv()
            .expect("should receive exactly one event");
        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(
            event.object["metadata"]["name"].as_str(),
            Some("single-event-cm")
        );

        // No second event
        assert!(
            watch_rx.try_recv().is_err(),
            "should not receive a second event for a single create"
        );
    }

    /// DSB-04: verifies the broadcast mode is PostCommitOnly.
    #[test]
    fn broadcast_mode_is_post_commit_only() {
        use crate::datastore::backend::WatchBroadcastMode;
        use crate::datastore::sqlite::watch_mode::current_broadcast_mode;
        assert_eq!(current_broadcast_mode(), WatchBroadcastMode::PostCommitOnly);
    }

    #[tokio::test]
    async fn broadcast_watch_event_routes_only_to_subscribed_topic() {
        let executor = open_in_memory("sqlite:memory:watch-bus-topic-routing").await;
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut pod_rx = ds.subscribe_watch(WatchTopic::new("v1", "Pod"));

        ds.broadcast_watch_event(create_staged_post_commit(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-1",
            1,
            "ADDED",
            serde_json::json!({"metadata": {"name": "cm-1", "namespace": "default"}}),
        ));
        assert!(
            matches!(
                pod_rx.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "Pod topic subscribers must not wake for ConfigMap events"
        );

        ds.broadcast_watch_event(create_staged_post_commit(
            "v1",
            "Pod",
            Some("default"),
            "pod-1",
            2,
            "ADDED",
            serde_json::json!({"metadata": {"name": "pod-1", "namespace": "default"}}),
        ));
        let event = pod_rx
            .try_recv()
            .expect("Pod topic subscriber must receive Pod events");
        assert_eq!(
            event.object.get("kind").and_then(|kind| kind.as_str()),
            Some("Pod")
        );
    }

    #[tokio::test]
    async fn publish_pending_batch_sends_grouped_signal_for_same_topic() {
        use klights_watch::WatchAdvance;

        let ds = crate::datastore::test_support::in_memory().await;
        let topic = WatchTopic::new("v1", "Pod");
        let mut signals = ds.subscribe_watch_signals(topic);

        publish_pending_batch(
            vec![
                create_staged_post_commit(
                    "v1",
                    "Pod",
                    Some("default"),
                    "pod-a",
                    10,
                    "MODIFIED",
                    serde_json::json!({"metadata": {"labels": {"app": "a"}}}),
                ),
                create_staged_post_commit(
                    "v1",
                    "Pod",
                    Some("default"),
                    "pod-b",
                    12,
                    "MODIFIED",
                    serde_json::json!({"metadata": {"labels": {"app": "b"}}}),
                ),
                create_staged_post_commit(
                    "v1",
                    "Pod",
                    Some("kube-system"),
                    "pod-c",
                    11,
                    "MODIFIED",
                    serde_json::json!({"metadata": {"labels": {"app": "c"}}}),
                ),
            ],
            ds.commit_sink.as_ref(),
        );

        let signal = signals.try_recv().expect("grouped signal");
        assert_eq!(signal.advances.len(), 2);
        assert!(signal.advances.contains(&WatchAdvance {
            namespace: Some("default".to_string()),
            low_rv: 10,
            high_rv: 12,
        }));
        assert!(signal.advances.contains(&WatchAdvance {
            namespace: Some("kube-system".to_string()),
            low_rv: 11,
            high_rv: 11,
        }));
    }
}
