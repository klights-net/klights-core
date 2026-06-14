use serde_json::Value;
use tokio::sync::broadcast;

use crate::watch::{WatchContentType, WatchReceiver, WatchTopic, encode_watch_payload};

use super::{
    CatchUpResource, Datastore, PendingWatchEvent, PodEndpointEvent, PodSlotAdmissionEvent,
    Resource, WatchEvent, hydrate_watch_event_data,
};

/// Free function to publish a pending watch event after DB commit.
///
/// This is the single entry point for post-commit watch broadcast.
/// Callable from:
/// - Request handler CRUD paths (Phase 1/2 SingleNode and Leader)
/// - Future Raft FSM apply hook (Phase 3 — every node, leader and
///   follower alike)
///
/// Per HA contract bullet #4, request handlers must never call the
/// watch bus directly — they hand a `PendingWatchEvent` back
/// and this function publishes it. tests/source_guard_tests.py enforces this.
pub fn publish_pending(pending: PendingWatchEvent, bus: &crate::watch::WatchBus) {
    let event = pending.event;
    crate::datastore::diagnostics::log_watch_event_broadcast(&event);
    bus.publish(event);
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

/// Create a PendingWatchEvent from raw parameters.
///
/// Used by crud operations to stage a watch event inside the transaction,
/// then broadcast after commit via `Datastore::broadcast_watch_event`.
pub fn create_pending_watch_event(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    event_type: &str,
    data: impl Into<std::sync::Arc<Value>>,
) -> PendingWatchEvent {
    let data = std::sync::Arc::unwrap_or_clone(data.into());
    let mut event = WatchEvent::from_type(
        event_type,
        hydrate_watch_event_data(data, api_version, kind, namespace, name, resource_version),
    );
    event.encoded_payload = encode_watch_payload(&event, WatchContentType::Json).ok();
    PendingWatchEvent { event }
}

impl Datastore {
    pub(super) fn watch_row_to_catchup_resource(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<CatchUpResource> {
        let data_bytes: Vec<u8> = row.get(6)?;
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
        Ok(CatchUpResource {
            resource,
            event_type: catchup_event_type_from_db(event_type),
        })
    }

    pub fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    pub fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver {
        self.watch_bus.subscribe_many(topics)
    }

    /// Broadcast a watch event after the DB transaction has committed.
    /// Delegates to the free function `publish_pending` so the broadcast
    /// path is identical whether called from CRUD methods or a future
    /// Raft FSM apply hook.
    pub fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        publish_pending(pending, &self.watch_bus);
    }

    /// Subscribe to internal `pod_endpoints` table events. Used by Phase 2
    /// reconcilers; root-only Phase 1 has no consumers but the channel is
    /// always present so subscriber wiring is uniform across modes.
    pub fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.pod_endpoint_tx.subscribe()
    }

    pub fn pod_endpoint_sender(&self) -> broadcast::Sender<PodEndpointEvent> {
        self.pod_endpoint_tx.clone()
    }

    pub fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.pod_slot_admission_tx.subscribe()
    }

    pub fn pod_slot_admission_sender(&self) -> broadcast::Sender<PodSlotAdmissionEvent> {
        self.pod_slot_admission_tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn broadcast_watch_event_sends_to_subscribers() {
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:broadcast-test",
            )
            .await
            .unwrap();
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut watch_rx = ds.subscribe_watch(WatchTopic::new("v1", "Pod"));

        let pending = create_pending_watch_event(
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:create-broadcast-test",
            )
            .await
            .unwrap();
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:update-broadcast-test",
            )
            .await
            .unwrap();
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:delete-broadcast-test",
            )
            .await
            .unwrap();
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:status-broadcast-test",
            )
            .await
            .unwrap();
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:dsb04-one-event",
            )
            .await
            .unwrap();
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
        let executor =
            crate::datastore::sqlite::DbExecutor::open_in_memory_with_default_supervisor(
                "sqlite:memory:watch-bus-topic-routing",
            )
            .await
            .unwrap();
        let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
            .await
            .unwrap();
        let mut pod_rx = ds.subscribe_watch(WatchTopic::new("v1", "Pod"));

        ds.broadcast_watch_event(create_pending_watch_event(
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

        ds.broadcast_watch_event(create_pending_watch_event(
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
}
