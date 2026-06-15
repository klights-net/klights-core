use anyhow::Result;
use serde_json::Value;

use crate::datastore::DatastoreBackend;
#[cfg(test)]
use crate::datastore::backend_kind::BackendKind;
use crate::datastore::command::StorageCommand;
#[cfg(test)]
use crate::datastore::node_local::selector;
use crate::kubelet::outbox::payload::OutboxOperation;
#[cfg(test)]
use crate::kubelet::outbox::payload::OutboxPayload;
use crate::kubelet::outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSubject};
#[cfg(test)]
use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn non_persisted_event(reason: &str, message: &str, event_type: &str) -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "reason": reason,
        "message": message,
        "type": event_type
    })
}

pub struct PodEventRecord<'a> {
    pub pod: &'a Value,
    pub reason: &'a str,
    pub message: &'a str,
    pub event_type: &'a str,
    pub reporting_component: &'a str,
    pub reporting_instance: &'a str,
}

/// Create and store a K8s Event object for a pod lifecycle event.
/// Returns the created Event as a JSON Value.
#[cfg(test)]
pub async fn emit_pod_event(
    ds: &dyn DatastoreBackend,
    pod: &Value,
    reason: &str,
    message: &str,
    event_type: &str,
    reporting_component: &str,
    reporting_instance: &str,
) -> Result<Value> {
    let node_db = test_node_db().await?;
    let outbox = Outbox::new(node_db.clone());
    let event = emit_pod_event_impl(
        ds,
        Some(&outbox),
        PodEventRecord {
            pod,
            reason,
            message,
            event_type,
            reporting_component,
            reporting_instance,
        },
    )
    .await?;
    flush_single_outbox_command(ds, &node_db, "emit_pod_event").await?;
    Ok(event)
}

#[cfg(test)]
async fn test_node_db() -> Result<crate::datastore::node_local::NodeLocalHandle> {
    selector::open_node_local(
        BackendKind::Sqlite,
        None,
        std::sync::Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        None,
        "sqlite:test-emit-pod-event",
    )
    .await
}

#[cfg(test)]
async fn flush_single_outbox_command(
    ds: &dyn DatastoreBackend,
    node_db: &crate::datastore::node_local::NodeLocalHandle,
    lease_token: &str,
) -> Result<()> {
    let Some(row) = node_db
        .claim_next_due_outbox(epoch_ms(), 1_000, lease_token)
        .await?
    else {
        return Ok(());
    };

    let command = OutboxPayload::decode_protobuf(&row.payload_proto)?;
    let result = match command.command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            ..
        } => ds
            .create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
            .await
            .map(|_| ()),
        other => {
            anyhow::bail!(
                "unsupported outbox command in test emit_pod_event path: {:?}",
                other
            );
        }
    };
    result?;
    node_db
        .complete_outbox(row.id, lease_token)
        .await
        .map(|_| ())
}

pub async fn emit_pod_event_with_outbox(
    ds: &dyn DatastoreBackend,
    outbox: Option<&Outbox>,
    record: PodEventRecord<'_>,
) -> Result<Value> {
    emit_pod_event_impl(ds, outbox, record).await
}

/// Outcome of the namespace preflight before emitting a pod event.
#[derive(Debug, PartialEq, Eq)]
enum NamespacePreflight {
    /// Namespace is present (or the check could not be performed) — emit the
    /// event.
    Proceed,
    /// Namespace is definitively missing or terminating — suppress the event.
    SkipTerminating,
}

/// Classify the namespace preflight result. A definitive `Forbidden` (missing or
/// terminating namespace) suppresses the event; ANY other error fails OPEN and
/// proceeds. Failing open matters on workers: the preflight reads namespace state
/// through a fresh leader RPC, so a transient leader blip / connection drop would
/// otherwise silently drop the event BEFORE it is durably enqueued. The leader
/// re-validates the namespace when it applies the EventCreate outbox entry, so
/// proceeding is safe and strictly better than dropping.
fn classify_namespace_preflight(result: Result<(), crate::api::AppError>) -> NamespacePreflight {
    match result {
        Ok(()) => NamespacePreflight::Proceed,
        Err(crate::api::AppError::Forbidden(_)) => NamespacePreflight::SkipTerminating,
        Err(_) => NamespacePreflight::Proceed,
    }
}

async fn emit_pod_event_impl(
    ds: &dyn DatastoreBackend,
    outbox: Option<&Outbox>,
    record: PodEventRecord<'_>,
) -> Result<Value> {
    let PodEventRecord {
        pod,
        reason,
        message,
        event_type,
        reporting_component,
        reporting_instance,
    } = record;
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.name"))?;

    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.namespace"))?;

    let pod_uid = pod
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.uid"))?;

    let preflight = crate::api::reject_if_namespace_missing_or_terminating(ds, namespace).await;
    if let Err(err) = &preflight
        && !matches!(err, crate::api::AppError::Forbidden(_))
    {
        // Fail open: do not drop the event on a transport/DB error. The leader
        // re-validates the namespace when it applies the EventCreate.
        tracing::warn!(
            namespace = %namespace,
            pod = %pod_name,
            "namespace preflight failed (transport/db error); emitting event anyway: {:?}",
            err
        );
    }
    match classify_namespace_preflight(preflight) {
        NamespacePreflight::Proceed => {}
        NamespacePreflight::SkipTerminating => {
            tracing::debug!(
                namespace = %namespace,
                pod = %pod_name,
                reason = %reason,
                "skipping pod event in terminating namespace"
            );
            return Ok(non_persisted_event(reason, message, event_type));
        }
    }

    // Generate unique event name: <pod-name>.<random-suffix>
    // Use first 8 chars of UUID (hex format)
    let random_suffix = uuid::Uuid::new_v4().simple().to_string();
    let random_suffix = &random_suffix[0..8];
    let event_name = format!("{}.{}", pod_name, random_suffix);

    let now = crate::utils::k8s_timestamp();

    // Conformance stability: kubelet may re-enter create/reconcile paths for the
    // same pod while assignment is unchanged. Avoid unbounded duplicate Scheduled
    // events for the same pod+message+source tuple.
    if reason == "Scheduled" {
        let existing = ds
            .list_resources(
                "v1",
                "Event",
                Some(namespace),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let duplicate = existing.items.iter().any(|res| {
            let data = &res.data;
            data.pointer("/involvedObject/uid")
                .and_then(|v| v.as_str())
                .is_some_and(|uid| uid == pod_uid)
                && data
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .is_some_and(|r| r == reason)
                && data
                    .get("message")
                    .and_then(|v| v.as_str())
                    .is_some_and(|m| m == message)
                && data
                    .pointer("/source/component")
                    .and_then(|v| v.as_str())
                    .is_some_and(|c| c == reporting_component)
                && data
                    .pointer("/source/host")
                    .and_then(|v| v.as_str())
                    .is_some_and(|h| h == reporting_instance)
        });
        if duplicate {
            return Ok(non_persisted_event(reason, message, event_type));
        }
    }

    let event = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": event_name,
            "namespace": namespace,
            "creationTimestamp": now
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": pod_name,
            "namespace": namespace,
            "uid": pod_uid
        },
        "reason": reason,
        "message": message,
        "type": event_type,
        "source": {
            "component": reporting_component,
            "host": reporting_instance
        },
        "firstTimestamp": now,
        "lastTimestamp": now,
        "count": 1
    });

    let subject_key = format!("v1/Event/{namespace}/{event_name}");
    OutboxSendPlanner::new(outbox)
        .route(OutboxCommand {
            idempotency_key: format!("EventCreate:{subject_key}:{}", uuid::Uuid::new_v4()),
            operation: OutboxOperation::EventCreate,
            subject: OutboxSubject {
                key: subject_key,
                namespace: Some(namespace.to_string()),
                name: event_name.clone(),
                uid: None,
            },
            pod_uid: pod_uid.to_string(),
            command: StorageCommand::CreateResource {
                api_version: "v1".to_string(),
                kind: "Event".to_string(),
                namespace: Some(namespace.to_string()),
                name: event_name.clone(),
                data: event.clone(),
            },
            now_ms: epoch_ms(),
        })
        .await?;
    Ok(event)
}

fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_preflight_fails_open_on_transport_error() {
        // Present namespace -> emit.
        assert_eq!(
            classify_namespace_preflight(Ok(())),
            NamespacePreflight::Proceed
        );
        // Definitive missing/terminating -> suppress.
        assert_eq!(
            classify_namespace_preflight(Err(crate::api::AppError::Forbidden(
                "namespace foo is being terminated".into()
            ))),
            NamespacePreflight::SkipTerminating
        );
        // Transport / DB error must FAIL OPEN (proceed) so a leader blip never
        // silently drops a pod event before it is durably enqueued.
        assert_eq!(
            classify_namespace_preflight(Err(crate::api::AppError::Internal(
                "connection reset by peer".into()
            ))),
            NamespacePreflight::Proceed
        );
        assert_eq!(
            classify_namespace_preflight(Err(crate::api::AppError::ServiceUnavailable(
                "leader not ready".into()
            ))),
            NamespacePreflight::Proceed
        );
    }

    fn create_test_pod() -> Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "namespace": "default",
                "uid": "test-uid-12345"
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "image": "nginx:latest"
                }]
            }
        })
    }

    #[tokio::test]
    async fn test_emit_pod_event_creates_valid_event_json() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        let event = emit_pod_event(
            &ds,
            &pod,
            "Scheduled",
            "Successfully assigned default/test-pod to node1",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        // Verify basic structure
        assert_eq!(event.get("apiVersion").and_then(|v| v.as_str()), Some("v1"));
        assert_eq!(event.get("kind").and_then(|v| v.as_str()), Some("Event"));

        // Verify metadata
        let metadata = event.get("metadata").expect("Event missing metadata");
        assert!(metadata.get("name").and_then(|v| v.as_str()).is_some());
        assert_eq!(
            metadata.get("namespace").and_then(|v| v.as_str()),
            Some("default")
        );
        assert!(
            metadata
                .get("creationTimestamp")
                .and_then(|v| v.as_str())
                .is_some()
        );

        // Verify reason, message, type
        assert_eq!(
            event.get("reason").and_then(|v| v.as_str()),
            Some("Scheduled")
        );
        assert_eq!(
            event.get("message").and_then(|v| v.as_str()),
            Some("Successfully assigned default/test-pod to node1")
        );
        assert_eq!(event.get("type").and_then(|v| v.as_str()), Some("Normal"));

        // Verify source
        let source = event.get("source").expect("Event missing source");
        assert_eq!(
            source.get("component").and_then(|v| v.as_str()),
            Some("klights-kubelet")
        );
        assert_eq!(source.get("host").and_then(|v| v.as_str()), Some("node1"));

        // Verify timestamps
        assert!(
            event
                .get("firstTimestamp")
                .and_then(|v| v.as_str())
                .is_some()
        );
        assert!(
            event
                .get("lastTimestamp")
                .and_then(|v| v.as_str())
                .is_some()
        );

        // Verify count
        assert_eq!(event.get("count").and_then(|v| v.as_i64()), Some(1));
    }

    #[tokio::test]
    async fn test_emit_pod_event_uses_correct_involved_object() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        let event = emit_pod_event(
            &ds,
            &pod,
            "Started",
            "Started container nginx",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        let involved_object = event
            .get("involvedObject")
            .expect("Event missing involvedObject");
        assert_eq!(
            involved_object.get("apiVersion").and_then(|v| v.as_str()),
            Some("v1")
        );
        assert_eq!(
            involved_object.get("kind").and_then(|v| v.as_str()),
            Some("Pod")
        );
        assert_eq!(
            involved_object.get("name").and_then(|v| v.as_str()),
            Some("test-pod")
        );
        assert_eq!(
            involved_object.get("namespace").and_then(|v| v.as_str()),
            Some("default")
        );
        assert_eq!(
            involved_object.get("uid").and_then(|v| v.as_str()),
            Some("test-uid-12345")
        );
    }

    #[tokio::test]
    async fn test_emit_pod_event_normal_type() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        let event = emit_pod_event(
            &ds,
            &pod,
            "Pulled",
            "Successfully pulled image \"nginx:latest\"",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        assert_eq!(event.get("type").and_then(|v| v.as_str()), Some("Normal"));
    }

    #[tokio::test]
    async fn test_emit_pod_event_warning_type() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        let event = emit_pod_event(
            &ds,
            &pod,
            "Failed",
            "Error: failed to pull image",
            "Warning",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        assert_eq!(event.get("type").and_then(|v| v.as_str()), Some("Warning"));
    }

    #[tokio::test]
    async fn test_emit_pod_event_unique_names() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        let event1 = emit_pod_event(
            &ds,
            &pod,
            "Pulling",
            "Pulling image",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        let event2 = emit_pod_event(
            &ds,
            &pod,
            "Pulled",
            "Pulled image",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        let name1 = event1
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .expect("Event 1 missing name");
        let name2 = event2
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .expect("Event 2 missing name");

        assert_ne!(name1, name2, "Event names should be unique");
    }

    #[tokio::test]
    async fn test_emit_pod_event_dedupes_scheduled_for_same_pod_message_and_source() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        emit_pod_event(
            &ds,
            &pod,
            "Scheduled",
            "Successfully assigned default/test-pod to node1",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        emit_pod_event(
            &ds,
            &pod,
            "Scheduled",
            "Successfully assigned default/test-pod to node1",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        let events = ds
            .list_resources(
                "v1",
                "Event",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let scheduled_count = events
            .items
            .iter()
            .filter(|e| e.data.get("reason").and_then(|v| v.as_str()) == Some("Scheduled"))
            .count();
        assert_eq!(scheduled_count, 1);
    }

    #[tokio::test]
    async fn test_emit_pod_event_does_not_dedupe_non_scheduled_events() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        emit_pod_event(
            &ds,
            &pod,
            "Pulling",
            "Pulling image \"nginx:latest\"",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        emit_pod_event(
            &ds,
            &pod,
            "Pulling",
            "Pulling image \"nginx:latest\"",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        let events = ds
            .list_resources(
                "v1",
                "Event",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let pulling_count = events
            .items
            .iter()
            .filter(|e| e.data.get("reason").and_then(|v| v.as_str()) == Some("Pulling"))
            .count();
        assert_eq!(pulling_count, 2);
    }

    #[tokio::test]
    async fn test_emit_pod_event_skips_persisting_in_terminating_namespace() {
        let ds = crate::datastore::test_support::in_memory().await;
        ds.create_namespace(
            "terminating-events",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-events",
                    "uid": "terminating-events-uid",
                    "deletionTimestamp": crate::utils::k8s_timestamp()
                },
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .unwrap();

        let mut pod = create_test_pod();
        pod["metadata"]["namespace"] = serde_json::json!("terminating-events");

        let event = emit_pod_event(
            &ds,
            &pod,
            "Scheduled",
            "Successfully assigned terminating-events/test-pod to node1",
            "Normal",
            "klights-kubelet",
            "node1",
        )
        .await
        .unwrap();

        assert_eq!(
            event.get("reason").and_then(|v| v.as_str()),
            Some("Scheduled")
        );
        let events = ds
            .list_resources(
                "v1",
                "Event",
                Some("terminating-events"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            events.items.is_empty(),
            "internal kubelet events must not recreate content in a terminating namespace"
        );
    }

    #[tokio::test]
    async fn test_emit_pod_event_with_outbox_none_is_rejected() {
        let ds = crate::datastore::test_support::in_memory().await;
        let pod = create_test_pod();

        emit_pod_event_with_outbox(
            &ds,
            None,
            PodEventRecord {
                pod: &pod,
                reason: "Started",
                message: "Started container app",
                event_type: "Normal",
                reporting_component: "klights-kubelet",
                reporting_instance: "node-a",
            },
        )
        .await
        .expect_err("event should reject when outbox is unavailable");

        let events = ds
            .list_resources(
                "v1",
                "Event",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert!(
            events.items.is_empty(),
            "event should not be persisted without outbox"
        );
    }
}
