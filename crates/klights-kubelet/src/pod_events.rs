use anyhow::Result;
use serde_json::Value;

use crate::node_outbox::payload::OutboxOperation;
use crate::node_outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSubject};
use klights_cluster_core::StorageCommand;

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
    pub operation_now: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodEventNamespaceEligibility {
    Allowed,
    Missing,
    Terminating,
}

/// Focused cluster reads needed by Pod event production.
#[async_trait::async_trait]
pub trait PodEventQuery: Send + Sync {
    async fn namespace_eligibility(
        &self,
        namespace: &str,
    ) -> anyhow::Result<PodEventNamespaceEligibility>;

    async fn list_events(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>>;
}

/// Focused leader-side persistence effect for control-plane-authored Events.
#[async_trait::async_trait]
pub trait PodEventEffect: Send + Sync {
    async fn create_event(&self, namespace: &str, name: &str, event: Value) -> anyhow::Result<()>;
}

pub async fn emit_pod_event_with_outbox<Q>(
    query: &Q,
    outbox: Option<&Outbox>,
    record: PodEventRecord<'_>,
) -> Result<Value>
where
    Q: PodEventQuery + ?Sized,
{
    emit_pod_event_impl::<Q, dyn PodEventEffect>(
        query,
        PodEventPersistence::NodeOutbox(outbox),
        record,
    )
    .await
}

pub async fn emit_worker_pod_event(
    query: &dyn PodEventQuery,
    outbox: &Outbox,
    record: PodEventRecord<'_>,
) -> Result<Value> {
    emit_pod_event_impl::<dyn PodEventQuery, dyn PodEventEffect>(
        query,
        PodEventPersistence::NodeOutbox(Some(outbox)),
        record,
    )
    .await
}

/// Persist an Event authored by a leader-owned control-plane component.
///
/// Control-plane Events must not be sent through the node-authenticated outbox:
/// their reporting instance is the controller rather than a kubelet node, so
/// worker Event authorization correctly rejects them. The supplied datastore is
/// the leader-owned cluster port and preserves Raft proposal/apply semantics.
pub async fn emit_control_plane_pod_event<Q, E>(
    query: &Q,
    effect: &E,
    record: PodEventRecord<'_>,
) -> Result<Value>
where
    Q: PodEventQuery + ?Sized,
    E: PodEventEffect + ?Sized,
{
    emit_pod_event_impl(query, PodEventPersistence::LeaderEffect(effect), record).await
}

enum PodEventPersistence<'a, E: PodEventEffect + ?Sized> {
    NodeOutbox(Option<&'a Outbox>),
    LeaderEffect(&'a E),
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
fn classify_namespace_preflight(
    result: anyhow::Result<PodEventNamespaceEligibility>,
) -> NamespacePreflight {
    match result {
        Ok(PodEventNamespaceEligibility::Allowed) => NamespacePreflight::Proceed,
        Ok(PodEventNamespaceEligibility::Missing | PodEventNamespaceEligibility::Terminating) => {
            NamespacePreflight::SkipTerminating
        }
        Err(_) => NamespacePreflight::Proceed,
    }
}

async fn emit_pod_event_impl<Q, E>(
    query: &Q,
    persistence: PodEventPersistence<'_, E>,
    record: PodEventRecord<'_>,
) -> Result<Value>
where
    Q: PodEventQuery + ?Sized,
    E: PodEventEffect + ?Sized,
{
    let PodEventRecord {
        pod,
        reason,
        message,
        event_type,
        reporting_component,
        reporting_instance,
        operation_now,
    } = record;
    let operation_now_ms = operation_now.timestamp_millis();
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

    let preflight = query.namespace_eligibility(namespace).await;
    if let Err(err) = &preflight {
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

    let now = klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now);

    // Conformance stability: kubelet may re-enter create/reconcile paths for the
    // same pod while assignment is unchanged. Avoid unbounded duplicate Scheduled
    // events for the same pod+message+source tuple.
    if reason == "Scheduled" {
        let existing = query.list_events(namespace).await?;
        let duplicate = existing.iter().any(|res| {
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
    match persistence {
        PodEventPersistence::NodeOutbox(outbox) => {
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
                    now_ms: operation_now_ms,
                })
                .await?;
        }
        PodEventPersistence::LeaderEffect(effect) => {
            effect
                .create_event(namespace, &event_name, event.clone())
                .await?;
        }
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_preflight_preserves_fail_open_behavior() {
        assert_eq!(
            classify_namespace_preflight(Ok(PodEventNamespaceEligibility::Allowed)),
            NamespacePreflight::Proceed
        );
        assert_eq!(
            classify_namespace_preflight(Ok(PodEventNamespaceEligibility::Terminating)),
            NamespacePreflight::SkipTerminating
        );
        assert_eq!(
            classify_namespace_preflight(Err(anyhow::anyhow!("connection reset by peer"))),
            NamespacePreflight::Proceed
        );
    }
}
