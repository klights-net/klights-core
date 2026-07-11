use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::RwLock;

use crate::datastore::DatastoreBackend;
use crate::datastore::command::StorageCommand;
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
use crate::replication::protocol::ForwardedResource;

#[derive(Clone)]
pub struct N1Raft {
    backend: Arc<dyn DatastoreBackend>,
    last_commit_index: Arc<RwLock<i64>>,
}

impl N1Raft {
    pub fn new(backend: Arc<dyn DatastoreBackend>) -> Self {
        Self {
            backend,
            last_commit_index: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn last_commit_index(&self) -> i64 {
        *self.last_commit_index.read().await
    }

    pub async fn propose_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
        authoring_node: &str,
    ) -> std::result::Result<RaftOutboxApply, OutboxApplyError> {
        self.propose_outbox_with_watermark(
            idempotency_key,
            operation,
            payload,
            authoring_node,
            None,
        )
        .await
    }

    pub async fn propose_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<RaftOutboxApply, OutboxApplyError> {
        let applied = propose_outbox_on_backend_with_watermark(
            self.backend.as_ref(),
            idempotency_key,
            operation,
            payload,
            authoring_node,
            watermark,
        )
        .await?;
        if let Some(applied_rv) = applied.applied_resource_version() {
            *self.last_commit_index.write().await = applied_rv;
        }
        Ok(applied)
    }
}

pub async fn propose_outbox_on_backend(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
) -> std::result::Result<RaftOutboxApply, OutboxApplyError> {
    propose_outbox_on_backend_with_watermark(
        db,
        idempotency_key,
        operation,
        payload,
        authoring_node,
        None,
    )
    .await
}

pub async fn propose_outbox_on_backend_with_watermark(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
    watermark: Option<crate::log_apply::OutboxStreamWatermark>,
) -> std::result::Result<RaftOutboxApply, OutboxApplyError> {
    let decoded = OutboxPayload::decode_protobuf(&payload)
        .map_err(|err| OutboxApplyError::Retryable(err.to_string()))?;
    if operation == OutboxOperation::LeaseRenew {
        crate::node_lease_tracker::ensure_lease_renew_command(&decoded.command, authoring_node)
            .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
        return Ok(RaftOutboxApply {
            result: OutboxApplyResult::Applied { applied_rv: 0 },
            resource: None,
            command: None,
        });
    }
    if watermark.is_none() {
        crate::control_plane::client::apply::reject_pod_uid_mismatch(db, &decoded.command).await?;
    }

    let watermark_present = watermark.is_some();
    let uid_bound_pod_target = is_uid_bound_pod_command(&decoded.command);
    let deleted_resource = resource_before_delete(db, &decoded.command).await?;
    let result = match db
        .apply_outbox_transactionally_with_watermark(
            idempotency_key,
            operation.as_str(),
            payload.as_ref(),
            authoring_node,
            watermark,
        )
        .await
    {
        Ok(result) => result,
        Err(err) => {
            let classified = match err {
                OutboxApplyError::Retryable(_) => {
                    crate::control_plane::client::apply::classify_apply_error_for_command(
                        &decoded.command,
                        err,
                    )
                }
                other => other,
            };
            // T1: Non-leader voters now stay in sync via the shared
            // log_apply follower path (same code replicas use). Raft
            // state-machine apply errors are surfaced directly — no
            // more silently skipping or tolerating conflicts. The
            // log_apply follower guarantees proper ordering so these
            // errors don't occur in steady state.
            return Err(classified);
        }
    };

    // T1: All apply results are now propagated (errors surface,
    // AlreadyApplied returns the stored resource). The log_apply
    // follower ensures proper state ordering so these no longer
    // need to be silently swallowed.
    let resource = resource_after_apply(db, &decoded.command, deleted_resource).await?;
    let command = if watermark_present && uid_bound_pod_target && resource.is_none() {
        None
    } else {
        Some(decoded.command)
    };
    Ok(RaftOutboxApply {
        result,
        resource,
        command,
    })
}

fn is_uid_bound_pod_command(command: &StorageCommand) -> bool {
    let (api_version, kind, preconditions) = match command {
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            preconditions,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            preconditions,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            preconditions,
            ..
        } => (api_version, kind, preconditions),
        _ => return false,
    };
    api_version == "v1"
        && kind == "Pod"
        && preconditions
            .uid
            .as_deref()
            .is_some_and(|uid| !uid.is_empty())
}

pub struct RaftOutboxApply {
    pub result: OutboxApplyResult,
    pub resource: Option<ForwardedResource>,
    pub command: Option<StorageCommand>,
}

impl RaftOutboxApply {
    pub fn applied_resource_version(&self) -> Option<i64> {
        match &self.result {
            OutboxApplyResult::Applied { applied_rv } => Some(*applied_rv),
            OutboxApplyResult::AlreadyApplied { applied_rv } => *applied_rv,
        }
    }
}

async fn resource_before_delete(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
) -> std::result::Result<Option<ForwardedResource>, OutboxApplyError> {
    let StorageCommand::DeleteResource {
        api_version,
        kind,
        namespace,
        name,
        ..
    } = command
    else {
        if let StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            namespace,
            name,
            ..
        } = command
        {
            return db
                .get_resource(api_version, kind, namespace.as_deref(), name)
                .await
                .map(|resource| resource.map(ForwardedResource::from))
                .map_err(|err| OutboxApplyError::Retryable(err.to_string()));
        }
        return Ok(None);
    };
    db.get_resource(api_version, kind, namespace.as_deref(), name)
        .await
        .map(|resource| resource.map(ForwardedResource::from))
        .map_err(|err| OutboxApplyError::Retryable(err.to_string()))
}

async fn resource_after_apply(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
    deleted_resource: Option<ForwardedResource>,
) -> std::result::Result<Option<ForwardedResource>, OutboxApplyError> {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            ..
        } => db
            .get_resource(api_version, kind, namespace.as_deref(), name)
            .await
            .map(|resource| resource.map(ForwardedResource::from))
            .map_err(|err| OutboxApplyError::Retryable(err.to_string())),
        StorageCommand::DeleteResource { .. }
        | StorageCommand::DeleteResourceWithTombstone { .. } => Ok(deleted_resource),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::datastore::ResourcePreconditions;

    fn outbox_payload(command: StorageCommand) -> Bytes {
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode outbox payload"),
        )
    }

    #[tokio::test]
    async fn watermarked_stale_uid_bound_pod_row_advances_stream_without_side_effect_command() {
        let db = crate::datastore::test_support::in_memory().await;
        let watermark = crate::log_apply::OutboxStreamWatermark {
            client_id: "worker-client".to_string(),
            stream_id: 11,
            stream_seq: 1,
        };

        let result = propose_outbox_on_backend_with_watermark(
            &db,
            "missing-pod-status",
            OutboxOperation::PodStatus,
            outbox_payload(StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "already-gone".to_string(),
                status: json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("gone-uid".to_string()),
                    resource_version: None,
                },
                observed_status_stamp: Some(42),
            }),
            "worker-a",
            Some(watermark.clone()),
        )
        .await
        .expect("stale UID-bound Pod status must consume its outbox stream watermark");

        assert!(matches!(result.result, OutboxApplyResult::Applied { .. }));
        assert!(result.resource.is_none());
        assert!(
            result.command.is_none(),
            "watermark-only stale Pod consumption must not fire resource side effects"
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![watermark]
        );

        propose_outbox_on_backend_with_watermark(
            &db,
            "next-stream-entry",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::CreateNamespace {
                name: "after-stale-gap".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "after-stale-gap"}
                }),
            }),
            "worker-a",
            Some(crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 11,
                stream_seq: 2,
            }),
        )
        .await
        .expect("next stream entry must not wedge behind a stale Pod row gap");

        assert!(db.get_namespace("after-stale-gap").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn watermarked_stale_pod_metadata_duplicate_consumes_stream_without_overwriting_live_labels()
     {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "adopted-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "adopted-pod",
                        "uid": "pod-uid",
                        "labels": {"name": "matching-name"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "adopter",
                            "uid": "rs-uid",
                            "controller": true,
                            "blockOwnerDeletion": true
                        }]
                    },
                    "spec": {"nodeName": "worker-a"},
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .expect("seed adopted Pod");

        let mut relabeled = (*created.data).clone();
        relabeled["metadata"]["labels"] = json!({"name": "not-matching-name"});
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "adopted-pod",
            relabeled,
            created.resource_version,
        )
        .await
        .expect("relabel Pod before stale duplicate adoption reaches leader");

        let first_watermark = crate::log_apply::OutboxStreamWatermark {
            client_id: "worker-client".to_string(),
            stream_id: 84,
            stream_seq: 1,
        };
        let first = propose_outbox_on_backend_with_watermark(
            &db,
            "stale-duplicate-adoption",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::UpdateResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "adopted-pod".to_string(),
                data: (*created.data).clone(),
                expected_rv: created.resource_version,
                preconditions: ResourcePreconditions {
                    uid: Some("pod-uid".to_string()),
                    resource_version: Some(created.resource_version),
                },
            }),
            "worker-a",
            Some(first_watermark.clone()),
        )
        .await
        .expect("stale duplicate PodMetadata must consume its stream watermark");

        assert!(matches!(first.result, OutboxApplyResult::Applied { .. }));
        let live = db
            .get_resource("v1", "Pod", Some("default"), "adopted-pod")
            .await
            .unwrap()
            .expect("live Pod remains");
        assert_eq!(
            live.data.pointer("/metadata/labels/name"),
            Some(&json!("not-matching-name")),
            "stale duplicate metadata must not overwrite the live relabel"
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![first_watermark]
        );

        propose_outbox_on_backend_with_watermark(
            &db,
            "next-after-stale-duplicate-adoption",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::CreateNamespace {
                name: "after-stale-podmetadata".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "after-stale-podmetadata"}
                }),
            }),
            "worker-a",
            Some(crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 84,
                stream_seq: 2,
            }),
        )
        .await
        .expect("next stream entry must not wedge behind stale PodMetadata");

        assert!(
            db.get_namespace("after-stale-podmetadata")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn watermarked_stale_pod_metadata_release_clears_ownerrefs_without_overwriting_live_metadata()
     {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "release-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "release-pod",
                        "uid": "release-pod-uid",
                        "labels": {"name": "matching-name"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "adopter",
                            "uid": "rs-uid",
                            "controller": true,
                            "blockOwnerDeletion": true
                        }]
                    },
                    "spec": {"nodeName": "worker-a"},
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .expect("seed adopted Pod");

        let mut relabeled = (*created.data).clone();
        relabeled["metadata"]["labels"] = json!({"name": "not-matching-name"});
        let relabeled = db
            .update_resource(
                "v1",
                "Pod",
                Some("default"),
                "release-pod",
                relabeled,
                created.resource_version,
            )
            .await
            .expect("relabel adopted Pod");

        let mut live_with_annotation = (*relabeled.data).clone();
        live_with_annotation["metadata"]["annotations"] = json!({"live": "preserve"});
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "release-pod",
            live_with_annotation,
            relabeled.resource_version,
        )
        .await
        .expect("advance live metadata before release reaches leader");

        let mut release = (*relabeled.data).clone();
        release["metadata"]["ownerReferences"] = json!([]);
        propose_outbox_on_backend_with_watermark(
            &db,
            "stale-ownerref-release",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::UpdateResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "release-pod".to_string(),
                data: release,
                expected_rv: relabeled.resource_version,
                preconditions: ResourcePreconditions {
                    uid: Some("release-pod-uid".to_string()),
                    resource_version: Some(relabeled.resource_version),
                },
            }),
            "worker-a",
            Some(crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 85,
                stream_seq: 1,
            }),
        )
        .await
        .expect("stale ownerReferences release must apply against the live Pod");

        let live = db
            .get_resource("v1", "Pod", Some("default"), "release-pod")
            .await
            .unwrap()
            .expect("live Pod remains");
        assert_eq!(
            live.data.pointer("/metadata/ownerReferences"),
            Some(&json!([])),
            "release must clear ownerReferences"
        );
        assert_eq!(
            live.data.pointer("/metadata/labels/name"),
            Some(&json!("not-matching-name")),
            "release must preserve the relabel that made the Pod no longer match"
        );
        assert_eq!(
            live.data.pointer("/metadata/annotations/live"),
            Some(&json!("preserve")),
            "release must preserve newer live metadata"
        );
    }

    #[tokio::test]
    async fn watermarked_terminal_event_create_conflict_advances_stream() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_namespace(
            "event-conflict",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "event-conflict"}
            }),
        )
        .await
        .expect("create event namespace");
        db.create_resource(
            "events.k8s.io/v1",
            "Event",
            Some("event-conflict"),
            "duplicate-event",
            json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {
                    "namespace": "event-conflict",
                    "name": "duplicate-event",
                    "uid": "existing-event-uid"
                },
                "eventTime": "2026-07-07T00:00:00Z",
                "reportingController": "klights.test",
                "reportingInstance": "worker-a",
                "action": "Started",
                "reason": "AlreadyThere",
                "regarding": {"kind": "Pod", "namespace": "event-conflict", "name": "pod-a"}
            }),
        )
        .await
        .expect("seed duplicate Event");

        let first_watermark = crate::log_apply::OutboxStreamWatermark {
            client_id: "worker-client".to_string(),
            stream_id: 54,
            stream_seq: 1,
        };
        let first = propose_outbox_on_backend_with_watermark(
            &db,
            "duplicate-event-create",
            OutboxOperation::EventCreate,
            outbox_payload(StorageCommand::CreateResource {
                api_version: "events.k8s.io/v1".to_string(),
                kind: "Event".to_string(),
                namespace: Some("event-conflict".to_string()),
                name: "duplicate-event".to_string(),
                data: json!({
                    "apiVersion": "events.k8s.io/v1",
                    "kind": "Event",
                    "metadata": {
                        "namespace": "event-conflict",
                        "name": "duplicate-event"
                    },
                    "eventTime": "2026-07-07T00:00:01Z",
                    "reportingController": "klights.test",
                    "reportingInstance": "worker-a",
                    "action": "Started",
                    "reason": "Duplicate",
                    "regarding": {"kind": "Pod", "namespace": "event-conflict", "name": "pod-a"}
                }),
            }),
            "worker-a",
            Some(first_watermark.clone()),
        )
        .await
        .expect("terminal duplicate Event must still consume the stream watermark");

        assert!(matches!(first.result, OutboxApplyResult::Applied { .. }));
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![first_watermark]
        );

        propose_outbox_on_backend_with_watermark(
            &db,
            "next-event-create",
            OutboxOperation::EventCreate,
            outbox_payload(StorageCommand::CreateResource {
                api_version: "events.k8s.io/v1".to_string(),
                kind: "Event".to_string(),
                namespace: Some("event-conflict".to_string()),
                name: "next-event".to_string(),
                data: json!({
                    "apiVersion": "events.k8s.io/v1",
                    "kind": "Event",
                    "metadata": {
                        "namespace": "event-conflict",
                        "name": "next-event"
                    },
                    "eventTime": "2026-07-07T00:00:02Z",
                    "reportingController": "klights.test",
                    "reportingInstance": "worker-a",
                    "action": "Started",
                    "reason": "Next",
                    "regarding": {"kind": "Pod", "namespace": "event-conflict", "name": "pod-b"}
                }),
            }),
            "worker-a",
            Some(crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 54,
                stream_seq: 2,
            }),
        )
        .await
        .expect("next stream entry must not wedge behind terminal EventCreate conflict");

        assert!(
            db.get_resource(
                "events.k8s.io/v1",
                "Event",
                Some("event-conflict"),
                "next-event"
            )
            .await
            .unwrap()
            .is_some()
        );
    }

    #[tokio::test]
    async fn watermarked_duplicate_node_registration_advances_stream() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "mn-replica",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "mn-replica", "uid": "existing-node-uid"}
            }),
        )
        .await
        .expect("seed duplicate Node");

        let first_watermark = crate::log_apply::OutboxStreamWatermark {
            client_id: "worker-client".to_string(),
            stream_id: 62,
            stream_seq: 1,
        };
        let first = propose_outbox_on_backend_with_watermark(
            &db,
            "duplicate-node-registration",
            OutboxOperation::NodeRegistration,
            outbox_payload(StorageCommand::CreateResource {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: "mn-replica".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "mn-replica"},
                    "spec": {},
                    "status": {}
                }),
            }),
            "mn-replica",
            Some(first_watermark.clone()),
        )
        .await
        .expect("duplicate NodeRegistration must still consume the stream watermark");

        assert!(matches!(first.result, OutboxApplyResult::Applied { .. }));
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![first_watermark]
        );

        propose_outbox_on_backend_with_watermark(
            &db,
            "next-node-registration-stream-entry",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::CreateNamespace {
                name: "after-node-registration-gap".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "after-node-registration-gap"}
                }),
            }),
            "mn-replica",
            Some(crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 62,
                stream_seq: 2,
            }),
        )
        .await
        .expect("next stream entry must not wedge behind duplicate NodeRegistration");

        assert!(
            db.get_namespace("after-node-registration-gap")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn raft_outbox_runtime_reconcile_applies_complete_worker_status() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "kube-system",
                    "name": "coredns",
                    "uid": "uid-coredns"
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "coredns", "image": "coredns/coredns:1.11.1"}]
                },
                "status": {
                    "phase": "Pending",
                    "podIP": "10.50.1.3",
                    "podIPs": [{"ip": "10.50.1.3"}],
                    "hostIP": "10.99.0.14",
                    "hostIPs": [{"ip": "10.99.0.14"}],
                    "containerStatuses": [{
                        "name": "coredns",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ContainerCreating"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        propose_outbox_on_backend(
            &db,
            "runtime-reconcile-complete-status",
            OutboxOperation::RuntimeReconcile,
            outbox_payload(StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("kube-system".to_string()),
                name: "coredns".to_string(),
                status: json!({
                    "phase": "Running",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.15",
                    "hostIPs": [{"ip": "10.99.0.15"}],
                    "containerStatuses": [{
                        "name": "coredns",
                        "containerID": "containerd://container-a",
                        "image": "docker.io/coredns/coredns:1.11.1",
                        "imageID": "sha256:test",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-31T10:53:05Z"}}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-coredns".to_string()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            }),
            "worker-1",
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.data["status"]["phase"], json!("Running"));
        assert_eq!(stored.data["status"]["podIP"], json!("10.50.1.9"));
        assert_eq!(stored.data["status"]["podIPs"][0]["ip"], json!("10.50.1.9"));
        assert_eq!(stored.data["status"]["hostIP"], json!("10.99.0.15"));
        assert_eq!(
            stored.data["status"]["hostIPs"][0]["ip"],
            json!("10.99.0.15")
        );
        assert_eq!(
            stored.data["status"]["containerStatuses"][0]["state"]["running"]["startedAt"],
            json!("2026-05-31T10:53:05Z")
        );
    }

    #[tokio::test]
    async fn raft_leader_status_update_preserves_authored_unknown_conditions() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "kube-system",
                    "name": "coredns",
                    "uid": "uid-coredns"
                },
                "spec": {
                    "nodeName": "mn-controlplane1",
                    "containers": [{"name": "coredns", "image": "coredns/coredns:1.11.1"}]
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.50.0.2",
                    "podIPs": [{"ip": "10.50.0.2"}],
                    "containerStatuses": [{
                        "name": "coredns",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-31T10:53:05Z"}}
                    }],
                    "conditions": [
                        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-05-31T10:53:05Z"},
                        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-31T10:53:05Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        propose_outbox_on_backend(
            &db,
            "raft-leader-mn-controlplane2-local-status",
            OutboxOperation::PodStatus,
            outbox_payload(StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("kube-system".to_string()),
                name: "coredns".to_string(),
                status: json!({
                    "phase": "Unknown",
                    "podIP": "10.50.0.2",
                    "podIPs": [{"ip": "10.50.0.2"}],
                    "containerStatuses": [{
                        "name": "coredns",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-31T10:53:05Z"}}
                    }],
                    "conditions": [
                        {
                            "type": "ContainersReady",
                            "status": "Unknown",
                            "reason": "NodeStatusUnknown",
                            "message": "Node status is unknown.",
                            "lastTransitionTime": "2026-05-31T10:54:00Z"
                        },
                        {
                            "type": "Ready",
                            "status": "Unknown",
                            "reason": "NodeStatusUnknown",
                            "message": "Node status is unknown.",
                            "lastTransitionTime": "2026-05-31T10:54:00Z"
                        }
                    ]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-coredns".to_string()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            }),
            "mn-controlplane2",
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();
        let conditions = stored.data["status"]["conditions"].as_array().unwrap();
        for condition_type in ["ContainersReady", "Ready"] {
            let condition = conditions
                .iter()
                .find(|condition| condition["type"] == condition_type)
                .unwrap();
            assert_eq!(condition["status"], json!("Unknown"));
            assert_eq!(condition["reason"], json!("NodeStatusUnknown"));
        }
    }
}
