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
        let applied = propose_outbox_on_backend(
            self.backend.as_ref(),
            idempotency_key,
            operation,
            payload,
            authoring_node,
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
    crate::control_plane::client::apply::reject_pod_uid_mismatch(db, &decoded.command).await?;

    let deleted_resource = resource_before_delete(db, &decoded.command).await?;
    let result = match db
        .apply_outbox_transactionally(
            idempotency_key,
            operation.as_str(),
            payload.as_ref(),
            authoring_node,
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
    Ok(RaftOutboxApply {
        result,
        resource,
        command: Some(decoded.command),
    })
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
        StorageCommand::DeleteResource { .. } => Ok(deleted_resource),
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
