use std::sync::Arc;

use bytes::Bytes;
use klights_cluster_core::{
    OutboxApplyError, OutboxApplyOutcome, OutboxOperation, StorageCommand,
    classify_apply_error_for_command, pod_target,
};
use klights_cluster_store::{
    PodUidPreconditionRead as _, PodUidPreconditionRequest, PodUidPreconditionState,
};
use tokio::sync::RwLock;

use super::super::DatastoreBackend;
use crate::replication::protocol::ForwardedResource;
use crate::storage_wire_codec::decode_outbox_payload_protobuf;

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
    let decoded = decode_outbox_payload_protobuf(&payload)
        .map_err(|err| OutboxApplyError::Retryable(err.to_string()))?;
    if operation == OutboxOperation::LeaseRenew {
        crate::node_lease_tracker::ensure_lease_renew_command(&decoded.command, authoring_node)
            .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
        return Ok(RaftOutboxApply {
            result: OutboxApplyOutcome::Applied { applied_rv: 0 },
            resource: None,
            command: None,
            pod_endpoint_effect: super::super::PodEndpointEffect::NotApplicable,
        });
    }
    if watermark.is_none() {
        reject_pod_uid_mismatch(db, &decoded.command).await?;
    }

    let watermark_present = watermark.is_some();
    let uid_bound_pod_target = is_uid_bound_pod_command(&decoded.command);
    let durable_actor_finalization = operation == OutboxOperation::PodMetadata
        && (matches!(decoded.command, StorageCommand::FinalizeBoundPod { .. })
            || matches!(
                &decoded.command,
                StorageCommand::DeleteResource {
                    api_version,
                    kind,
                    namespace: Some(_),
                    preconditions,
                    ..
                } if api_version == "v1"
                    && kind == "Pod"
                    && preconditions.uid.as_deref().is_some_and(|uid| !uid.is_empty())
                    && preconditions.resource_version.is_none()
            ));
    let resource_before = resource_before_apply(db, &decoded.command).await?;
    let (result, resource_effect, pod_endpoint_effect, committed_resource) = match db
        .apply_outbox_transactionally_with_watermark_effect(
            idempotency_key,
            operation.as_str(),
            payload.as_ref(),
            authoring_node,
            watermark,
        )
        .await
    {
        Ok(effect) => effect.into_parts(),
        Err(err) => {
            let classified = match err {
                OutboxApplyError::Retryable(_) => {
                    classify_apply_error_for_command(&decoded.command, err)
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
    let resource_changed = resource_effect == super::super::ResourceMutationEffect::Changed;

    // T1: All apply results are now propagated (errors surface,
    // AlreadyApplied returns the stored resource). The log_apply
    // follower ensures proper state ordering so these no longer
    // need to be silently swallowed.
    let pod_patch_is_explicitly_irrelevant = matches!(
        &decoded.command,
        StorageCommand::PatchResource {
            api_version,
            kind,
            patch,
            ..
        } if api_version == "v1"
            && kind == "Pod"
            && !pod_patch_touches_endpoint_metadata(patch)
    );
    let resource = if matches!(
        decoded.command,
        StorageCommand::DeleteResource { .. } | StorageCommand::FinalizeBoundPod { .. }
    ) {
        committed_resource.map(crate::replication::protocol::ForwardedResource::from)
    } else if matches!(
        decoded.command,
        StorageCommand::DeleteResourceWithTombstone { .. }
    ) {
        resource_before.clone()
    } else if resource_changed && !pod_patch_is_explicitly_irrelevant {
        resource_after_apply(db, &decoded.command).await?
    } else {
        None
    };
    let pod_resource_effect = pod_side_effect_resource_changed(
        &decoded.command,
        resource_before.as_ref(),
        resource.as_ref(),
        resource_changed,
    );
    let newly_applied = matches!(result, OutboxApplyOutcome::Applied { .. });
    let replayable_actor_cascade = durable_actor_finalization && resource.is_some();
    let suppress_side_effect = (!newly_applied && !replayable_actor_cascade)
        || (watermark_present && uid_bound_pod_target && resource.is_none())
        || pod_resource_effect == Some(false);
    let resource = if pod_resource_effect == Some(false) {
        None
    } else {
        resource
    };
    let effect_command = if durable_actor_finalization
        && matches!(decoded.command, StorageCommand::DeleteResource { .. })
    {
        resource.as_ref().map_or(decoded.command, |resource| {
            crate::bound_pod_finalization_command::author(
                resource
                    .namespace
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
                resource.name.clone(),
                resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                authoring_node.to_string(),
                resource.resource_version,
            )
        })
    } else {
        decoded.command
    };
    let command = (!suppress_side_effect).then_some(effect_command);
    Ok(RaftOutboxApply {
        result,
        resource,
        command,
        pod_endpoint_effect,
    })
}

async fn reject_pod_uid_mismatch(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
) -> std::result::Result<(), OutboxApplyError> {
    let Some((namespace, name, preconditions)) = pod_target(command) else {
        return Ok(());
    };
    let Some(expected_uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
        return Ok(());
    };
    let request = PodUidPreconditionRequest::new(namespace, name, expected_uid);
    match db
        .read_pod_uid_precondition(request)
        .await
        .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?
    {
        PodUidPreconditionState::Matches => Ok(()),
        PodUidPreconditionState::Missing => Err(OutboxApplyError::NotFound(format!(
            "Pod {namespace}/{name} not found"
        ))),
        PodUidPreconditionState::Mismatch { actual_uid } => Err(OutboxApplyError::UidMismatch {
            expected: expected_uid.to_string(),
            actual: actual_uid,
        }),
    }
}

fn is_uid_bound_pod_command(command: &StorageCommand) -> bool {
    if let StorageCommand::FinalizeBoundPod {
        namespace,
        name,
        pod_uid,
        node_name,
        observed_resource_version,
    } = command
    {
        return !namespace.is_empty()
            && !name.is_empty()
            && !pod_uid.is_empty()
            && !node_name.is_empty()
            && *observed_resource_version > 0;
    }
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
    pub(crate) result: OutboxApplyOutcome,
    pub(crate) resource: Option<ForwardedResource>,
    pub(crate) command: Option<StorageCommand>,
    pub(crate) pod_endpoint_effect: super::super::PodEndpointEffect,
}

impl RaftOutboxApply {
    pub fn applied_resource_version(&self) -> Option<i64> {
        match &self.result {
            OutboxApplyOutcome::Applied { applied_rv } => Some(*applied_rv),
            OutboxApplyOutcome::AlreadyApplied { applied_rv } => *applied_rv,
        }
    }
}

async fn resource_before_apply(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
) -> std::result::Result<Option<ForwardedResource>, OutboxApplyError> {
    match command {
        StorageCommand::DeleteResource { .. } => Ok(None),
        StorageCommand::DeleteResourceWithTombstone {
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
        StorageCommand::FinalizeBoundPod { .. } => Ok(None),
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            patch,
            ..
        } if api_version == "v1" && kind == "Pod" && pod_patch_touches_endpoint_metadata(patch) => {
            db.get_resource(api_version, kind, namespace.as_deref(), name)
                .await
                .map(|resource| resource.map(ForwardedResource::from))
                .map_err(|err| OutboxApplyError::Retryable(err.to_string()))
        }
        _ => Ok(None),
    }
}

async fn resource_after_apply(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
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
        }
        | StorageCommand::PatchResource {
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
        _ => Ok(None),
    }
}

fn pod_side_effect_resource_changed(
    command: &StorageCommand,
    resource_before: Option<&ForwardedResource>,
    resource_after: Option<&ForwardedResource>,
    resource_changed: bool,
) -> Option<bool> {
    match command {
        StorageCommand::UpdateStatus {
            api_version, kind, ..
        } if api_version == "v1" && kind == "Pod" => Some(resource_changed),
        StorageCommand::DeleteResource {
            api_version, kind, ..
        } if api_version == "v1" && kind == "Pod" => Some(resource_after.is_some()),
        StorageCommand::FinalizeBoundPod { .. } => Some(resource_after.is_some()),
        StorageCommand::PatchResource {
            api_version,
            kind,
            patch,
            ..
        } if api_version == "v1" && kind == "Pod" => Some(
            pod_patch_touches_endpoint_metadata(patch)
                && resource_changed
                && pod_endpoint_metadata_changed(resource_before, resource_after),
        ),
        _ => None,
    }
}

fn pod_patch_touches_endpoint_metadata(patch: &serde_json::Value) -> bool {
    patch
        .pointer("/metadata")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|metadata| {
            metadata.contains_key("labels") || metadata.contains_key("deletionTimestamp")
        })
}

fn pod_endpoint_metadata_changed(
    before: Option<&ForwardedResource>,
    after: Option<&ForwardedResource>,
) -> bool {
    ["/metadata/labels", "/metadata/deletionTimestamp"]
        .into_iter()
        .any(|pointer| {
            before.and_then(|resource| resource.data.pointer(pointer))
                != after.and_then(|resource| resource.data.pointer(pointer))
        })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::datastore::ResourcePreconditions;
    use crate::node_outbox::payload::OutboxPayload;

    fn outbox_payload(command: StorageCommand) -> Bytes {
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode outbox payload"),
        )
    }

    #[tokio::test]
    async fn pod_patch_effects_expose_only_endpoint_relevant_changed_resources() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "patched",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "patched",
                        "uid": "patched-uid",
                        "labels": {"app": "old"}
                    },
                    "spec": {"nodeName": "worker-a"}
                }),
            )
            .await
            .unwrap();
        let patch = |id: &'static str, patch: serde_json::Value| {
            propose_outbox_on_backend(
                &db,
                id,
                OutboxOperation::PodMetadata,
                outbox_payload(StorageCommand::PatchResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "patched".to_string(),
                    patch_kind: crate::datastore::PatchKind::Merge,
                    patch,
                    preconditions: ResourcePreconditions {
                        uid: Some(pod.uid.clone()),
                        resource_version: None,
                    },
                    strict_resource_version: false,
                }),
                "worker-a",
            )
        };

        let labels = patch(
            "patch-labels",
            json!({"metadata": {"labels": {"app": "new"}}}),
        )
        .await
        .unwrap();
        assert!(labels.resource.is_some());
        assert!(labels.command.is_some());

        let deleting = patch(
            "patch-deletion-timestamp",
            json!({"metadata": {"deletionTimestamp": "2026-07-18T00:00:00Z"}}),
        )
        .await
        .unwrap();
        assert!(deleting.resource.is_some());
        assert!(deleting.command.is_some());

        let annotation = patch(
            "patch-annotation",
            json!({"metadata": {"annotations": {"example.test/value": "x"}}}),
        )
        .await
        .unwrap();
        assert!(annotation.resource.is_none());
        assert!(annotation.command.is_none());

        let no_op = patch(
            "patch-labels-noop",
            json!({"metadata": {"labels": {"app": "new"}}}),
        )
        .await
        .unwrap();
        assert!(no_op.resource.is_none());
        assert!(no_op.command.is_none());
    }

    #[tokio::test]
    async fn actor_finalization_persists_exact_receipt_for_durable_cascade_replay() {
        let db = crate::datastore::test_support::in_memory().await;
        let create_bound_terminating_pod = |name: &'static str, uid: &'static str, finalizers| {
            db.create_resource(
                "v1",
                "Pod",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": name,
                        "uid": uid,
                        "deletionTimestamp": "2026-07-24T00:00:00Z",
                        "finalizers": finalizers
                    },
                    "spec": {"nodeName": "worker-a"}
                }),
            )
        };
        create_bound_terminating_pod("blocked", "blocked-uid", json!(["hold.example/finalizer"]))
            .await
            .unwrap();
        let eligible = create_bound_terminating_pod("eligible", "eligible-uid", json!([]))
            .await
            .unwrap();
        let eligible_before = ForwardedResource::from(eligible);

        let finalize =
            |id: &'static str, name: &'static str, uid: &'static str, observed_resource_version| {
                propose_outbox_on_backend(
                    &db,
                    id,
                    OutboxOperation::PodMetadata,
                    outbox_payload(StorageCommand::FinalizeBoundPod {
                        namespace: "default".to_string(),
                        name: name.to_string(),
                        pod_uid: uid.to_string(),
                        node_name: "worker-a".to_string(),
                        observed_resource_version,
                    }),
                    "worker-a",
                )
            };

        let blocked_rv = db
            .get_resource("v1", "Pod", Some("default"), "blocked")
            .await
            .unwrap()
            .unwrap()
            .resource_version;
        let blocked = finalize("blocked-finalize", "blocked", "blocked-uid", blocked_rv)
            .await
            .unwrap();
        assert!(blocked.command.is_none());
        assert!(blocked.resource.is_none());
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "blocked")
                .await
                .unwrap()
                .is_some()
        );

        let deleted = finalize(
            "eligible-finalize",
            "eligible",
            "eligible-uid",
            eligible_before.resource_version,
        )
        .await
        .unwrap();
        assert!(matches!(
            deleted.command,
            Some(StorageCommand::FinalizeBoundPod { .. })
        ));
        assert_eq!(deleted.resource.as_ref(), Some(&eligible_before));
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "eligible")
                .await
                .unwrap()
                .is_none()
        );

        let duplicate = finalize(
            "eligible-finalize",
            "eligible",
            "eligible-uid",
            eligible_before.resource_version,
        )
        .await
        .unwrap();
        assert!(matches!(
            duplicate.result,
            OutboxApplyOutcome::AlreadyApplied { .. }
        ));
        assert!(
            matches!(
                duplicate.command,
                Some(StorageCommand::FinalizeBoundPod { .. })
            ),
            "a committed actor delete must remain replayable until its durable \
             dependent cascade has been delivered"
        );
        assert_eq!(
            duplicate.resource.as_ref(),
            Some(&eligible_before),
            "replay must use the exact transactional pre-delete Pod, not a \
             detached read after the row is gone"
        );
    }

    #[tokio::test]
    async fn actor_finalization_serializes_after_same_uid_metadata_generation() {
        let db = crate::datastore::test_support::in_memory().await;
        let observed = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "generation-race",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "generation-race",
                        "uid": "stable-uid",
                        "deletionTimestamp": "2026-07-24T00:00:00Z"
                    },
                    "spec": {"nodeName": "worker-a"}
                }),
            )
            .await
            .unwrap();
        let mut newer = (*observed.data).clone();
        newer["metadata"]["annotations"] = json!({"race.example/generation": "newer"});
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "generation-race",
            newer,
            observed.resource_version,
        )
        .await
        .unwrap();

        let finalized = propose_outbox_on_backend(
            &db,
            "stale-generation-finalize",
            OutboxOperation::PodMetadata,
            outbox_payload(StorageCommand::FinalizeBoundPod {
                namespace: "default".to_string(),
                name: "generation-race".to_string(),
                pod_uid: "stable-uid".to_string(),
                node_name: "worker-a".to_string(),
                observed_resource_version: observed.resource_version,
            }),
            "worker-a",
        )
        .await
        .unwrap();

        assert!(matches!(
            finalized.command,
            Some(StorageCommand::FinalizeBoundPod { .. })
        ));
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "generation-race")
                .await
                .unwrap()
                .is_none(),
            "benign same-UID metadata churn must serialize before actor finalization"
        );
    }

    #[tokio::test]
    async fn pod_status_apply_reports_effective_endpoint_delta_from_pre_and_post_state() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "status-delta",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "status-delta",
                        "uid": "status-delta-uid",
                        "labels": {"app": "web"}
                    },
                    "spec": {"nodeName": "worker-a"},
                    "status": {
                        "phase": "Running",
                        "podIP": "10.42.0.8",
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
        let apply = |id: &'static str, status: serde_json::Value| {
            propose_outbox_on_backend(
                &db,
                id,
                OutboxOperation::PodStatus,
                outbox_payload(StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "status-delta".to_string(),
                    status,
                    expected_rv: None,
                    preconditions: ResourcePreconditions {
                        uid: Some(pod.uid.clone()),
                        resource_version: None,
                    },
                    observed_status_stamp: None,
                }),
                "worker-a",
            )
        };

        let unchanged = apply(
            "same-endpoint-status",
            json!({
                "phase": "Running",
                "podIP": "10.42.0.8",
                "conditions": [{"type": "Ready", "status": "True"}]
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            unchanged.pod_endpoint_effect,
            crate::datastore::PodEndpointEffect::Unchanged
        );

        let changed = apply(
            "changed-endpoint-status",
            json!({
                "phase": "Running",
                "podIP": "10.42.0.9",
                "conditions": [{"type": "Ready", "status": "True"}]
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            changed.pod_endpoint_effect,
            crate::datastore::PodEndpointEffect::Changed
        );
    }

    #[tokio::test]
    async fn watermarked_stale_uid_bound_pod_row_advances_stream_without_side_effect_command() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "legacy-rv-seed",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "legacy-rv-seed"}
            }),
        )
        .await
        .unwrap();
        let rv_before = db.get_current_resource_version().await.unwrap();
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
        .await;
        let Err(error) = result else {
            panic!("stale UID-bound Pod status must return its durable typed terminal decision")
        };

        assert!(
            matches!(&error, OutboxApplyError::NotFound(_)),
            "unexpected stale UID result: {error:?}"
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![watermark]
        );
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            rv_before,
            "ledger-only stale status must not allocate a public resourceVersion"
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
    async fn stamped_stale_equal_pod_status_ledger_only_has_no_side_effect() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "stamped-side-effect",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "stamped-side-effect",
                    "uid": "stamped-side-effect-uid"
                },
                "spec": {"nodeName": "worker-a"},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("seed Pod");
        let mut watch = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"));

        let deliver = |key: &'static str, stream_seq: i64, stamp: i64, phase: &'static str| {
            propose_outbox_on_backend_with_watermark(
                &db,
                key,
                OutboxOperation::PodStatus,
                outbox_payload(StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "stamped-side-effect".to_string(),
                    status: json!({"phase": phase}),
                    expected_rv: None,
                    preconditions: ResourcePreconditions {
                        uid: Some("stamped-side-effect-uid".to_string()),
                        resource_version: None,
                    },
                    observed_status_stamp: Some(stamp),
                }),
                "worker-a",
                Some(crate::log_apply::OutboxStreamWatermark {
                    client_id: "worker-client".to_string(),
                    stream_id: 119,
                    stream_seq,
                }),
            )
        };

        let reads_before_fresh = db.resource_get_call_count_for_test();
        let fresh = deliver("fresh-status", 1, 10, "Running")
            .await
            .expect("fresh status applies");
        assert_eq!(
            db.resource_get_call_count_for_test() - reads_before_fresh,
            1,
            "fresh status needs only its returned-resource read"
        );
        assert!(fresh.command.is_some(), "fresh status must emit effects");
        assert!(
            fresh.resource.is_some(),
            "fresh status must return its resource effect"
        );
        watch.try_recv().expect("fresh status emits a watch event");
        let fresh_rv = db.get_current_resource_version().await.unwrap();

        let duplicate = deliver("fresh-status", 1, 10, "Running")
            .await
            .expect("same idempotency key replays its durable result");
        assert!(
            matches!(duplicate.result, OutboxApplyOutcome::AlreadyApplied { .. }),
            "same idempotency key must be reported as AlreadyApplied"
        );
        assert!(
            duplicate.command.is_none(),
            "AlreadyApplied replay must not re-fire controller or Service effects"
        );
        assert!(
            duplicate.resource.is_none(),
            "AlreadyApplied replay must not claim a new resource effect"
        );
        assert_eq!(
            duplicate.pod_endpoint_effect,
            crate::datastore::PodEndpointEffect::Unchanged,
            "AlreadyApplied must preserve the atomic no-effect classification"
        );
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            fresh_rv,
            "AlreadyApplied replay must remain resourceVersion-neutral"
        );
        assert!(matches!(
            watch.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        for (key, stream_seq, stamp) in [("stale-status", 2, 9), ("equal-status", 3, 10)] {
            let reads_before = db.resource_get_call_count_for_test();
            let no_change = deliver(key, stream_seq, stamp, "Pending")
                .await
                .expect("stale/equal status consumes its ledger position");
            assert_eq!(
                db.resource_get_call_count_for_test() - reads_before,
                0,
                "{key} needs no inference or returned-resource read"
            );
            assert!(
                no_change.command.is_none(),
                "{key} must not return a command that can fire controller/Service effects"
            );
            assert!(
                no_change.resource.is_none(),
                "{key} must not claim the still-live Pod as a resource effect"
            );
            assert_eq!(
                no_change.pod_endpoint_effect,
                crate::datastore::PodEndpointEffect::NotApplicable,
                "{key} has no committed Pod mutation to classify"
            );
            assert_eq!(
                db.get_current_resource_version().await.unwrap(),
                fresh_rv,
                "{key} must stay resourceVersion-neutral"
            );
            assert!(
                matches!(
                    watch.try_recv(),
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty)
                ),
                "{key} must stay watch-neutral"
            );
            assert!(
                db.get_applied_outbox(key).await.unwrap().is_some(),
                "{key} must retain its durable idempotency ledger"
            );
        }
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![crate::log_apply::OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 119,
                stream_seq: 3,
            }],
            "stale/equal decisions must durably advance the exact stream"
        );

        let reads_before_newer = db.resource_get_call_count_for_test();
        let newer = deliver("newer-status", 4, 11, "Succeeded")
            .await
            .expect("newer status applies after ledger-only decisions");
        assert_eq!(
            db.resource_get_call_count_for_test() - reads_before_newer,
            1,
            "newer status needs only its returned-resource read"
        );
        assert!(newer.command.is_some(), "newer status must emit effects");
        assert_eq!(
            newer.pod_endpoint_effect,
            crate::datastore::PodEndpointEffect::Changed
        );
        assert!(
            newer.resource.is_some(),
            "newer status must return its resource effect"
        );
        watch.try_recv().expect("newer status emits a watch event");
        assert!(db.get_current_resource_version().await.unwrap() > fresh_rv);
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

        assert!(matches!(first.result, OutboxApplyOutcome::Applied { .. }));
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

        assert!(matches!(first.result, OutboxApplyOutcome::Applied { .. }));
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

        assert!(matches!(first.result, OutboxApplyOutcome::Applied { .. }));
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
