//! Immutable replication-owned datastore sequencing tests.

// Test assertions briefly lock a mock proposer's recorded-call log to
// inspect it after an awaited operation; the std guard is dropped at end of
// statement and the test runtime is single-threaded.
#![allow(clippy::await_holding_lock)]
use super::*;
use crate::datastore::backend::DatastoreBackend;
use async_trait::async_trait;
use klights_cluster_core::command::{COMMAND_CODEC_VERSION, CommandId};
use klights_cluster_core::{
    PatchKind, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a completely constructed sequencer with an inline proposal
/// capability that applies commands to the passive backend.
async fn make_ds_with_inline_proposer() -> (
    SequencedDatastore,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    use crate::datastore::backend::DatastoreHandle;

    struct InlineProposer {
        inner: DatastoreHandle,
        calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl super::RaftProposal for InlineProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            self.calls
                .lock()
                .unwrap()
                .push(command.variant_name().to_string());
            if matches!(command, StorageCommand::DeleteResourceWithTombstone { .. }) {
                let commit = self
                    .inner
                    .build_log_apply_commit_for_command(
                        command,
                        klights_kubelet::node_outbox::payload::OutboxOperation::PodStatus.as_str(),
                        "inline-proposer",
                    )
                    .await?;
                return self.inner.apply_raft_log_apply_commit(commit).await;
            }
            crate::bootstrap::outbox_apply_adapter::propose_command_on_backend(
                self.inner.as_ref(),
                command,
            )
            .await
            .map_err(|e| anyhow::anyhow!("inline propose: {e}"))
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            self.calls
                .lock()
                .unwrap()
                .push(command.variant_name().to_string());
            let outcome =
                crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    klights_kubelet::node_outbox::payload::OutboxOperation::try_from(operation)
                        .map_err(|e| {
                            klights_cluster_core::OutboxApplyError::Retryable(e.to_string())
                        })?,
                    command,
                    authoring_node,
                    None,
                )
                .await?;
            Ok(outcome.into_parts().0)
        }
    }

    let inner: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let proposer = Arc::new(InlineProposer {
        inner: inner.clone(),
        calls: calls.clone(),
    });
    let ds = SequencedDatastore::new(inner, proposer);
    (ds, calls)
}

struct PanicProposal;

#[async_trait]
impl super::RaftProposal for PanicProposal {
    async fn propose_command(
        &self,
        _command: StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        panic!("this operation must not submit a raft proposal")
    }

    async fn propose_outbox_command(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        panic!("this operation must not submit an outbox proposal")
    }
}

fn assert_application_apply_rejected(error: anyhow::Error, operation: &str) {
    let message = error.to_string();
    assert!(
        message.contains("sequenced datastore rejects application-side committed apply"),
        "unexpected {operation} rejection: {message}"
    );
    assert!(
        message.contains(operation),
        "{operation} rejection must name the denied operation: {message}"
    );
    assert!(
        message.contains("private passive Raft state-machine backend"),
        "{operation} rejection must identify the privileged owner: {message}"
    );
}

/// DSB-HA-02: SingleNode (Raft N=1) exercises the replicated path
/// through the raft proposer.

/// DSB-HA-02: leader allows writes through raft proposer.

/// T7.2: leader writes route through the raft proposer.

// T3: `leader_write_appends_durable_log_apply_entry` deleted —
// `log_apply_entries` table and its backend methods are removed.
// Raft AppendEntries through apply_log_apply_commit is the only
// replication path (T1.3).

// T3: `log_apply_commit_uses_watch_row_*` and `log_apply_auto_index_*`
// tests deleted — `log_apply_entries` table and
// `log_apply_commit_for_applied_command` method are removed.

/// LeaseRenew outbox operations are short-circuited and return
/// early without routing through the raft proposer.

/// DSB-HA-02 coverage gate: the DatastoreApplier impl maps every
/// StorageCommand variant to a corresponding Datastore method.

/// P3-11c4: in Raft mode with a RaftProposal attached, `create_resource`
/// must route the StorageCommand through the proposer instead of
/// hitting the inner backend directly. The inline proposer in this
/// test records each call and then applies the command synchronously
/// against the inner so the wrapper's read-back succeeds.

/// P3-11c4: delete_resource_with_preconditions_observed_rv must route
/// the DeleteResource command through raft, then surface the cluster's
/// current resource version (read back after the apply path advances).

// ── T7.1: EnsureClusterMetadata command ──

// ── T7.3: follower proposer rejects before local mutation ──

/// Helper: creates a SequencedDatastore in Raft mode with a
/// proposer that always rejects (simulating a non-leader node).
async fn make_ds_with_follower_proposer() -> (
    SequencedDatastore,
    std::sync::Arc<dyn crate::datastore::DatastoreBackend>,
) {
    let inner: std::sync::Arc<dyn crate::datastore::DatastoreBackend> = std::sync::Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    struct FollowerProposer;
    #[async_trait]
    impl super::RaftProposal for FollowerProposer {
        async fn propose_command(
            &self,
            _command: klights_cluster_core::command::StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            Err(anyhow::anyhow!(
                "not the leader; forward to current raft leader"
            ))
        }
        async fn propose_outbox_command(
            &self,
            _k: &str,
            _o: &str,
            _c: klights_cluster_core::command::StorageCommand,
            _a: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            Err(klights_cluster_core::OutboxApplyError::Retryable(
                "not the leader".into(),
            ))
        }
    }
    let ds = SequencedDatastore::new(inner.clone(), std::sync::Arc::new(FollowerProposer));
    (ds, inner)
}

// ── T7.1 gap: set_klights_meta must route through raft proposer ──

/// With an inline proposer, set_klights_meta must route through raft
/// and the value must be visible after apply.

/// Follower proposer must reject set_klights_meta without local mutation.

/// Live multinode regression: a leader-side scheduler preemption writes the
/// victim's termination as a full `UpdateResource` (metadata.deletionTimestamp
/// plus a status carrying the scheduler-owned `DisruptionTarget` condition).
/// That write is replicated through raft, so it lands in
/// `apply_command_to_backend`. A concurrent kubelet status write can bump the
/// live row's resourceVersion ahead of the preemption command's meta RV
/// before the preemption command applies. In that case the apply path
/// preserves the live `.status` over the proposed one via
/// `preserve_status_subresource_on_main_update` — and that preserve step
/// MUST route through the central Pod status merge so the scheduler-owned
/// `DisruptionTarget` condition is not dropped on the floor.

/// Multinode scheduler bind regression: the scheduler writes a full Pod
/// `UpdateResource` carrying both `spec.nodeName` and `PodScheduled=True`.
/// Raft apply preserves Pod status for ordinary main-resource updates, but
/// it must not preserve the old `SchedulingPending` condition over a
/// scheduler-owned bind transition. Otherwise the object becomes internally
/// inconsistent (`spec.nodeName` set while `PodScheduled=False`) and e2e
/// waits for Running time out on a pod that kubelet never admits.

/// Reproduces the live SchedulerPreemption conformance failure: after the
/// leader-side scheduler preemption writes `DisruptionTarget` to the victim,
/// the leader's own kubelet runtime-reconcile status write races the
/// preemption and lands a snapshot computed BEFORE preemption (no
/// DisruptionTarget). That status write is proposed through raft as
/// `StorageCommand::UpdateStatus` with `observed_status_stamp: None` — the
/// leader-direct path never carries an outbox stamp. The raft apply must
/// still preserve scheduler-owned Pod conditions, otherwise the stale
/// kubelet snapshot permanently clobbers `DisruptionTarget` (subsequent
/// reconciles read the clobbered row and never restore the condition),
/// which is exactly what the live run observed: victim terminating with no
/// DisruptionTarget.

struct ReplicatedStaleStatusCase {
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&'static str>,
    name: &'static str,
    uid: &'static str,
    initial: serde_json::Value,
    stale_status: serde_json::Value,
    expected_pointer: &'static str,
    expected_value: serde_json::Value,
}

async fn apply_replicated_stale_status_case(
    case: ReplicatedStaleStatusCase,
) -> crate::datastore::Resource {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            case.api_version,
            case.kind,
            case.namespace,
            case.name,
            case.initial,
        )
        .await
        .expect("create stale status fixture");

    db.patch_resource_latest_with_preconditions(
        case.api_version,
        case.kind,
        case.namespace,
        case.name,
        crate::datastore::ResourcePatchRequest::new(
            crate::datastore::PatchKind::Merge,
            serde_json::json!({"metadata": {"annotations": {"patchedstatus": "true"}}}),
            ResourcePreconditions {
                uid: Some(case.uid.to_string()),
                resource_version: None,
            },
        ),
    )
    .await
    .expect("metadata patch advances live resourceVersion");

    apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: case.api_version.into(),
            kind: case.kind.into(),
            namespace: case.namespace.map(str::to_string),
            name: case.name.into(),
            status: case.stale_status.clone(),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some(case.uid.into()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId(format!("stale-status-{}-{}", case.kind, case.name)),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: created.resource_version,
            uid: Some(case.uid.into()),
            timestamp_ms: 0,
            authoring_node: "controlplane1".into(),
        },
    )
    .await
    .expect("same-UID stale status apply should rebase onto metadata-only rv churn");

    let live = db
        .get_resource(case.api_version, case.kind, case.namespace, case.name)
        .await
        .expect("read final stale status fixture")
        .expect("final stale status fixture exists");
    assert_eq!(
        live.data.pointer(case.expected_pointer),
        Some(&case.expected_value)
    );
    assert_eq!(
        live.data.pointer("/metadata/annotations/patchedstatus"),
        Some(&serde_json::json!("true")),
        "status rebase must preserve metadata-only changes that advanced the resourceVersion"
    );
    live
}

#[path = "tests/namespace.rs"]
mod namespace;
#[path = "tests/outbox.rs"]
mod outbox;
#[path = "tests/pod.rs"]
mod pod;
#[path = "tests/recovery.rs"]
mod recovery;
#[path = "tests/resource.rs"]
mod resource;
#[path = "tests/status_network.rs"]
mod status_network;
#[path = "tests/watch_meta.rs"]
mod watch_meta;
