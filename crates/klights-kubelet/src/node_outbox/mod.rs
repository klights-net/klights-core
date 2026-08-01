pub mod payload;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use klights_leader_api::{LeaderOutboxDelivery, OutboxDeliveryRequest, OutboxPayloadCodec};
use klights_node_store::{
    OutboxAttemptFailure, OutboxAttemptFailureRecord, OutboxBatchClaimRequest, OutboxClaimRequest,
    OutboxCompletion, OutboxDispatchCounters, OutboxDispatcherStore, OutboxEnqueue,
    OutboxFailureDisposition, OutboxLease, OutboxNow, OutboxProducerStore, OutboxRecord,
    OutboxStatusStampStore, OutboxSupersedeRequest, PodCheckpointKey, PodStatusCheckpointApplied,
    PodStatusCheckpointStore, PodStatusCheckpointUpsert, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration,
};
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use klights_cluster_core::Resource;
use klights_cluster_core::StorageCommand;
use klights_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};
use klights_types::{PodIdentity, ResourceKey};

use self::payload::{OutboxOperation, OutboxOperationExt as _};

use klights_cluster_core::OutboxApplyOutcome as OutboxApplyResult;

// bug-grpc: lease must outlast a worst-case pipelined WAN apply so a slow
// `apply_outbox` does not expire its own claim mid-flight (which would let
// `requeue_expired_outbox_leases` re-claim it and make the post-RPC
// `complete_outbox` race on a stale token). Sized at ~6× the gRPC connect
// timeout (10 s) so even a full handshake + slow round-trip stays inside.
const DEFAULT_LEASE_MS: i64 = 60_000;
const MAX_BACKOFF_MS: i64 = 60_000;
const MAX_OUTBOX_ATTEMPTS: i64 = 720;
// bug-grpc: in-flight window for pipelined leader dispatch. Matches the
// Status channel-lane pool size so concurrent `apply_outbox` calls spread
// one-per-connection across the lane (no single-connection TCP HOL).
pub const DEFAULT_DISPATCH_INFLIGHT: usize = 4;
pub const PRODUCTION_DISPATCH_BATCH_SIZE: usize = 16;
// bug-grpc: backoff after a transient dispatch-iteration error so the
// dispatcher loop never exits (worker status reporting must not die).
const DISPATCH_ERROR_BACKOFF_MS: u64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxSendRoute {
    /// Request was enqueued into node-local outbox and will be retried by
    /// `OutboxDispatcher`.
    Enqueued,
}

pub struct OutboxSendPlanner<'a> {
    outbox: Option<&'a Outbox>,
}

impl<'a> OutboxSendPlanner<'a> {
    pub const fn new(outbox: Option<&'a Outbox>) -> Self {
        Self { outbox }
    }

    pub async fn route(&self, command: OutboxCommand) -> Result<OutboxSendRoute> {
        let Some(outbox) = self.outbox else {
            anyhow::bail!(
                "outbox is unavailable for node-local queueing; caller must retry after outbox initialization"
            );
        };
        outbox.enqueue_command(command).await?;
        Ok(OutboxSendRoute::Enqueued)
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn enqueue_node_dataplane_metadata(
    outbox: Option<&Outbox>,
    node_name: &str,
    mode: klights_leader_api::NetworkNodeMode,
    encryption: klights_leader_api::DataplaneEncryption,
    public_key: Option<String>,
    endpoint: String,
    port: Option<u16>,
    now_ms: i64,
) -> Result<()> {
    let subject_key = format!("v1/Node/{node_name}/dataplane");
    OutboxSendPlanner::new(outbox)
        .route(OutboxCommand {
            idempotency_key: format!("NodeDataplane:{subject_key}:{}", uuid::Uuid::new_v4()),
            operation: OutboxOperation::NodeDataplane,
            subject: OutboxSubject {
                key: subject_key,
                namespace: None,
                name: node_name.to_string(),
                uid: None,
            },
            pod_uid: String::new(),
            command: StorageCommand::UpdateNodeDataplane {
                node_name: node_name.to_string(),
                mode: match mode {
                    klights_leader_api::NetworkNodeMode::Root => "root",
                    klights_leader_api::NetworkNodeMode::Rootless => "rootless",
                }
                .to_string(),
                encryption: match encryption {
                    klights_leader_api::DataplaneEncryption::WireGuard => "enabled",
                    klights_leader_api::DataplaneEncryption::Direct => "disabled",
                }
                .to_string(),
                public_key,
                endpoint,
                port,
            },
            now_ms,
        })
        .await
        .map(|_| ())
}

#[derive(Clone)]
pub struct OutboxStores {
    producer: Arc<dyn OutboxProducerStore>,
    dispatcher: Arc<dyn OutboxDispatcherStore>,
    pod_checkpoints: Arc<dyn PodStatusCheckpointStore>,
    runtime_checkpoints: Arc<dyn RuntimeObservationCheckpointStore>,
    status_stamps: Arc<dyn OutboxStatusStampStore>,
}

impl OutboxStores {
    pub fn new(
        producer: Arc<dyn OutboxProducerStore>,
        dispatcher: Arc<dyn OutboxDispatcherStore>,
        pod_checkpoints: Arc<dyn PodStatusCheckpointStore>,
        runtime_checkpoints: Arc<dyn RuntimeObservationCheckpointStore>,
        status_stamps: Arc<dyn OutboxStatusStampStore>,
    ) -> Self {
        Self {
            producer,
            dispatcher,
            pod_checkpoints,
            runtime_checkpoints,
            status_stamps,
        }
    }

    async fn enqueue_outbox(&self, entry: OutboxEnqueue) -> Result<()> {
        self.producer
            .enqueue_outbox(entry)
            .await
            .map_err(Into::into)
    }

    async fn get_pod_status_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<PodStatusCheckpointState>> {
        let checkpoint = self
            .pod_checkpoints
            .get_pod_status_checkpoint(PodCheckpointKey::try_new(pod_uid)?)
            .await?;
        checkpoint
            .map(|checkpoint| {
                Ok(PodStatusCheckpointState {
                    pod_uid: checkpoint.pod().uid.clone(),
                    namespace: checkpoint.pod().namespace.clone(),
                    pod_name: checkpoint.pod().name.clone(),
                    base_rv: checkpoint.base_position(),
                    applied_rv: checkpoint.applied_position(),
                    status: serde_json::from_slice(checkpoint.status_payload())?,
                })
            })
            .transpose()
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: Value,
        updated_ms: i64,
    ) -> Result<()> {
        let checkpoint = PodStatusCheckpointUpsert::try_new(
            PodIdentity::new(namespace, pod_name, pod_uid),
            base_rv,
            serde_json::to_vec(&status)?,
            updated_ms,
        )?;
        self.pod_checkpoints
            .upsert_pod_status_checkpoint(checkpoint)
            .await
            .map_err(Into::into)
    }

    async fn mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> Result<()> {
        self.pod_checkpoints
            .mark_pod_status_checkpoint_applied(PodStatusCheckpointApplied::try_new(
                pod_uid, applied_rv, updated_ms,
            )?)
            .await
            .map_err(Into::into)
    }

    async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.pod_checkpoints
            .delete_pod_status_checkpoint(PodCheckpointKey::try_new(pod_uid)?)
            .await
            .map_err(Into::into)
    }

    async fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpointState,
    ) -> Result<()> {
        self.runtime_checkpoints
            .upsert_runtime_observation_checkpoint(
                klights_node_store::RuntimeObservationCheckpoint::try_new(
                    checkpoint.pod_uid,
                    checkpoint.container_ids,
                    RuntimeObservationGeneration::try_from(checkpoint.generation)?,
                    checkpoint.updated_ms,
                )?,
            )
            .await
            .map_err(Into::into)
    }

    async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<RuntimeObservationCheckpointState>> {
        Ok(self
            .runtime_checkpoints
            .get_runtime_observation_checkpoint(PodCheckpointKey::try_new(pod_uid)?)
            .await?
            .map(|checkpoint| RuntimeObservationCheckpointState {
                pod_uid: checkpoint.pod_uid().to_string(),
                container_ids: checkpoint.container_ids().to_vec(),
                generation: checkpoint.generation().get() as u64,
                updated_ms: checkpoint.updated_ms(),
            }))
    }

    async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.runtime_checkpoints
            .delete_runtime_observation_checkpoint(PodCheckpointKey::try_new(pod_uid)?)
            .await
            .map_err(Into::into)
    }

    async fn requeue_expired_outbox_leases(&self, now_ms: i64) -> Result<usize> {
        self.dispatcher
            .requeue_expired_outbox_leases(OutboxNow::try_new(now_ms)?)
            .await
            .map_err(Into::into)
    }

    async fn claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Vec<OutboxRow>> {
        self.dispatcher
            .claim_due_outbox_batch(OutboxBatchClaimRequest::try_new(
                now_ms,
                limit,
                lease_ms,
                lease_token,
            )?)
            .await?
            .into_iter()
            .map(OutboxRow::try_from_record)
            .collect()
    }

    async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Option<OutboxRow>> {
        self.dispatcher
            .claim_next_due_outbox(OutboxClaimRequest::try_new(now_ms, lease_ms, lease_token)?)
            .await?
            .map(OutboxRow::try_from_record)
            .transpose()
    }

    async fn next_outbox_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
        self.dispatcher
            .next_outbox_wake_ms(OutboxNow::try_new(now_ms)?)
            .await
            .map_err(Into::into)
    }

    async fn mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> Result<bool> {
        self.dispatcher
            .mark_outbox_attempt_failed(OutboxAttemptFailure::try_new(
                id,
                lease_token,
                backoff_until_ms,
                error,
            )?)
            .await
            .map_err(Into::into)
    }

    async fn record_outbox_failure(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
        max_attempts: i64,
    ) -> Result<OutboxFailureDisposition> {
        self.dispatcher
            .record_outbox_failure(OutboxAttemptFailureRecord::try_new(
                id,
                lease_token,
                backoff_until_ms,
                error,
                max_attempts,
            )?)
            .await
            .map_err(Into::into)
    }

    async fn renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> Result<bool> {
        self.dispatcher
            .renew_outbox_lease(OutboxLease::try_new(id, lease_token, leased_until_ms)?)
            .await
            .map_err(Into::into)
    }

    async fn complete_outbox(&self, id: i64, lease_token: &str) -> Result<bool> {
        self.dispatcher
            .complete_outbox(OutboxCompletion::try_new(id, lease_token)?)
            .await
            .map_err(Into::into)
    }

    async fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> Result<usize> {
        self.dispatcher
            .complete_superseded_status_outbox_for_terminal_pod_delete(
                OutboxSupersedeRequest::try_new(subject_key, terminal_delete_id)?,
            )
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
struct PodStatusCheckpointState {
    pod_uid: String,
    namespace: String,
    pod_name: String,
    base_rv: i64,
    applied_rv: Option<i64>,
    status: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservationCheckpointState {
    pub pod_uid: String,
    pub container_ids: Vec<String>,
    pub generation: u64,
    pub updated_ms: i64,
}

#[derive(Debug, Clone)]
struct OutboxRow {
    id: i64,
    client_id: String,
    idempotency_key: String,
    subject_key: String,
    pod_uid: String,
    operation: String,
    is_terminal_pod_delete: bool,
    stream_id: i64,
    stream_seq: i64,
    payload_proto: Vec<u8>,
    attempt: i64,
    next_due_ms: i64,
    lease_token: Option<String>,
}

impl OutboxRow {
    fn try_from_record(record: OutboxRecord) -> Result<Self> {
        Ok(Self {
            id: record.id(),
            client_id: record.client_id().to_string(),
            idempotency_key: record.idempotency_key().to_string(),
            subject_key: record.subject().subject_key().to_string(),
            pod_uid: record.subject().pod_uid().to_string(),
            operation: record.operation().to_string(),
            is_terminal_pod_delete: record.classification().terminal_delete()
                == klights_node_store::TerminalDeleteClassification::ActorOwnedPodDelete,
            stream_id: record.sequence().stream_id(),
            stream_seq: record.sequence().stream_seq(),
            payload_proto: record.payload().to_vec(),
            attempt: record.attempt(),
            next_due_ms: record.next_due_ms(),
            lease_token: record.lease_token().map(str::to_string),
        })
    }
}

#[derive(Clone)]
pub struct Outbox {
    stores: OutboxStores,
    codec: Arc<dyn OutboxPayloadCodec>,
    notify: Arc<Notify>,
    stamp: Arc<tokio::sync::Mutex<StampState>>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

/// In-memory state of the per-node status-stamp allocator. `next` is the last
/// stamp issued; `reserved` is the durable ceiling persisted to node-local meta
/// (see [`Outbox::next_status_stamp`]). `seeded` guards the one-time load of the
/// persisted ceiling on first use.
#[derive(Default)]
struct StampState {
    seeded: bool,
    next: i64,
    reserved: i64,
}

/// Headroom (in stamp units) reserved per node-local persistence write. The
/// ceiling is persisted at most once per this many issued stamps (or per this
/// many microseconds of wall-clock advance), bounding node-local writes while
/// keeping idle cost at zero.
const STATUS_STAMP_RESERVE_BLOCK: i64 = 5_000_000;

pub struct OutboxSubject {
    pub key: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: Option<String>,
}

impl OutboxSubject {
    pub fn new(
        key: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        uid: Option<String>,
    ) -> Self {
        Self {
            key: key.into(),
            namespace,
            name: name.into(),
            uid,
        }
    }
}

pub struct OutboxCommand {
    pub idempotency_key: String,
    pub operation: OutboxOperation,
    pub subject: OutboxSubject,
    pub pod_uid: String,
    pub command: StorageCommand,
    pub now_ms: i64,
}

impl OutboxCommand {
    pub fn new(
        idempotency_key: impl Into<String>,
        operation: OutboxOperation,
        subject: OutboxSubject,
        pod_uid: impl Into<String>,
        command: StorageCommand,
        now_ms: i64,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            operation,
            subject,
            pod_uid: pod_uid.into(),
            command,
            now_ms,
        }
    }
}

impl Outbox {
    pub fn compose(
        stores: OutboxStores,
        codec: Arc<dyn OutboxPayloadCodec>,
        notify: Arc<Notify>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            stores,
            codec,
            notify,
            stamp: Arc::new(tokio::sync::Mutex::new(StampState::default())),
            wall_clock,
        }
    }

    pub fn notify_handle(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    /// Issue a strictly-monotonic per-node status stamp for an outbound Pod
    /// status snapshot.
    ///
    /// The leader drops an outbox status whose stamp is `<=` the one it last
    /// applied for that Pod (the lost-update gate), so a stamp that regressed
    /// across a worker restart — e.g. an NTP step-back or VM clock skew — would
    /// make a genuinely newer status look stale and be silently discarded. To
    /// stay monotonic independent of the wall clock we persist a reserved
    /// ceiling to node-local meta *before* issuing any stamp that reaches it
    /// (a hi/lo allocator), so the seed on the next boot is always `>=` every
    /// stamp already issued. A wall-clock floor keeps freshly issued stamps
    /// comparable in magnitude with rows written before this allocator existed.
    pub async fn next_status_stamp(&self) -> Result<i64> {
        let now_us = wall_clock_epoch_ms(self.wall_clock.as_ref()).saturating_mul(1_000);
        self.next_status_stamp_with_clock(now_us).await
    }

    /// Clock-injected core of [`Outbox::next_status_stamp`] for deterministic
    /// tests (including simulated clock regression across restart).
    async fn next_status_stamp_with_clock(&self, now_us: i64) -> Result<i64> {
        let mut st = self.stamp.lock().await;
        if !st.seeded {
            let persisted = self
                .stores
                .status_stamps
                .read_status_stamp_high_water()
                .await?;
            // Seed both the issue cursor and the reserved ceiling from the
            // persisted high-water so the first stamp after restart exceeds
            // every previously issued stamp even if the clock has regressed.
            st.next = persisted;
            st.reserved = persisted;
            st.seeded = true;
        }
        let candidate = now_us.max(st.next.saturating_add(1));
        if candidate >= st.reserved {
            // Reserve and durably persist a new ceiling BEFORE issuing, so a
            // crash can never lose a stamp below what was already handed out.
            let new_reserved = candidate.saturating_add(STATUS_STAMP_RESERVE_BLOCK);
            self.stores
                .status_stamps
                .write_status_stamp_high_water(new_reserved)
                .await?;
            st.reserved = new_reserved;
        }
        st.next = candidate;
        Ok(candidate)
    }

    pub async fn enqueue_command(&self, command: OutboxCommand) -> Result<()> {
        let OutboxCommand {
            idempotency_key,
            operation,
            subject,
            pod_uid,
            command,
            now_ms,
        } = command;
        let OutboxSubject {
            key: subject_key,
            namespace: subject_namespace,
            name: subject_name,
            uid: subject_uid,
        } = subject;
        let classification = operation
            .classification(&command)
            .map_err(anyhow::Error::new)?;
        let payload = self.codec.encode(&command)?.to_vec();
        let (subject_api_version, subject_kind) = operation.subject_api_version_kind();
        let resource = ResourceKey::new(
            subject_api_version,
            subject_kind,
            subject_namespace,
            subject_name,
        );
        self.stores
            .enqueue_outbox(OutboxEnqueue::try_new(
                idempotency_key,
                now_ms,
                klights_node_store::OutboxSubject::new(subject_key, resource, subject_uid, pod_uid),
                operation.as_str(),
                classification,
                payload,
                now_ms,
            )?)
            .await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn record_pod_status_checkpoint(
        &self,
        pod: &Resource,
        status: Value,
        updated_ms: i64,
    ) -> Result<()> {
        let namespace = pod.namespace.as_deref().unwrap_or("default");
        let previous = self.stores.get_pod_status_checkpoint(&pod.uid).await?;
        let status = merge_checkpoint_status_for_record(pod, status, previous.as_ref(), updated_ms);
        self.stores
            .upsert_pod_status_checkpoint(
                &pod.uid,
                namespace,
                &pod.name,
                pod.resource_version,
                status,
                updated_ms,
            )
            .await
    }

    pub async fn merge_pod_status_checkpoint(&self, mut pod: Resource) -> Result<Resource> {
        let Some(checkpoint) = self.stores.get_pod_status_checkpoint(&pod.uid).await? else {
            return Ok(pod);
        };

        let namespace = pod.namespace.as_deref().unwrap_or("default");
        if checkpoint.namespace != namespace || checkpoint.pod_name != pod.name {
            self.stores
                .delete_pod_status_checkpoint(&checkpoint.pod_uid)
                .await?;
            return Ok(pod);
        }

        if let Some(applied_rv) = checkpoint.applied_rv
            && pod.resource_version >= applied_rv
            && pod_status_contains_checkpoint(&pod.data, &checkpoint.status)
        {
            self.stores
                .delete_pod_status_checkpoint(&checkpoint.pod_uid)
                .await?;
            return Ok(pod);
        }

        if pod.resource_version < checkpoint.base_rv {
            return Ok(pod);
        }

        let mut data = (*pod.data).clone();
        if !data.is_object() {
            return Ok(pod);
        }
        let Some(object) = data.as_object_mut() else {
            return Ok(pod);
        };
        let status_slot = object
            .entry("status".to_string())
            .or_insert_with(|| Value::Object(Default::default()));
        match (status_slot.as_object_mut(), checkpoint.status) {
            (Some(live), Value::Object(pending)) => {
                for (key, value) in pending {
                    live.insert(key, value);
                }
            }
            (_, pending) => {
                *status_slot = pending;
            }
        }
        pod.data = Arc::new(data);
        Ok(pod)
    }

    pub async fn mark_pod_status_checkpoint_applied_result(
        &self,
        pod_uid: &str,
        result: &OutboxApplyResult,
        updated_ms: i64,
    ) -> Result<()> {
        match result {
            OutboxApplyResult::Applied { applied_rv }
            | OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(applied_rv),
            } => {
                self.stores
                    .mark_pod_status_checkpoint_applied(pod_uid, *applied_rv, updated_ms)
                    .await
            }
            OutboxApplyResult::AlreadyApplied { applied_rv: None } => Ok(()),
        }
    }

    pub async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.stores.delete_pod_status_checkpoint(pod_uid).await
    }

    pub async fn record_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
        container_ids: Vec<String>,
        generation: u64,
        updated_ms: i64,
    ) -> Result<()> {
        self.stores
            .upsert_runtime_observation_checkpoint(RuntimeObservationCheckpointState {
                pod_uid: pod_uid.to_string(),
                container_ids,
                generation,
                updated_ms,
            })
            .await
    }

    pub async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<RuntimeObservationCheckpointState>> {
        self.stores
            .get_runtime_observation_checkpoint(pod_uid)
            .await
    }

    pub async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.stores
            .delete_runtime_observation_checkpoint(pod_uid)
            .await
    }
}

impl klights_leader_api::NodeOutbox for Outbox {
    fn enqueue(
        &self,
        command: klights_leader_api::NodeOutboxCommand,
    ) -> klights_leader_api::NodeOutboxFuture<'_, klights_leader_api::NodeOutboxRoute> {
        Box::pin(async move {
            self.enqueue_command(OutboxCommand {
                idempotency_key: command.idempotency_key,
                operation: command.operation,
                subject: OutboxSubject {
                    key: command.subject.key,
                    namespace: command.subject.namespace,
                    name: command.subject.name,
                    uid: command.subject.uid,
                },
                pod_uid: command.pod_uid,
                command: command.command,
                now_ms: command.now_ms,
            })
            .await
            .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))?;
            Ok(klights_leader_api::NodeOutboxRoute::Enqueued)
        })
    }

    fn next_status_stamp(&self) -> klights_leader_api::NodeOutboxFuture<'_, i64> {
        Box::pin(async move {
            Outbox::next_status_stamp(self)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }

    fn record_pod_status_checkpoint<'a>(
        &'a self,
        checkpoint: &'a Resource,
        updated_ms: i64,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async move {
            let status = checkpoint
                .data
                .pointer("/status")
                .cloned()
                .unwrap_or(Value::Null);
            Outbox::record_pod_status_checkpoint(self, checkpoint, status, updated_ms)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }

    fn merge_pod_status_checkpoint(
        &self,
        pod: Resource,
    ) -> klights_leader_api::NodeOutboxFuture<'_, Resource> {
        Box::pin(async move {
            Outbox::merge_pod_status_checkpoint(self, pod)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }

    fn delete_pod_status_checkpoint<'a>(
        &'a self,
        pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async move {
            Outbox::delete_pod_status_checkpoint(self, pod_uid)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }

    fn record_runtime_observation_checkpoint<'a>(
        &'a self,
        pod_uid: &'a str,
        container_ids: Vec<String>,
        generation: u64,
        updated_ms: i64,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async move {
            Outbox::record_runtime_observation_checkpoint(
                self,
                pod_uid,
                container_ids,
                generation,
                updated_ms,
            )
            .await
            .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }

    fn get_runtime_observation_checkpoint<'a>(
        &'a self,
        pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<
        'a,
        Option<klights_leader_api::NodeRuntimeObservationCheckpoint>,
    > {
        Box::pin(async move {
            let checkpoint = Outbox::get_runtime_observation_checkpoint(self, pod_uid)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))?;
            Ok(checkpoint.map(|checkpoint| {
                klights_leader_api::NodeRuntimeObservationCheckpoint::new(
                    checkpoint.pod_uid,
                    checkpoint.container_ids,
                    checkpoint.generation,
                )
            }))
        })
    }

    fn delete_runtime_observation_checkpoint<'a>(
        &'a self,
        pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async move {
            Outbox::delete_runtime_observation_checkpoint(self, pod_uid)
                .await
                .map_err(|error| klights_leader_api::NodeOutboxError::new(error.to_string()))
        })
    }
}

fn merge_checkpoint_status_for_record(
    pod: &Resource,
    mut incoming: Value,
    previous: Option<&PodStatusCheckpointState>,
    updated_ms: i64,
) -> Value {
    let operation_now =
        chrono::DateTime::from_timestamp_millis(updated_ms).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    let namespace = pod.namespace.as_deref().unwrap_or("default");
    let Some(previous) = previous else {
        normalize_bound_pod_scheduled_condition(pod, &mut incoming, None, operation_now);
        return incoming;
    };
    if previous.namespace != namespace || previous.pod_name != pod.name {
        normalize_bound_pod_scheduled_condition(pod, &mut incoming, None, operation_now);
        return incoming;
    }

    if let (Some(incoming_obj), Some(previous_obj)) =
        (incoming.as_object_mut(), previous.status.as_object())
    {
        preserve_previous_checkpoint_field(incoming_obj, previous_obj, "podIP");
        preserve_previous_checkpoint_field(incoming_obj, previous_obj, "podIPs");
        preserve_previous_checkpoint_field(incoming_obj, previous_obj, "hostIP");
        preserve_previous_checkpoint_field(incoming_obj, previous_obj, "hostIPs");
        preserve_previous_checkpoint_field(incoming_obj, previous_obj, "qosClass");
        merge_checkpoint_conditions(incoming_obj, previous_obj);
    }

    normalize_bound_pod_scheduled_condition(
        pod,
        &mut incoming,
        Some(&previous.status),
        operation_now,
    );
    incoming
}

fn preserve_previous_checkpoint_field(
    incoming: &mut serde_json::Map<String, Value>,
    previous: &serde_json::Map<String, Value>,
    field: &str,
) {
    if !incoming
        .get(field)
        .is_none_or(status_checkpoint_value_is_empty)
    {
        return;
    }
    let Some(previous_value) = previous.get(field) else {
        return;
    };
    if status_checkpoint_value_is_empty(previous_value) {
        return;
    }
    incoming.insert(field.to_string(), previous_value.clone());
}

fn status_checkpoint_value_is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.trim().is_empty(),
        Value::Array(values) => values.is_empty(),
        Value::Object(values) => values.is_empty(),
        _ => false,
    }
}

fn merge_checkpoint_conditions(
    incoming: &mut serde_json::Map<String, Value>,
    previous: &serde_json::Map<String, Value>,
) {
    let Some(previous_conditions) = previous
        .get("conditions")
        .and_then(|value| value.as_array())
    else {
        return;
    };
    let conditions = incoming
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
    }
    let Some(incoming_conditions) = conditions.as_array_mut() else {
        return;
    };
    for condition in previous_conditions {
        let Some(condition_type) = condition.get("type").and_then(|value| value.as_str()) else {
            continue;
        };
        if incoming_conditions.iter().any(|candidate| {
            candidate.get("type").and_then(|value| value.as_str()) == Some(condition_type)
        }) {
            continue;
        }
        incoming_conditions.push(condition.clone());
    }
}

fn normalize_bound_pod_scheduled_condition(
    pod: &Resource,
    status: &mut Value,
    previous_status: Option<&Value>,
    operation_now: chrono::DateTime<chrono::Utc>,
) {
    let bound = pod
        .data
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .is_some_and(|node| !node.is_empty());
    if !bound {
        return;
    }

    let true_condition = pod_scheduled_true_condition(status)
        .or_else(|| previous_status.and_then(pod_scheduled_true_condition))
        .or_else(|| {
            pod_scheduled_true_condition(pod.data.pointer("/status").unwrap_or(&Value::Null))
        })
        .unwrap_or_else(|| {
            serde_json::json!({
                "type": "PodScheduled",
                "status": "True",
                "lastTransitionTime": klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now),
            })
        });

    let Some(status_obj) = status.as_object_mut() else {
        return;
    };
    let conditions = status_obj
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };
    if let Some(existing) = conditions.iter_mut().find(|condition| {
        condition.get("type").and_then(|value| value.as_str()) == Some("PodScheduled")
    }) {
        *existing = true_condition;
    } else {
        conditions.push(true_condition);
    }
}

fn pod_scheduled_true_condition(status: &Value) -> Option<Value> {
    status
        .get("conditions")
        .and_then(|conditions| conditions.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("PodScheduled")
                    && condition.get("status").and_then(|value| value.as_str()) == Some("True")
            })
        })
        .cloned()
}

fn pod_status_contains_checkpoint(pod: &Value, checkpoint_status: &Value) -> bool {
    let Some(live_status) = pod.pointer("/status") else {
        return false;
    };
    let Some(checkpoint) = checkpoint_status.as_object() else {
        return live_status == checkpoint_status;
    };
    let Some(live) = live_status.as_object() else {
        return false;
    };
    checkpoint
        .iter()
        .all(|(key, value)| live.get(key).is_some_and(|live_value| live_value == value))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    Dispatched,
    Idle { next_wake_ms: Option<i64> },
}

pub struct OutboxDispatcher {
    stores: OutboxStores,
    codec: Arc<dyn OutboxPayloadCodec>,
    client: Arc<dyn LeaderOutboxDelivery>,
    notify: Arc<Notify>,
    lease_renewal_supervisor: Option<Arc<TaskSupervisor>>,
    lease_ms: i64,
    batch_mode: bool,
    batch_size: usize,
    dispatch_total: std::sync::Arc<std::sync::atomic::AtomicU64>,
    dispatch_errors_total: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// T4/T7: RTT estimator fed by successful worker→leader apply_outbox
    /// round-trips. The retry backoff reads `estimate_ms()` instead of the
    /// old fixed 200 ms default, so a lossy ~400 ms RTT backs off on the right
    /// scale. Idle-silent (no applies ⇒ no samples).
    rtt: std::sync::Arc<klights_types::RttEstimator>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl OutboxDispatcher {
    pub fn new(
        stores: OutboxStores,
        codec: Arc<dyn OutboxPayloadCodec>,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self::new_with_rtt_estimator(
            stores,
            codec,
            client,
            notify,
            std::sync::Arc::new(klights_types::RttEstimator::new()),
            wall_clock,
        )
    }

    fn new_with_rtt_estimator(
        stores: OutboxStores,
        codec: Arc<dyn OutboxPayloadCodec>,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
        rtt: std::sync::Arc<klights_types::RttEstimator>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            stores,
            codec,
            client,
            notify,
            lease_renewal_supervisor: None,
            lease_ms: DEFAULT_LEASE_MS,
            batch_mode: false,
            batch_size: 16,
            dispatch_total: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            dispatch_errors_total: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            rtt,
            wall_clock,
        }
    }

    pub fn production(
        stores: OutboxStores,
        codec: Arc<dyn OutboxPayloadCodec>,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self::new(stores, codec, client, notify, wall_clock)
            .with_batch_mode(PRODUCTION_DISPATCH_BATCH_SIZE)
    }

    /// Return shared counters so callers (node_admin) can read them
    /// without going through node.db (future optimization).
    pub fn dispatch_counters(
        &self,
    ) -> (
        std::sync::Arc<std::sync::atomic::AtomicU64>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        (
            self.dispatch_total.clone(),
            self.dispatch_errors_total.clone(),
        )
    }

    /// T4/T7: current RTT estimate (ms) derived from successful apply
    /// round-trips, or the default before any traffic has flowed.
    #[cfg(feature = "test-support")]
    pub fn rtt_estimate_ms(&self) -> i64 {
        self.rtt.estimate_ms()
    }

    /// Enable leader dispatch batching: claims multiple rows per node.db
    /// transaction, applies each individually, then completes successes in
    /// a single node.db transaction.
    pub fn with_batch_mode(mut self, batch_size: usize) -> Self {
        self.batch_mode = true;
        self.batch_size = batch_size.clamp(1, 256);
        self
    }

    #[cfg(feature = "test-support")]
    pub fn compose_with_rtt_estimator_for_test(
        stores: OutboxStores,
        codec: Arc<dyn OutboxPayloadCodec>,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
        rtt: std::sync::Arc<klights_types::RttEstimator>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self::new_with_rtt_estimator(stores, codec, client, notify, rtt, wall_clock)
    }

    #[cfg(feature = "test-support")]
    pub fn with_lease_renewal_for_test(
        mut self,
        supervisor: Arc<TaskSupervisor>,
        lease_ms: i64,
    ) -> Self {
        self.lease_renewal_supervisor = Some(supervisor);
        self.lease_ms = lease_ms.max(1);
        self
    }

    pub async fn start(
        mut self,
        supervisor: Arc<TaskSupervisor>,
        cancel: CancellationToken,
    ) -> Result<SupervisedJoinHandle<()>> {
        self.lease_renewal_supervisor = Some(supervisor.clone());
        let supervisor_for_run = supervisor.clone();
        supervisor
            .spawn_async(
                TaskCategory::Background,
                "kubelet_outbox_dispatcher",
                async move {
                    if let Err(err) = self.run(supervisor_for_run, cancel).await {
                        tracing::warn!(error = %err, "outbox dispatcher stopped with error");
                    }
                },
            )
            .await
            .map_err(Into::into)
    }

    pub async fn run(
        self,
        supervisor: Arc<TaskSupervisor>,
        cancel: CancellationToken,
    ) -> Result<()> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            // bug-grpc: the dispatcher must NEVER exit on a transient
            // error — a dead dispatcher means the worker silently stops
            // reporting pod status (the 10-minute "stable cluster"
            // stall). A node.db blip, a slow-RPC lease race, or any
            // other transient failure is logged and backed off, then the
            // loop continues. Only `cancel` ends the task.
            match self
                .dispatch_due_once(wall_clock_epoch_ms(self.wall_clock.as_ref()))
                .await
            {
                Ok(DispatchOutcome::Dispatched) => continue,
                Ok(DispatchOutcome::Idle { next_wake_ms }) => {
                    let sleep_until = next_wake_ms
                        .map(|epoch_ms| {
                            instant_for_epoch_ms(
                                epoch_ms,
                                wall_clock_epoch_ms(self.wall_clock.as_ref()),
                            )
                        })
                        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = self.notify.notified() => {},
                        result = supervisor.sleep_until("kubelet_outbox_next_due", sleep_until) => {
                            result?;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "outbox dispatch iteration failed; backing off, NOT exiting"
                    );
                    self.dispatch_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = self.notify.notified() => {},
                        result = supervisor.sleep(
                            "kubelet_outbox_error_backoff",
                            Duration::from_millis(DISPATCH_ERROR_BACKOFF_MS),
                        ) => {
                            result?;
                        }
                    }
                }
            }
        }
    }

    pub async fn dispatch_due_once(&self, now_ms: i64) -> Result<DispatchOutcome> {
        self.stores.requeue_expired_outbox_leases(now_ms).await?;

        // Claim a window of due rows. In single mode the window is 1; in
        // batch mode it is `batch_size`. Either way the claimed rows are
        // dispatched concurrently (pipelined) so the worker keeps
        // multiple WAN `apply_outbox` round-trips in flight rather than
        // one row per RTT.
        let lease_token = uuid::Uuid::new_v4().to_string();
        let rows = if self.batch_mode {
            self.stores
                .claim_due_outbox_batch(now_ms, self.batch_size, self.lease_ms, &lease_token)
                .await?
        } else {
            self.stores
                .claim_next_due_outbox(now_ms, self.lease_ms, &lease_token)
                .await?
                .into_iter()
                .collect()
        };

        if rows.is_empty() {
            return Ok(DispatchOutcome::Idle {
                next_wake_ms: self.stores.next_outbox_wake_ms(now_ms).await?,
            });
        }

        tracing::info!(
            target: "klights::outbox_dispatch",
            claimed = rows.len(),
            "outbox dispatch: claimed due rows for dispatch"
        );

        self.dispatch_rows_pipelined(rows, now_ms).await;
        let _ = self.persist_dispatch_counters().await;
        Ok(DispatchOutcome::Dispatched)
    }

    /// bug-grpc: dispatch a batch of claimed rows concurrently with a
    /// bounded in-flight window. Each row's WAN `apply_outbox` and its
    /// node.db effects are handled independently by `process_claimed_row`,
    /// so a slow or failing row never stalls the others. Per-subject FIFO
    /// is preserved by the claim (at most one row per subject per batch),
    /// and cross-subject commands are idempotent + rv-guarded, so
    /// concurrent application is safe.
    async fn dispatch_rows_pipelined(&self, rows: Vec<OutboxRow>, now_ms: i64) {
        use futures::stream::{FuturesUnordered, StreamExt as _};

        let window = self.batch_size.max(DEFAULT_DISPATCH_INFLIGHT).max(1);
        let mut rows = rows.into_iter();
        let mut in_flight = FuturesUnordered::new();

        // Prime the window.
        for _ in 0..window {
            match rows.next() {
                Some(row) => in_flight.push(self.process_claimed_row(row, now_ms)),
                None => break,
            }
        }
        // Drain, refilling as each row completes to keep the window full
        // without ever exceeding it.
        while in_flight.next().await.is_some() {
            if let Some(row) = rows.next() {
                in_flight.push(self.process_claimed_row(row, now_ms));
            }
        }
    }

    /// bug-grpc: apply one claimed row end-to-end. Infallible at the
    /// dispatch level — every error is logged and made non-fatal so the
    /// dispatcher loop never exits:
    /// - a missing/stale lease token or a lost `complete_outbox` race is
    ///   warned and skipped; `requeue_expired_outbox_leases` re-claims it.
    /// - a transient apply error backs the row off; a terminal one drops
    ///   it; max attempts dead-letters it.
    ///
    /// Shared by single and batch dispatch (DRY): the only difference
    /// between the modes is the claim window size.
    async fn process_claimed_row(&self, row: OutboxRow, now_ms: i64) {
        let Some(lease_token) = row.lease_token.as_deref() else {
            tracing::warn!(
                outbox_id = row.id,
                "claimed outbox row has no lease token; skipping (will be requeued)"
            );
            return;
        };
        let assigned_stream_position = row.stream_id > 0 && row.stream_seq > 0;
        let operation = match OutboxOperation::try_from(row.operation.as_str()) {
            Ok(operation) => Some(operation),
            Err(err) => {
                tracing::warn!(
                    idempotency_key = %row.idempotency_key,
                    error = %err,
                    assigned_stream_position,
                    "unknown outbox operation"
                );
                if assigned_stream_position {
                    None
                } else {
                    self.dispatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.complete_row(row.id, lease_token, &row.idempotency_key)
                        .await;
                    return;
                }
            }
        };
        let mut use_terminal_sentinel = operation.is_none();
        let delivery_operation = if let Some(operation) = operation {
            match operation.try_delivery_operation() {
                Ok(operation) => operation,
                Err(err) => {
                    tracing::warn!(
                        idempotency_key = %row.idempotency_key,
                        error = %err,
                        assigned_stream_position,
                        "non-deliverable outbox operation"
                    );
                    if assigned_stream_position {
                        use_terminal_sentinel = true;
                        klights_leader_api::OutboxDeliveryOperation::PodStatus
                    } else {
                        self.dispatch_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.complete_row(row.id, lease_token, &row.idempotency_key)
                            .await;
                        return;
                    }
                }
            }
        } else {
            klights_leader_api::OutboxDeliveryOperation::PodStatus
        };

        let mut records_checkpoint = !use_terminal_sentinel
            && operation.is_some_and(OutboxOperation::supersedable_pod_status)
            && !row.pod_uid.is_empty();
        if records_checkpoint {
            tracing::info!(
                target: "klights::outbox_dispatch",
                idempotency_key = %row.idempotency_key,
                pod_uid = %row.pod_uid,
                attempt = row.attempt,
                "outbox dispatch: claimed pod-status row"
            );
        }
        let dispatch_start = std::time::Instant::now();
        let delivery_payload = if use_terminal_sentinel {
            match self
                .codec
                .encode(&payload::terminal_decision_command(&row.idempotency_key))
            {
                Ok(payload) => payload.to_vec(),
                Err(err) => {
                    tracing::warn!(
                        idempotency_key = %row.idempotency_key,
                        error = %err,
                        "failed to encode assigned terminal outbox decision"
                    );
                    let backoff_until_ms = now_ms.saturating_add(adaptive_jittered_backoff_ms(
                        row.attempt,
                        &row.idempotency_key,
                        self.rtt.estimate_ms(),
                    ));
                    if let Err(error) = self
                        .stores
                        .mark_outbox_attempt_failed(
                            row.id,
                            lease_token,
                            backoff_until_ms,
                            &err.to_string(),
                        )
                        .await
                    {
                        tracing::warn!(outbox_id = row.id, error = %error, "mark terminal-decision encode failure failed");
                    }
                    return;
                }
            }
        } else {
            row.payload_proto.clone()
        };
        let request = match OutboxDeliveryRequest::try_new(
            row.idempotency_key.clone(),
            delivery_operation,
            Arc::<[u8]>::from(delivery_payload),
            row.client_id.clone(),
            row.stream_id,
            row.stream_seq,
        ) {
            Ok(request) => request,
            Err(err) => {
                tracing::warn!(
                    idempotency_key = %row.idempotency_key,
                    error = %err,
                    "invalid durable outbox request"
                );
                let has_exact_delivery_identity = !row.idempotency_key.is_empty()
                    && !row.client_id.is_empty()
                    && row.stream_id > 0
                    && row.stream_seq > 0;
                if has_exact_delivery_identity {
                    let sentinel = self
                        .codec
                        .encode(&payload::terminal_decision_command(&row.idempotency_key))
                        .map_err(anyhow::Error::from)
                        .and_then(|payload| {
                            OutboxDeliveryRequest::try_new(
                                row.idempotency_key.clone(),
                                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                                payload,
                                row.client_id.clone(),
                                row.stream_id,
                                row.stream_seq,
                            )
                            .map_err(anyhow::Error::from)
                        });
                    if let Ok(request) = sentinel {
                        records_checkpoint = false;
                        request
                    } else {
                        self.dead_letter_invalid_claimed_row(
                            &row,
                            lease_token,
                            &format!("invalid assigned outbox request: {err}"),
                        )
                        .await;
                        return;
                    }
                } else {
                    self.dead_letter_invalid_claimed_row(
                        &row,
                        lease_token,
                        &format!("invalid unaddressable outbox request: {err}"),
                    )
                    .await;
                    return;
                }
            }
        };
        let applied = self
            .deliver_with_lease_renewal(&row, lease_token, request)
            .await;
        let elapsed_ms = dispatch_start.elapsed().as_millis() as u64;
        if records_checkpoint {
            tracing::info!(
                target: "klights::outbox_dispatch",
                idempotency_key = %row.idempotency_key,
                pod_uid = %row.pod_uid,
                attempt = row.attempt,
                elapsed_ms,
                resolved = !applied
                    .as_ref()
                    .is_err_and(|error| error.is_retryable() || !error.is_terminal()),
                "outbox dispatch: pod-status row apply_outbox resolved"
            );
        }
        match applied {
            Ok(result) => {
                // T4/T7: a successful apply round-trip is a clean RTT sample
                // for the worker→leader path; feed the backoff estimator.
                self.rtt.record_sample(dispatch_start.elapsed());
                self.dispatch_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let apply_result = OutboxApplyResult::from(result);
                if records_checkpoint
                    && let Err(err) = self
                        .mark_pod_status_checkpoint_applied_result(
                            &row.pod_uid,
                            &apply_result,
                            now_ms,
                        )
                        .await
                {
                    tracing::warn!(pod_uid = %row.pod_uid, error = %err, "mark checkpoint applied failed");
                }
                self.complete_row(row.id, lease_token, &row.idempotency_key)
                    .await;
                if row.is_terminal_pod_delete
                    && let Err(err) = self
                        .stores
                        .complete_superseded_status_outbox_for_terminal_pod_delete(
                            &row.subject_key,
                            row.id,
                        )
                        .await
                {
                    tracing::warn!(
                        outbox_id = row.id,
                        pod_uid = %row.pod_uid,
                        error = %err,
                        "complete superseded pod status outbox rows failed"
                    );
                }
                if row.is_terminal_pod_delete
                    && let Err(err) = self.stores.delete_pod_status_checkpoint(&row.pod_uid).await
                {
                    tracing::warn!(
                        outbox_id = row.id,
                        pod_uid = %row.pod_uid,
                        error = %err,
                        "delete terminal Pod status checkpoint failed"
                    );
                }
            }
            Err(err) if err.is_retryable() || !err.is_terminal() => {
                self.dispatch_errors_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let error = err.to_string();
                let backoff_until_ms = now_ms.saturating_add(adaptive_jittered_backoff_ms(
                    row.attempt,
                    &row.idempotency_key,
                    self.rtt.estimate_ms(),
                ));
                if matches!(
                    err,
                    klights_leader_api::OutboxDeliveryError::CodecIncompatible { .. }
                ) {
                    match self
                        .stores
                        .mark_outbox_attempt_failed(row.id, lease_token, backoff_until_ms, &error)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => tracing::warn!(
                            outbox_id = row.id,
                            "codec-incompatible outbox failure was not recorded because the lease was lost"
                        ),
                        Err(mark_err) => tracing::warn!(
                            outbox_id = row.id,
                            error = %mark_err,
                            "record codec-incompatible outbox retry failed"
                        ),
                    }
                    return;
                }
                match self
                    .stores
                    .record_outbox_failure(
                        row.id,
                        lease_token,
                        backoff_until_ms,
                        &error,
                        MAX_OUTBOX_ATTEMPTS,
                    )
                    .await
                {
                    Ok(OutboxFailureDisposition::DeadLettered) => {
                        tracing::warn!(
                            idempotency_key = %row.idempotency_key,
                            attempts = %row.attempt.saturating_add(1),
                            "outbox row exceeded max attempts, moving to dead letter"
                        );
                        if records_checkpoint
                            && let Err(err) =
                                self.stores.delete_pod_status_checkpoint(&row.pod_uid).await
                        {
                            tracing::warn!(pod_uid = %row.pod_uid, error = %err, "delete checkpoint failed");
                        }
                    }
                    Ok(OutboxFailureDisposition::RetryScheduled) => {}
                    Ok(OutboxFailureDisposition::LeaseLost) => tracing::warn!(
                        outbox_id = row.id,
                        "outbox failure was not recorded because the lease was lost"
                    ),
                    Err(move_err) => tracing::warn!(
                        outbox_id = row.id,
                        error = %move_err,
                        "record outbox failure or dead-letter move failed"
                    ),
                }
            }
            // All remaining `OutboxApplyError` variants (NotFound,
            // UidMismatch, ConflictTerminal) are terminal: drop the row.
            Err(err) => {
                debug_assert!(err.is_terminal());
                self.dispatch_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if actor_owned_pod_delete_needs_dead_letter(&row, &err) {
                    tracing::warn!(
                        idempotency_key = %row.idempotency_key,
                        error = %err,
                        "actor-owned Pod delete hit terminal outbox error; moving to dead letter"
                    );
                    match self
                        .stores
                        .record_outbox_failure(row.id, lease_token, now_ms, &err.to_string(), 1)
                        .await
                    {
                        Ok(OutboxFailureDisposition::DeadLettered) => {
                            if (records_checkpoint || row.is_terminal_pod_delete)
                                && let Err(error) =
                                    self.stores.delete_pod_status_checkpoint(&row.pod_uid).await
                            {
                                tracing::warn!(pod_uid = %row.pod_uid, error = %error, "delete checkpoint failed");
                            }
                            return;
                        }
                        Ok(OutboxFailureDisposition::LeaseLost) => {
                            tracing::warn!(
                                idempotency_key = %row.idempotency_key,
                                "actor-owned Pod delete terminal row was not moved to dead letter"
                            );
                            return;
                        }
                        Ok(OutboxFailureDisposition::RetryScheduled) => unreachable!(
                            "max_attempts=1 must dead-letter the first recorded failure"
                        ),
                        Err(move_err) => {
                            tracing::warn!(
                                idempotency_key = %row.idempotency_key,
                                error = %move_err,
                                "actor-owned Pod delete dead-letter move failed"
                            );
                            return;
                        }
                    }
                }
                if (records_checkpoint || row.is_terminal_pod_delete)
                    && let Err(error) = self.stores.delete_pod_status_checkpoint(&row.pod_uid).await
                {
                    tracing::warn!(pod_uid = %row.pod_uid, error = %error, "delete checkpoint failed");
                }
                tracing::debug!(
                    idempotency_key = %row.idempotency_key,
                    error = %err,
                    "dropping terminal outbox row"
                );
                self.complete_row(row.id, lease_token, &row.idempotency_key)
                    .await;
            }
        }
    }

    async fn dead_letter_invalid_claimed_row(
        &self,
        row: &OutboxRow,
        lease_token: &str,
        error: &str,
    ) {
        match self
            .stores
            .record_outbox_failure(row.id, lease_token, row.next_due_ms, error, 1)
            .await
        {
            Ok(OutboxFailureDisposition::DeadLettered) => tracing::warn!(
                outbox_id = row.id,
                "invalid durable outbox row moved to dead letter"
            ),
            Ok(OutboxFailureDisposition::LeaseLost) => tracing::warn!(
                outbox_id = row.id,
                "invalid durable outbox row retained after lease loss"
            ),
            Ok(OutboxFailureDisposition::RetryScheduled) => {
                unreachable!("max_attempts=1 must dead-letter the first recorded failure")
            }
            Err(error) => tracing::warn!(
                outbox_id = row.id,
                error = %error,
                "failed to durably dead-letter invalid outbox row"
            ),
        }
    }

    async fn deliver_with_lease_renewal(
        &self,
        row: &OutboxRow,
        lease_token: &str,
        request: OutboxDeliveryRequest,
    ) -> std::result::Result<
        klights_leader_api::OutboxDeliveryResult,
        klights_leader_api::OutboxDeliveryError,
    > {
        let Some(supervisor) = self.lease_renewal_supervisor.as_ref() else {
            return self.client.deliver_outbox(request).await;
        };

        let mut delivery = self.client.deliver_outbox(request);
        let renewal_period = Duration::from_millis((self.lease_ms / 3).max(1) as u64);
        let timer_name = format!("kubelet_outbox_lease_renewal/{}", row.id);
        loop {
            tokio::select! {
                result = &mut delivery => return result,
                timer_result = supervisor.sleep(timer_name.clone(), renewal_period) => {
                    timer_result.map_err(|error| {
                        klights_leader_api::OutboxDeliveryError::Retryable(error.to_string())
                    })?;
                    if supervisor.root_cancellation_token().is_cancelled() {
                        return Err(klights_leader_api::OutboxDeliveryError::cancelled());
                    }
                    let leased_until_ms = self
                        .lease_ms
                        .max(1)
                        .saturating_add(wall_clock_epoch_ms(self.wall_clock.as_ref()));
                    let renewed = self
                        .stores
                        .renew_outbox_lease(row.id, lease_token, leased_until_ms)
                        .await
                        .map_err(|error| {
                            klights_leader_api::OutboxDeliveryError::Retryable(error.to_string())
                        })?;
                    if !renewed {
                        return Err(klights_leader_api::OutboxDeliveryError::Retryable(format!(
                            "outbox lease lost while delivery was in flight for row {}",
                            row.id,
                        )));
                    }
                }
            }
        }
    }

    /// bug-grpc: complete a row, treating a lost lease race (0 rows
    /// changed / node.db error) as non-fatal — the row stays
    /// claimed-expired and `requeue_expired_outbox_leases` re-handles it.
    async fn complete_row(&self, id: i64, lease_token: &str, idempotency_key: &str) {
        match self.stores.complete_outbox(id, lease_token).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    outbox_id = id,
                    idempotency_key = %idempotency_key,
                    "complete_outbox found no matching lease (lease race); will be requeued"
                );
            }
            Err(err) => {
                tracing::warn!(
                    outbox_id = id,
                    idempotency_key = %idempotency_key,
                    error = %err,
                    "complete_outbox failed; will be requeued"
                );
            }
        }
    }

    async fn persist_dispatch_counters(&self) {
        let total = self
            .dispatch_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let errors = self
            .dispatch_errors_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let _ = self
            .stores
            .dispatcher
            .write_dispatch_counters(
                OutboxDispatchCounters::try_new(
                    total.min(i64::MAX as u64) as i64,
                    errors.min(i64::MAX as u64) as i64,
                )
                .expect("bounded dispatch counters are non-negative"),
            )
            .await;
    }

    async fn mark_pod_status_checkpoint_applied_result(
        &self,
        pod_uid: &str,
        result: &OutboxApplyResult,
        updated_ms: i64,
    ) -> Result<()> {
        match result {
            OutboxApplyResult::Applied { applied_rv }
            | OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(applied_rv),
            } => {
                self.stores
                    .mark_pod_status_checkpoint_applied(pod_uid, *applied_rv, updated_ms)
                    .await
            }
            OutboxApplyResult::AlreadyApplied { applied_rv: None } => Ok(()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_dispatcher_with_bootstrap_retry(
    stores: OutboxStores,
    codec: Arc<dyn OutboxPayloadCodec>,
    client: Arc<dyn LeaderOutboxDelivery>,
    notify: Arc<Notify>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
    supervisor: Arc<TaskSupervisor>,
    cancel: CancellationToken,
) {
    let supervisor_for_retry = supervisor.clone();
    match supervisor
        .spawn_async(
            TaskCategory::Background,
            "outbox_dispatcher_bootstrap_retry",
            async move {
                let mut delay = Duration::from_millis(500);
                let max_delay = Duration::from_secs(30);
                loop {
                    let dispatcher = OutboxDispatcher::production(
                        stores.clone(),
                        codec.clone(),
                        client.clone(),
                        notify.clone(),
                        wall_clock.clone(),
                    );
                    match dispatcher
                        .start(supervisor_for_retry.clone(), cancel.clone())
                        .await
                    {
                        Ok(_) => {
                            tracing::info!("Outbox dispatcher task started");
                            return;
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                delay_ms = %delay.as_millis(),
                                "outbox dispatcher start failed; retrying"
                            );
                            if supervisor_for_retry
                                .timeout(
                                    "outbox_dispatcher_retry_wait",
                                    delay,
                                    std::future::pending::<()>(),
                                )
                                .await
                                .is_err()
                            {
                                return;
                            }
                            let next_delay_ms = (delay.as_millis() * 2).min(max_delay.as_millis());
                            delay =
                                Duration::from_millis(next_delay_ms.try_into().unwrap_or(30_000));
                        }
                    }
                }
            },
        )
        .await
    {
        Ok(_) => {
            tracing::debug!("Outbox dispatcher startup is being retried in background if needed")
        }
        Err(err) => tracing::warn!(
            error = %err,
            "failed to spawn outbox dispatcher retry task; continuing with queued writes"
        ),
    }
}

/// RTT bounds for the adaptive outbox backoff window. The estimate itself
/// comes from the dispatcher's `RttEstimator` (T4/T7) — sampled from
/// successful worker→leader apply round-trips.
const BACKOFF_RTT_MIN_MS: i64 = 200;
const BACKOFF_RTT_MAX_MS: i64 = 3_000;
const BACKOFF_BASE_MIN_MS: i64 = 500;
const BACKOFF_BASE_MAX_MS: i64 = 3_000;
const BACKOFF_LOWER_FLOOR_MS: i64 = 250;
const BACKOFF_EXPONENT_CAP: u32 = 6;

/// `(lower, upper)` sleep window (ms) for an adaptive bounded outbox backoff.
///
/// Replaces the former 5 s linear floor, which was far too coarse for a ~200 ms
/// RTT lossy link: a transient apply error backed off a full 5 s before the
/// next attempt, starving status propagation. The window is RTT-aware and
/// bounded: `rtt = clamp(rtt_est, 200, 3000)`; `base = clamp(2*rtt, 500, 3000)`;
/// `upper = min(MAX_BACKOFF_MS, base * 2^min(attempt, 6))`;
/// `lower = min(upper, max(250, base/2))`.
fn adaptive_backoff_bounds(attempt: i64, rtt_est_ms: i64) -> (i64, i64) {
    let rtt = rtt_est_ms.clamp(BACKOFF_RTT_MIN_MS, BACKOFF_RTT_MAX_MS);
    let base = (2 * rtt).clamp(BACKOFF_BASE_MIN_MS, BACKOFF_BASE_MAX_MS);
    let exp = attempt.clamp(0, i64::from(BACKOFF_EXPONENT_CAP)).max(0) as u32;
    let upper = base.saturating_mul(1_i64 << exp).min(MAX_BACKOFF_MS);
    let lower = upper.min(BACKOFF_LOWER_FLOOR_MS.max(base / 2));
    (lower, upper)
}

#[cfg(feature = "test-support")]
pub const MAX_BACKOFF_MS_FOR_INTEGRATION_TEST: i64 = MAX_BACKOFF_MS;

#[cfg(feature = "test-support")]
pub const MAX_OUTBOX_ATTEMPTS_FOR_INTEGRATION_TEST: i64 = MAX_OUTBOX_ATTEMPTS;

#[cfg(feature = "test-support")]
pub fn adaptive_backoff_bounds_for_integration_test(attempt: i64, rtt_est_ms: i64) -> (i64, i64) {
    adaptive_backoff_bounds(attempt, rtt_est_ms)
}

#[cfg(feature = "test-support")]
pub fn adaptive_jittered_backoff_ms_for_integration_test(
    attempt: i64,
    idempotency_key: &str,
    rtt_est_ms: i64,
) -> i64 {
    adaptive_jittered_backoff_ms(attempt, idempotency_key, rtt_est_ms)
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn next_status_stamp_with_clock_for_integration_test(
    outbox: &Outbox,
    now_us: i64,
) -> Result<i64> {
    outbox.next_status_stamp_with_clock(now_us).await
}

/// Adaptive bounded backoff for a single outbox retry, deterministic in
/// `(idempotency_key, attempt, rtt_est_ms)` so the same failing row backs off
/// to the same instant across restarts — no RNG hot path. Picks a deterministic
/// point inside `[lower, upper]` via the existing FNV jitter.
fn adaptive_jittered_backoff_ms(attempt: i64, idempotency_key: &str, rtt_est_ms: i64) -> i64 {
    let (lower, upper) = adaptive_backoff_bounds(attempt, rtt_est_ms);
    let window = upper.saturating_sub(lower);
    lower.saturating_add(deterministic_jitter_ms(idempotency_key, attempt, window))
}

fn deterministic_jitter_ms(idempotency_key: &str, attempt: i64, window_ms: i64) -> i64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in idempotency_key
        .as_bytes()
        .iter()
        .copied()
        .chain(attempt.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % (window_ms.max(0) as u64 + 1)) as i64
}

fn actor_owned_pod_delete_needs_dead_letter(
    row: &OutboxRow,
    err: &klights_leader_api::OutboxDeliveryError,
) -> bool {
    use klights_leader_api::OutboxDeliveryError;

    if matches!(err, OutboxDeliveryError::NotFound(_)) {
        return false;
    }
    if !matches!(
        err,
        OutboxDeliveryError::UidMismatch { .. } | OutboxDeliveryError::ConflictTerminal(_)
    ) {
        return false;
    }
    row.is_terminal_pod_delete
}

fn wall_clock_epoch_ms(clock: &dyn klights_supervisor::WallClock) -> i64 {
    clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn instant_for_epoch_ms(epoch_ms: i64, now_epoch: i64) -> tokio::time::Instant {
    if epoch_ms <= now_epoch {
        tokio::time::Instant::now()
    } else {
        tokio::time::Instant::now()
            + Duration::from_millis(epoch_ms.saturating_sub(now_epoch) as u64)
    }
}
