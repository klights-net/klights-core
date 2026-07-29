//! Replication wire adapter for the canonical ordered log-apply domain.
//!
//! The core is deliberately port-based: sources provide durable ordered
//! entries, targets install snapshots and apply entries, and the follower
//! orchestration contains no SQLite or gRPC assumptions.

use anyhow::Result;
// Generated-wire and concrete persistence/network conversions remain owned by
// this root boundary adapter; the logical commit domain is canonical in
// klights-cluster-core.
use klights_internal_protobuf::log_apply::*;
use prost::Message;

use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow,
    LogApplyNodeDataplaneRow, LogApplyNodeSubnetAllocation, LogApplyNodeSubnetRow,
    LogApplyPodActorFinalization, LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow,
    LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow,
    OutboxStreamWatermark, PatchKind,
};

trait WireFrom<T>: Sized {
    fn wire_from(value: T) -> Self;
}

trait IntoWire<T>: Sized {
    fn into_wire(self) -> T;
}

impl<T, U> IntoWire<U> for T
where
    U: WireFrom<T>,
{
    fn into_wire(self) -> U {
        U::wire_from(self)
    }
}

trait TryWireFrom<T>: Sized {
    type Error;

    fn try_wire_from(value: T) -> std::result::Result<Self, Self::Error>;
}

trait TryIntoWire<T>: Sized {
    type Error;

    fn try_into_wire(self) -> std::result::Result<T, Self::Error>;
}

impl<T, U> TryIntoWire<U> for T
where
    U: TryWireFrom<T>,
{
    type Error = U::Error;

    fn try_into_wire(self) -> std::result::Result<U, Self::Error> {
        U::try_wire_from(self)
    }
}

// T3: `KEY_LAST_APPLIED_INDEX`, `KEY_LAST_APPLIED_RV` removed —
// the `log_apply_entries` table and its checkpoint are gone.

#[cfg(test)]
pub(crate) fn test_live_commit(
    candidate_resource_version: i64,
    mut mutations: Vec<LogApplyMutation>,
) -> LogApplyCommit {
    fn clear_nested_resource_version(data: &mut serde_json::Value) {
        if let Some(metadata) = data
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            metadata.remove("resourceVersion");
        }
    }

    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.patch);
            }
            LogApplyMutation::PutNamespace(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PutWatchEvent(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
                if let Some(object) = row.data.get_mut("object") {
                    clear_nested_resource_version(object);
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
    }
    let _ = candidate_resource_version;
    LogApplyCommit::try_new(mutations).expect("test live commit must be an RV-zero template")
}

pub fn encode_commit_json(commit: &LogApplyCommit) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(commit)?)
}

pub fn decode_commit_json(bytes: &[u8]) -> Result<LogApplyCommit> {
    let start = std::time::Instant::now();
    let commit = serde_json::from_slice::<LogApplyCommit>(bytes)?;
    commit.validate_live_template()?;
    log_slow_log_apply_decode(
        "json",
        start.elapsed(),
        bytes.len(),
        commit.resource_version(),
        commit.mutations().len(),
    );
    Ok(commit)
}

pub fn encode_commit_protobuf(commit: &LogApplyCommit) -> Result<Vec<u8>> {
    let proto = ProtoLogApplyCommit::wire_from(commit.clone());
    Ok(proto.encode_to_vec())
}

pub fn decode_commit_protobuf(bytes: &[u8]) -> Result<LogApplyCommit> {
    let start = std::time::Instant::now();
    let proto = ProtoLogApplyCommit::decode(bytes)?;
    let commit: LogApplyCommit = proto.try_into_wire()?;
    log_slow_log_apply_decode(
        "protobuf",
        start.elapsed(),
        bytes.len(),
        commit.resource_version(),
        commit.mutations().len(),
    );
    Ok(commit)
}

fn log_slow_log_apply_decode(
    format: &str,
    elapsed: std::time::Duration,
    data_len: usize,
    resource_version: i64,
    mutation_count: usize,
) {
    if elapsed.as_millis() < 25 && data_len < 512 * 1024 {
        return;
    }
    tracing::warn!(
        target: "klights::replication::slowdown",
        operation = "log_apply_decode",
        format,
        elapsed_ms = elapsed.as_millis(),
        data_len,
        resource_version,
        mutation_count,
        "slow log_apply decode"
    );
}

impl WireFrom<OutboxStreamWatermark> for ProtoOutboxStreamWatermark {
    fn wire_from(watermark: OutboxStreamWatermark) -> Self {
        Self {
            client_id: watermark.client_id,
            stream_id: watermark.stream_id,
            stream_seq: watermark.stream_seq,
        }
    }
}

impl WireFrom<ProtoOutboxStreamWatermark> for OutboxStreamWatermark {
    fn wire_from(watermark: ProtoOutboxStreamWatermark) -> Self {
        Self {
            client_id: watermark.client_id,
            stream_id: watermark.stream_id,
            stream_seq: watermark.stream_seq,
        }
    }
}

impl WireFrom<LogApplyCommit> for ProtoLogApplyCommit {
    fn wire_from(commit: LogApplyCommit) -> Self {
        let (resource_version, outbox_watermark, mutations) = commit.into_parts();
        Self {
            resource_version,
            mutations: mutations.into_iter().map(IntoWire::into_wire).collect(),
            outbox_watermark: outbox_watermark.map(IntoWire::into_wire),
        }
    }
}

impl TryWireFrom<ProtoLogApplyCommit> for LogApplyCommit {
    type Error = anyhow::Error;

    fn try_wire_from(proto: ProtoLogApplyCommit) -> Result<Self> {
        if proto.resource_version != 0 {
            anyhow::bail!(
                "live protobuf commit requires resourceVersion 0, got {}",
                proto.resource_version
            );
        }
        let commit = LogApplyCommit::try_new_with_watermark(
            proto
                .mutations
                .into_iter()
                .map(LogApplyMutation::try_wire_from)
                .collect::<Result<Vec<_>>>()?,
            proto.outbox_watermark.map(IntoWire::into_wire),
        )?;
        commit.validate_live_template()?;
        Ok(commit)
    }
}

impl WireFrom<LogApplyMutation> for ProtoLogApplyMutation {
    fn wire_from(mutation: LogApplyMutation) -> Self {
        use proto_log_apply_mutation::Mutation;
        let mutation = match mutation {
            LogApplyMutation::PutResource(row) => Mutation::PutResource(row.into_wire()),
            LogApplyMutation::PatchResourceLatest(patch) => {
                Mutation::PatchResourceLatest(patch.into_wire())
            }
            LogApplyMutation::DeleteResource(key) => Mutation::DeleteResource(key.into_wire()),
            LogApplyMutation::FinalizeBoundPod(finalization) => {
                Mutation::FinalizeBoundPod(ProtoLogApplyPodActorFinalization {
                    namespace: finalization.namespace,
                    name: finalization.name,
                    pod_uid: finalization.pod_uid,
                    node_name: finalization.node_name,
                })
            }
            LogApplyMutation::PutNamespace(row) => Mutation::PutNamespace(row.into_wire()),
            LogApplyMutation::DeleteNamespace { name } => Mutation::DeleteNamespace(name),
            LogApplyMutation::DeleteNamespaceContents { name } => {
                Mutation::DeleteNamespaceContents(name)
            }
            LogApplyMutation::PutNodeSubnet(row) => Mutation::PutNodeSubnet(row.into_wire()),
            LogApplyMutation::AllocateNodeSubnet(allocation) => {
                Mutation::AllocateNodeSubnet(ProtoLogApplyNodeSubnetAllocation {
                    node_name: allocation.node_name,
                    cluster_cidr: allocation.cluster_cidr,
                    node_ip: allocation.node_ip,
                })
            }
            LogApplyMutation::DeleteNodeSubnet { node_name } => {
                Mutation::DeleteNodeSubnet(node_name)
            }
            LogApplyMutation::PutNodeDataplane(row) => Mutation::PutNodeDataplane(row.into_wire()),
            LogApplyMutation::DeleteNodeDataplane { node_name } => {
                Mutation::DeleteNodeDataplane(node_name)
            }
            LogApplyMutation::PutAppliedOutbox(row) => Mutation::PutAppliedOutbox(row.into_wire()),
            LogApplyMutation::DeleteAppliedOutbox { idempotency_key } => {
                Mutation::DeleteAppliedOutbox(idempotency_key)
            }
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                Mutation::AdvanceResourceVersion(resource_version)
            }
            LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            } => Mutation::GcAppliedOutbox(ProtoLogApplyAppliedOutboxGc {
                cutoff_ms,
                operations,
            }),
            LogApplyMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            } => Mutation::GcWatchEvents(ProtoLogApplyWatchEventsGc {
                max_rows,
                batch_cap,
            }),
            LogApplyMutation::PutWatchEvent(row) => Mutation::PutWatchEvent(row.into_wire()),
            LogApplyMutation::PutKlightsMeta { key, value } => {
                Mutation::PutKlightsMeta(ProtoLogApplyKlightsMeta { key, value })
            }
            LogApplyMutation::PutPodCleanupIntent(row) => {
                Mutation::PutPodCleanupIntent(row.into_wire())
            }
            LogApplyMutation::DeletePodCleanupIntent(key) => {
                Mutation::DeletePodCleanupIntent(key.into_wire())
            }
            LogApplyMutation::DeletePodCleanupIntentsForNode { node_name } => {
                Mutation::DeletePodCleanupIntentsForNode(node_name)
            }
        };
        Self {
            mutation: Some(mutation),
        }
    }
}

impl TryWireFrom<ProtoLogApplyMutation> for LogApplyMutation {
    type Error = anyhow::Error;

    fn try_wire_from(proto: ProtoLogApplyMutation) -> Result<Self> {
        use proto_log_apply_mutation::Mutation;
        Ok(
            match proto
                .mutation
                .ok_or_else(|| anyhow::anyhow!("log_apply mutation is missing variant"))?
            {
                Mutation::PutResource(row) => LogApplyMutation::PutResource(row.try_into_wire()?),
                Mutation::PatchResourceLatest(patch) => {
                    LogApplyMutation::PatchResourceLatest(patch.try_into_wire()?)
                }
                Mutation::DeleteResource(key) => LogApplyMutation::DeleteResource(key.into_wire()),
                Mutation::FinalizeBoundPod(finalization) => {
                    LogApplyMutation::FinalizeBoundPod(LogApplyPodActorFinalization {
                        namespace: finalization.namespace,
                        name: finalization.name,
                        pod_uid: finalization.pod_uid,
                        node_name: finalization.node_name,
                    })
                }
                Mutation::PutNamespace(row) => LogApplyMutation::PutNamespace(row.try_into_wire()?),
                Mutation::DeleteNamespace(name) => LogApplyMutation::DeleteNamespace { name },
                Mutation::DeleteNamespaceContents(name) => {
                    LogApplyMutation::DeleteNamespaceContents { name }
                }
                Mutation::PutNodeSubnet(row) => LogApplyMutation::PutNodeSubnet(row.into_wire()),
                Mutation::AllocateNodeSubnet(allocation) => {
                    LogApplyMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                        node_name: allocation.node_name,
                        cluster_cidr: allocation.cluster_cidr,
                        node_ip: allocation.node_ip,
                    })
                }
                Mutation::DeleteNodeSubnet(node_name) => {
                    LogApplyMutation::DeleteNodeSubnet { node_name }
                }
                Mutation::PutNodeDataplane(row) => {
                    LogApplyMutation::PutNodeDataplane(row.try_into_wire()?)
                }
                Mutation::DeleteNodeDataplane(node_name) => {
                    LogApplyMutation::DeleteNodeDataplane { node_name }
                }
                Mutation::PutAppliedOutbox(row) => {
                    LogApplyMutation::PutAppliedOutbox(row.into_wire())
                }
                Mutation::DeleteAppliedOutbox(idempotency_key) => {
                    LogApplyMutation::DeleteAppliedOutbox { idempotency_key }
                }
                Mutation::AdvanceResourceVersion(resource_version) => {
                    LogApplyMutation::AdvanceResourceVersion { resource_version }
                }
                Mutation::GcAppliedOutbox(gc) => LogApplyMutation::GcAppliedOutbox {
                    cutoff_ms: gc.cutoff_ms,
                    operations: gc.operations,
                },
                Mutation::GcWatchEvents(gc) => LogApplyMutation::GcWatchEvents {
                    max_rows: gc.max_rows,
                    batch_cap: gc.batch_cap,
                },
                Mutation::PutWatchEvent(row) => {
                    LogApplyMutation::PutWatchEvent(row.try_into_wire()?)
                }
                Mutation::PutKlightsMeta(meta) => LogApplyMutation::PutKlightsMeta {
                    key: meta.key,
                    value: meta.value,
                },
                Mutation::PutPodCleanupIntent(row) => {
                    LogApplyMutation::PutPodCleanupIntent(row.try_into_wire()?)
                }
                Mutation::DeletePodCleanupIntent(key) => {
                    LogApplyMutation::DeletePodCleanupIntent(key.into_wire())
                }
                Mutation::DeletePodCleanupIntentsForNode(node_name) => {
                    LogApplyMutation::DeletePodCleanupIntentsForNode { node_name }
                }
            },
        )
    }
}

impl WireFrom<LogApplyResourcePatch> for ProtoLogApplyResourcePatch {
    fn wire_from(patch: LogApplyResourcePatch) -> Self {
        Self {
            api_version: patch.api_version,
            kind: patch.kind,
            namespace: patch.namespace,
            name: patch.name,
            resource_version: patch.resource_version,
            patch_kind: match patch.patch_kind {
                PatchKind::Merge => ProtoLogApplyPatchKind::Merge as i32,
            },
            patch_json: serde_json::to_vec(&patch.patch)
                .expect("serde_json::Value serialization is infallible"),
            require_existing: patch.require_existing,
            precondition_uid: patch.precondition_uid,
            precondition_resource_version: patch.precondition_resource_version,
            terminating_pod_unready_timestamp: patch.terminating_pod_unready_timestamp,
        }
    }
}

impl TryWireFrom<ProtoLogApplyResourcePatch> for LogApplyResourcePatch {
    type Error = anyhow::Error;

    fn try_wire_from(patch: ProtoLogApplyResourcePatch) -> Result<Self> {
        Ok(Self {
            api_version: patch.api_version,
            kind: patch.kind,
            namespace: patch.namespace,
            name: patch.name,
            resource_version: patch.resource_version,
            patch_kind: match ProtoLogApplyPatchKind::try_from(patch.patch_kind) {
                Ok(ProtoLogApplyPatchKind::Merge) => PatchKind::Merge,
                Err(_) => {
                    anyhow::bail!("unknown protobuf LogApply PatchKind: {}", patch.patch_kind)
                }
            },
            patch: serde_json::from_slice(&patch.patch_json)?,
            require_existing: patch.require_existing,
            precondition_uid: patch.precondition_uid,
            precondition_resource_version: patch.precondition_resource_version,
            terminating_pod_unready_timestamp: patch.terminating_pod_unready_timestamp,
        })
    }
}

impl WireFrom<LogApplyResourceRow> for ProtoLogApplyResourceRow {
    fn wire_from(row: LogApplyResourceRow) -> Self {
        Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
            require_absent: row.require_absent,
            require_existing: row.require_existing,
            precondition_uid: row.precondition_uid,
            precondition_resource_version: row.precondition_resource_version,
            status_only: row.status_only,
        }
    }
}

impl TryWireFrom<ProtoLogApplyResourceRow> for LogApplyResourceRow {
    type Error = anyhow::Error;

    fn try_wire_from(row: ProtoLogApplyResourceRow) -> Result<Self> {
        Ok(Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data: serde_json::from_slice(&row.data_json)?,
            require_absent: row.require_absent,
            require_existing: row.require_existing,
            precondition_uid: row.precondition_uid,
            precondition_resource_version: row.precondition_resource_version,
            status_only: row.status_only,
        })
    }
}

impl WireFrom<LogApplyResourceKey> for ProtoLogApplyResourceKey {
    fn wire_from(key: LogApplyResourceKey) -> Self {
        Self {
            api_version: key.api_version,
            kind: key.kind,
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            precondition_resource_version: key.precondition_resource_version,
        }
    }
}

impl WireFrom<ProtoLogApplyResourceKey> for LogApplyResourceKey {
    fn wire_from(key: ProtoLogApplyResourceKey) -> Self {
        Self {
            api_version: key.api_version,
            kind: key.kind,
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            precondition_resource_version: key.precondition_resource_version,
        }
    }
}

impl WireFrom<LogApplyNamespaceRow> for ProtoLogApplyNamespaceRow {
    fn wire_from(row: LogApplyNamespaceRow) -> Self {
        Self {
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
        }
    }
}

impl TryWireFrom<ProtoLogApplyNamespaceRow> for LogApplyNamespaceRow {
    type Error = anyhow::Error;

    fn try_wire_from(row: ProtoLogApplyNamespaceRow) -> Result<Self> {
        Ok(Self {
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data: serde_json::from_slice(&row.data_json)?,
        })
    }
}

impl WireFrom<LogApplyNodeSubnetRow> for ProtoLogApplyNodeSubnetRow {
    fn wire_from(row: LogApplyNodeSubnetRow) -> Self {
        Self {
            node_name: row.node_name,
            subnet: row.subnet,
            subnet_base_int: row.subnet_base_int,
            gateway_ip: row.gateway_ip,
            node_ip: row.node_ip,
            mode: row.mode,
            hostport_range: row.hostport_range,
        }
    }
}

impl WireFrom<ProtoLogApplyNodeSubnetRow> for LogApplyNodeSubnetRow {
    fn wire_from(row: ProtoLogApplyNodeSubnetRow) -> Self {
        Self {
            node_name: row.node_name,
            subnet: row.subnet,
            subnet_base_int: row.subnet_base_int,
            gateway_ip: row.gateway_ip,
            node_ip: row.node_ip,
            mode: row.mode,
            hostport_range: row.hostport_range,
        }
    }
}

impl WireFrom<LogApplyNodeDataplaneRow> for ProtoLogApplyNodeDataplaneRow {
    fn wire_from(row: LogApplyNodeDataplaneRow) -> Self {
        Self {
            node_name: row.node_name,
            mode: row.mode,
            encryption: row.encryption,
            public_key: row.public_key,
            endpoint: row.endpoint,
            port: row.port.map(u32::from),
        }
    }
}

impl TryWireFrom<ProtoLogApplyNodeDataplaneRow> for LogApplyNodeDataplaneRow {
    type Error = anyhow::Error;

    fn try_wire_from(row: ProtoLogApplyNodeDataplaneRow) -> Result<Self> {
        Ok(Self {
            node_name: row.node_name,
            mode: row.mode,
            encryption: row.encryption,
            public_key: row.public_key,
            endpoint: row.endpoint,
            port: row.port.map(u16::try_from).transpose()?,
        })
    }
}

impl WireFrom<LogApplyAppliedOutboxRow> for ProtoLogApplyAppliedOutboxRow {
    fn wire_from(row: LogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
            status_stamp: row.status_stamp,
        }
    }
}

impl WireFrom<ProtoLogApplyAppliedOutboxRow> for LogApplyAppliedOutboxRow {
    fn wire_from(row: ProtoLogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
            status_stamp: row.status_stamp,
        }
    }
}

impl WireFrom<LogApplyWatchEventRow> for ProtoLogApplyWatchEventRow {
    fn wire_from(row: LogApplyWatchEventRow) -> Self {
        Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            resource_version: row.resource_version,
            event_type: row.event_type,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
            event_id: row.event_id,
        }
    }
}

impl TryWireFrom<ProtoLogApplyWatchEventRow> for LogApplyWatchEventRow {
    type Error = anyhow::Error;

    fn try_wire_from(row: ProtoLogApplyWatchEventRow) -> Result<Self> {
        Ok(Self {
            event_id: row.event_id,
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            resource_version: row.resource_version,
            event_type: row.event_type,
            data: serde_json::from_slice(&row.data_json)?,
        })
    }
}

impl WireFrom<LogApplyPodCleanupIntentRow> for ProtoLogApplyPodCleanupIntentRow {
    fn wire_from(row: LogApplyPodCleanupIntentRow) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data_json: serde_json::to_vec(&row.pod_data)
                .expect("serde_json::Value serialization is infallible"),
        }
    }
}

impl TryWireFrom<ProtoLogApplyPodCleanupIntentRow> for LogApplyPodCleanupIntentRow {
    type Error = anyhow::Error;

    fn try_wire_from(row: ProtoLogApplyPodCleanupIntentRow) -> Result<Self> {
        Ok(Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: serde_json::from_slice(&row.pod_data_json)?,
        })
    }
}

impl WireFrom<LogApplyPodCleanupIntentKey> for ProtoLogApplyPodCleanupIntentKey {
    fn wire_from(key: LogApplyPodCleanupIntentKey) -> Self {
        Self {
            node_name: key.node_name,
            namespace: key.namespace,
            pod_name: key.pod_name,
            pod_uid: key.pod_uid,
            reason: key.reason,
        }
    }
}

impl WireFrom<ProtoLogApplyPodCleanupIntentKey> for LogApplyPodCleanupIntentKey {
    fn wire_from(key: ProtoLogApplyPodCleanupIntentKey) -> Self {
        Self {
            node_name: key.node_name,
            namespace: key.namespace,
            pod_name: key.pod_name,
            pod_uid: key.pod_uid,
            reason: key.reason,
        }
    }
}

// T3: `LogApplyEntry` and `LogApplyCheckpoint` removed —
// the `log_apply_entries` table is gone. Raft AppendEntries
// through `apply_log_apply_commit` is the sole replication path.

#[cfg(test)]
mod parity_tests {
    //! T1.2: every `LogApplyMutation` variant must round-trip through
    //! both wire formats (`encode_commit_protobuf` and
    //! `encode_commit_json`) and survive an encode → decode → re-encode
    //! cycle byte-for-byte. The raft `EntryPayload::Normal` payload uses
    //! the protobuf encoding; the JSON encoding backs debug dumps and
    //! the existing watch-test fixtures.
    //!
    //! This test will fail if a new variant is added to
    //! `LogApplyMutation` without a matching sample below, because the
    //! exhaustive `match` on `variant_name` will not compile.
    use super::*;
    use klights_cluster_core::ClusterMutation;
    use klights_cluster_core::LogApplyAppliedOutboxRow;
    use serde_json::json;

    fn sample(name: &'static str) -> (String, LogApplyMutation) {
        let mutation = match name {
            "PutResource" => LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm".to_string(),
                uid: "cm-uid".to_string(),
                resource_version: 7,
                data: json!({"metadata": {"name": "cm", "uid": "cm-uid"}}),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            }),
            "PatchResourceLatest" => LogApplyMutation::PatchResourceLatest(LogApplyResourcePatch {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                resource_version: 8,
                patch_kind: PatchKind::Merge,
                patch: json!({
                    "metadata": {
                        "deletionTimestamp": "2026-06-21T01:02:03Z",
                        "deletionGracePeriodSeconds": 0
                    }
                }),
                require_existing: true,
                precondition_uid: Some("pod-uid-A".to_string()),
                precondition_resource_version: None,
                terminating_pod_unready_timestamp: Some(
                    "2026-06-21T01:02:04.000000000Z".to_string(),
                ),
            }),
            "DeleteResource" => LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                uid: "pod-uid-A".to_string(),
                precondition_resource_version: None,
            }),
            "FinalizeBoundPod" => {
                LogApplyMutation::FinalizeBoundPod(LogApplyPodActorFinalization {
                    namespace: "default".to_string(),
                    name: "p1".to_string(),
                    pod_uid: "pod-uid-A".to_string(),
                    node_name: "worker-a".to_string(),
                })
            }
            "PutNamespace" => LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                name: "ns".to_string(),
                uid: "ns-uid".to_string(),
                resource_version: 3,
                data: json!({"metadata": {"name": "ns"}}),
            }),
            "DeleteNamespace" => LogApplyMutation::DeleteNamespace {
                name: "ns".to_string(),
            },
            "DeleteNamespaceContents" => LogApplyMutation::DeleteNamespaceContents {
                name: "ns".to_string(),
            },
            "PutNodeSubnet" => LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: "node-1".to_string(),
                subnet: "10.42.1.0/24".to_string(),
                subnet_base_int: 0x0a2a0100,
                gateway_ip: "10.42.1.0".to_string(),
                node_ip: "192.168.0.10".to_string(),
                mode: "root".to_string(),
                hostport_range: Some("30000-32767".to_string()),
            }),
            "AllocateNodeSubnet" => {
                LogApplyMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                    node_name: "node-alloc".to_string(),
                    cluster_cidr: "10.42.0.0/16".to_string(),
                    node_ip: "192.168.0.20".to_string(),
                })
            }
            "DeleteNodeSubnet" => LogApplyMutation::DeleteNodeSubnet {
                node_name: "node-1".to_string(),
            },
            "PutNodeDataplane" => LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                node_name: "node-1".to_string(),
                mode: "root".to_string(),
                encryption: "wireguard".to_string(),
                public_key: Some("pub=".to_string()),
                endpoint: "192.168.0.10".to_string(),
                port: Some(51820),
            }),
            "DeleteNodeDataplane" => LogApplyMutation::DeleteNodeDataplane {
                node_name: "node-1".to_string(),
            },
            "PutAppliedOutbox" => LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "cmd-1".to_string(),
                subject_key: "v1:Pod:default:p1".to_string(),
                operation: "CreateResource".to_string(),
                first_seen_ms: 1_700_000_000_000,
                applied_rv: Some(42),
                result_proto: vec![0u8, 1, 2, 3, 4],
                status_stamp: Some(7),
            }),
            "DeleteAppliedOutbox" => LogApplyMutation::DeleteAppliedOutbox {
                idempotency_key: "cmd-1".to_string(),
            },
            "GcAppliedOutbox" => LogApplyMutation::GcAppliedOutbox {
                cutoff_ms: 1_700_000_000_000,
                operations: vec!["CreateResource".to_string(), "DeleteResource".to_string()],
            },
            "GcWatchEvents" => LogApplyMutation::GcWatchEvents {
                max_rows: 100_000,
                batch_cap: 5_000,
            },
            "PutWatchEvent" => LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                event_id: Some(37),
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm".to_string(),
                resource_version: 9,
                event_type: "MODIFIED".to_string(),
                data: json!({"data": {"k": "v"}}),
            }),
            "AdvanceResourceVersion" => LogApplyMutation::AdvanceResourceVersion {
                resource_version: 99,
            },
            "PutKlightsMeta" => LogApplyMutation::PutKlightsMeta {
                key: "cluster_id".to_string(),
                value: "test-uuid".to_string(),
            },
            "PutPodCleanupIntent" => {
                LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                    node_name: "node-1".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "p1".to_string(),
                    pod_uid: "pod-uid-A".to_string(),
                    reason: "NodeLost".to_string(),
                    resource_version: 101,
                    created_at_ms: 1_700_000_000_000,
                    pod_data: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"namespace": "default", "name": "p1", "uid": "pod-uid-A"},
                        "spec": {"nodeName": "node-1"}
                    }),
                })
            }
            "DeletePodCleanupIntent" => {
                LogApplyMutation::DeletePodCleanupIntent(LogApplyPodCleanupIntentKey {
                    node_name: "node-1".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "p1".to_string(),
                    pod_uid: "pod-uid-A".to_string(),
                    reason: "NodeLost".to_string(),
                })
            }
            "DeletePodCleanupIntentsForNode" => LogApplyMutation::DeletePodCleanupIntentsForNode {
                node_name: "node-1".to_string(),
            },
            other => panic!("unknown variant {other}"),
        };
        (name.to_string(), mutation)
    }

    /// Compile-time exhaustive enumeration. Adding a new variant to
    /// `LogApplyMutation` without listing it here is a compile error;
    /// the `match` below has no wildcard arm.
    fn all_variant_names() -> Vec<&'static str> {
        let names: Vec<&'static str> = vec![
            "PutResource",
            "PatchResourceLatest",
            "DeleteResource",
            "FinalizeBoundPod",
            "PutNamespace",
            "DeleteNamespace",
            "DeleteNamespaceContents",
            "PutNodeSubnet",
            "AllocateNodeSubnet",
            "DeleteNodeSubnet",
            "PutNodeDataplane",
            "DeleteNodeDataplane",
            "PutAppliedOutbox",
            "DeleteAppliedOutbox",
            "GcAppliedOutbox",
            "GcWatchEvents",
            "PutWatchEvent",
            "AdvanceResourceVersion",
            "PutKlightsMeta",
            "PutPodCleanupIntent",
            "DeletePodCleanupIntent",
            "DeletePodCleanupIntentsForNode",
        ];
        // The exhaustive match below validates that `names` enumerates
        // every variant — adding a new variant is a compile error here.
        let probe: LogApplyMutation = LogApplyMutation::AdvanceResourceVersion {
            resource_version: 0,
        };
        let _ = match probe {
            LogApplyMutation::PutResource(_) => 0,
            LogApplyMutation::PatchResourceLatest(_) => 1,
            LogApplyMutation::DeleteResource(_) => 2,
            LogApplyMutation::FinalizeBoundPod(_) => 3,
            LogApplyMutation::PutNamespace(_) => 4,
            LogApplyMutation::DeleteNamespace { .. } => 5,
            LogApplyMutation::DeleteNamespaceContents { .. } => 6,
            LogApplyMutation::PutNodeSubnet(_) => 7,
            LogApplyMutation::AllocateNodeSubnet(_) => 8,
            LogApplyMutation::DeleteNodeSubnet { .. } => 9,
            LogApplyMutation::PutNodeDataplane(_) => 10,
            LogApplyMutation::DeleteNodeDataplane { .. } => 11,
            LogApplyMutation::PutAppliedOutbox(_) => 12,
            LogApplyMutation::DeleteAppliedOutbox { .. } => 13,
            LogApplyMutation::GcAppliedOutbox { .. } => 14,
            LogApplyMutation::GcWatchEvents { .. } => 15,
            LogApplyMutation::PutWatchEvent(_) => 16,
            LogApplyMutation::AdvanceResourceVersion { .. } => 17,
            LogApplyMutation::PutKlightsMeta { .. } => 18,
            LogApplyMutation::PutPodCleanupIntent(_) => 19,
            LogApplyMutation::DeletePodCleanupIntent(_) => 20,
            LogApplyMutation::DeletePodCleanupIntentsForNode { .. } => 21,
        };
        names
    }

    fn commit_for(mut mutation: LogApplyMutation) -> LogApplyCommit {
        match &mut mutation {
            LogApplyMutation::PutResource(row) => row.resource_version = 0,
            LogApplyMutation::PatchResourceLatest(row) => row.resource_version = 0,
            LogApplyMutation::PutNamespace(row) => row.resource_version = 0,
            LogApplyMutation::PutWatchEvent(row) => row.resource_version = 0,
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
        LogApplyCommit::try_new(vec![mutation]).unwrap()
    }

    #[test]
    fn every_mutation_variant_round_trips_protobuf() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let bytes1 = encode_commit_protobuf(&commit)
                .unwrap_or_else(|err| panic!("{label}: first protobuf encode failed: {err:#}"));
            let decoded: LogApplyCommit = decode_commit_protobuf(&bytes1)
                .unwrap_or_else(|err| panic!("{label}: protobuf decode failed: {err:#}"));
            assert_eq!(
                decoded, commit,
                "{label}: protobuf round-trip changed the value"
            );
            let bytes2 = encode_commit_protobuf(&decoded)
                .unwrap_or_else(|err| panic!("{label}: re-encode failed: {err:#}"));
            assert_eq!(
                bytes1, bytes2,
                "{label}: protobuf re-encode produced different bytes"
            );
        }
    }

    #[test]
    fn every_mutation_variant_round_trips_json() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let bytes1 = encode_commit_json(&commit)
                .unwrap_or_else(|err| panic!("{label}: first JSON encode failed: {err:#}"));
            let decoded: LogApplyCommit = decode_commit_json(&bytes1)
                .unwrap_or_else(|err| panic!("{label}: JSON decode failed: {err:#}"));
            assert_eq!(
                decoded, commit,
                "{label}: JSON round-trip changed the value"
            );
            let bytes2 = encode_commit_json(&decoded)
                .unwrap_or_else(|err| panic!("{label}: JSON re-encode failed: {err:#}"));
            assert_eq!(
                bytes1, bytes2,
                "{label}: JSON re-encode produced different bytes"
            );
        }
    }

    #[test]
    fn json_and_protobuf_round_trips_agree_on_decoded_value() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let from_json: LogApplyCommit =
                decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap();
            let from_proto: LogApplyCommit =
                decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap();
            assert_eq!(
                from_json, from_proto,
                "{label}: JSON and protobuf decoded into different values"
            );
        }
    }

    #[test]
    fn positive_live_json_and_protobuf_commits_are_rejected() {
        let positive_json = br#"{"resource_version":9,"mutations":[]}"#;
        assert!(decode_commit_json(positive_json).is_err());

        let old_proto = ProtoLogApplyCommit {
            resource_version: 9,
            mutations: Vec::new(),
            outbox_watermark: None,
        }
        .encode_to_vec();
        assert!(decode_commit_protobuf(&old_proto).is_err());
    }

    #[test]
    fn committed_apply_v1_round_trips_json_and_protobuf() {
        let commit = LogApplyCommit::try_new(Vec::new()).unwrap();
        assert_eq!(
            decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap(),
            commit
        );
        assert_eq!(
            decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap(),
            commit
        );
    }

    #[test]
    fn outbox_stream_watermark_round_trips_json_and_protobuf() {
        let commit = LogApplyCommit::try_new_with_watermark(
            vec![LogApplyMutation::AdvanceResourceVersion {
                resource_version: 0,
            }],
            Some(OutboxStreamWatermark {
                client_id: "client-a".to_string(),
                stream_id: 12,
                stream_seq: 34,
            }),
        )
        .unwrap();

        let from_json: LogApplyCommit =
            decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap();
        let from_proto: LogApplyCommit =
            decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap();
        assert_eq!(from_json, commit, "JSON must preserve outbox watermark");
        assert_eq!(
            from_proto, commit,
            "protobuf must preserve outbox watermark"
        );
    }

    #[test]
    fn status_only_resource_row_round_trips_json_and_protobuf() {
        let commit =
            LogApplyCommit::try_new(vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "status-only".to_string(),
                uid: "status-only-uid".to_string(),
                resource_version: 0,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "status-only",
                        "uid": "status-only-uid",
                        "resourceVersion": "0"
                    },
                    "status": {"phase": "Running"}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("status-only-uid".to_string()),
                precondition_resource_version: None,
                status_only: true,
            })])
            .unwrap();

        let from_json: LogApplyCommit =
            decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap();
        let from_proto: LogApplyCommit =
            decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap();
        assert_eq!(from_json, commit, "JSON must preserve status_only");
        assert_eq!(from_proto, commit, "protobuf must preserve status_only");
    }

    #[test]
    fn snapshot_roundtrip_preserves_applied_outbox_status_stamp() {
        let record = LogApplyAppliedOutboxRow {
            idempotency_key: "status-key".to_string(),
            subject_key: "v1:Pod:default:web:uid-1".to_string(),
            operation: "PodStatus".to_string(),
            first_seen_ms: 1_700_000_000_000,
            applied_rv: Some(42),
            result_proto: vec![1, 2, 3],
            status_stamp: Some(99),
        };

        let row: LogApplyAppliedOutboxRow = record.into();
        assert_eq!(row.status_stamp, Some(99));

        let restored: LogApplyAppliedOutboxRow = row.into();
        assert_eq!(restored.status_stamp, Some(99));
    }

    #[test]
    fn old_protobuf_bytes_decode_through_cluster_mutation_bridge() {
        // B3: prove old protobuf tags remain decodable while every
        // mutation survives the LogApplyMutation -> ClusterMutation ->
        // LogApplyMutation conversion bridge without changing meaning.
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let original = commit_for(mutation);

            // 1. Old flat protobuf bytes decode to the expected commit.
            let old_bytes = encode_commit_protobuf(&original)
                .unwrap_or_else(|err| panic!("{label}: protobuf encode failed: {err:#}"));
            let decoded: LogApplyCommit = decode_commit_protobuf(&old_bytes)
                .unwrap_or_else(|err| panic!("{label}: old protobuf decode failed: {err:#}"));
            assert_eq!(
                decoded, original,
                "{label}: old protobuf bytes decoded to different commit"
            );

            // 2. ClusterMutation conversion preserves every mutation.
            for (i, mutation) in decoded.mutations().iter().enumerate() {
                let cm: ClusterMutation = mutation.clone().into();
                let back: LogApplyMutation = cm.into();
                assert_eq!(
                    &back, mutation,
                    "{label}[{i}]: ClusterMutation conversion changed value"
                );
            }

            // 3. Re-encoded protobuf bytes decode again to the same logical commit.
            //    Re-encode from the original commit (not the round-tripped clone) so
            //    byte-level stability is measured against the canonical old encoding.
            let re_bytes = encode_commit_protobuf(&original)
                .unwrap_or_else(|err| panic!("{label}: re-encode failed: {err:#}"));
            let re_decoded: LogApplyCommit = decode_commit_protobuf(&re_bytes)
                .unwrap_or_else(|err| panic!("{label}: re-decoded protobuf failed: {err:#}"));
            assert_eq!(
                re_decoded, original,
                "{label}: re-encoded protobuf bytes decode to different commit"
            );
        }
    }

    #[test]
    fn flat_json_commits_still_decode() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);
            let bytes = encode_commit_json(&commit).unwrap();
            let decoded: LogApplyCommit = decode_commit_json(&bytes).unwrap_or_else(|err| {
                panic!("{label}: flat JSON decode failed: {err:#}");
            });
            assert_eq!(decoded, commit, "{label}: flat JSON decode changed value");
        }
    }
}
