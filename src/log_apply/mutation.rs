use anyhow;
/// Data-only family model: ClusterMutation wraps LogApplyMutation variants
/// into tagged family enums (Resource, Namespace, WatchHistory, Network,
/// OutboxLedger, ClusterMeta, PodCleanup) so consumers that only care about
/// a specific family can destructure without branching on unrelated variants.
///
/// The From/TryFrom conversions are infallible and never drop fields.
use serde::{Deserialize, Serialize};

use super::*;

/// Versioned envelope for ClusterMutation.
/// Every durable mutation carries an explicit version so decoders can reject
/// unknown future formats instead of silently misinterpreting them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionedClusterMutation {
    pub version: u32,
    pub mutation: ClusterMutation,
}

impl VersionedClusterMutation {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(mutation: ClusterMutation) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            mutation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResourceMutation {
    PutResource(LogApplyResourceRow),
    PatchResourceLatest(LogApplyResourcePatch),
    DeleteResource(LogApplyResourceKey),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NamespaceMutation {
    PutNamespace(LogApplyNamespaceRow),
    DeleteNamespace { name: String },
    DeleteNamespaceContents { name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WatchHistoryMutation {
    PutWatchEvent(LogApplyWatchEventRow),
    GcWatchEvents { max_rows: i64, batch_cap: i64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NetworkMutation {
    PutNodeSubnet(LogApplyNodeSubnetRow),
    AllocateNodeSubnet(LogApplyNodeSubnetAllocation),
    DeleteNodeSubnet { node_name: String },
    PutNodeDataplane(LogApplyNodeDataplaneRow),
    DeleteNodeDataplane { node_name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OutboxLedgerMutation {
    PutAppliedOutbox(LogApplyAppliedOutboxRow),
    DeleteAppliedOutbox {
        idempotency_key: String,
    },
    GcAppliedOutbox {
        cutoff_ms: i64,
        operations: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterMetaMutation {
    AdvanceResourceVersion { resource_version: i64 },
    PutKlightsMeta { key: String, value: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PodCleanupMutation {
    PutPodCleanupIntent(LogApplyPodCleanupIntentRow),
    DeletePodCleanupIntent(LogApplyPodCleanupIntentKey),
    DeletePodCleanupIntentsForNode { node_name: String },
}

/// Tagged enum with `#[serde(tag = "family", content = "mutation")]`.
/// Every LogApplyMutation variant round-trips through this enum without loss.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", content = "mutation")]
pub enum ClusterMutation {
    Resource(ResourceMutation),
    Namespace(NamespaceMutation),
    WatchHistory(WatchHistoryMutation),
    Network(NetworkMutation),
    OutboxLedger(OutboxLedgerMutation),
    ClusterMeta(ClusterMetaMutation),
    PodCleanup(PodCleanupMutation),
}

impl From<LogApplyMutation> for ClusterMutation {
    fn from(m: LogApplyMutation) -> Self {
        match m {
            LogApplyMutation::PutResource(v) => {
                ClusterMutation::Resource(ResourceMutation::PutResource(v))
            }
            LogApplyMutation::PatchResourceLatest(v) => {
                ClusterMutation::Resource(ResourceMutation::PatchResourceLatest(v))
            }
            LogApplyMutation::DeleteResource(v) => {
                ClusterMutation::Resource(ResourceMutation::DeleteResource(v))
            }
            LogApplyMutation::PutNamespace(v) => {
                ClusterMutation::Namespace(NamespaceMutation::PutNamespace(v))
            }
            LogApplyMutation::DeleteNamespace { name } => {
                ClusterMutation::Namespace(NamespaceMutation::DeleteNamespace { name })
            }
            LogApplyMutation::DeleteNamespaceContents { name } => {
                ClusterMutation::Namespace(NamespaceMutation::DeleteNamespaceContents { name })
            }
            LogApplyMutation::PutWatchEvent(v) => {
                ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(v))
            }
            LogApplyMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            } => ClusterMutation::WatchHistory(WatchHistoryMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            }),
            LogApplyMutation::PutNodeSubnet(v) => {
                ClusterMutation::Network(NetworkMutation::PutNodeSubnet(v))
            }
            LogApplyMutation::AllocateNodeSubnet(v) => {
                ClusterMutation::Network(NetworkMutation::AllocateNodeSubnet(v))
            }
            LogApplyMutation::DeleteNodeSubnet { node_name } => {
                ClusterMutation::Network(NetworkMutation::DeleteNodeSubnet { node_name })
            }
            LogApplyMutation::PutNodeDataplane(v) => {
                ClusterMutation::Network(NetworkMutation::PutNodeDataplane(v))
            }
            LogApplyMutation::DeleteNodeDataplane { node_name } => {
                ClusterMutation::Network(NetworkMutation::DeleteNodeDataplane { node_name })
            }
            LogApplyMutation::PutAppliedOutbox(v) => {
                ClusterMutation::OutboxLedger(OutboxLedgerMutation::PutAppliedOutbox(v))
            }
            LogApplyMutation::DeleteAppliedOutbox { idempotency_key } => {
                ClusterMutation::OutboxLedger(OutboxLedgerMutation::DeleteAppliedOutbox {
                    idempotency_key,
                })
            }
            LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            } => ClusterMutation::OutboxLedger(OutboxLedgerMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            }),
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                ClusterMutation::ClusterMeta(ClusterMetaMutation::AdvanceResourceVersion {
                    resource_version,
                })
            }
            LogApplyMutation::PutKlightsMeta { key, value } => {
                ClusterMutation::ClusterMeta(ClusterMetaMutation::PutKlightsMeta { key, value })
            }
            LogApplyMutation::PutPodCleanupIntent(v) => {
                ClusterMutation::PodCleanup(PodCleanupMutation::PutPodCleanupIntent(v))
            }
            LogApplyMutation::DeletePodCleanupIntent(v) => {
                ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntent(v))
            }
            LogApplyMutation::DeletePodCleanupIntentsForNode { node_name } => {
                ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntentsForNode {
                    node_name,
                })
            }
        }
    }
}

impl ClusterMutation {
    pub fn into_log_apply_mutation(self) -> LogApplyMutation {
        self.into()
    }
}

impl From<ClusterMutation> for LogApplyMutation {
    fn from(cm: ClusterMutation) -> Self {
        match cm {
            ClusterMutation::Resource(ResourceMutation::PutResource(v)) => {
                LogApplyMutation::PutResource(v)
            }
            ClusterMutation::Resource(ResourceMutation::PatchResourceLatest(v)) => {
                LogApplyMutation::PatchResourceLatest(v)
            }
            ClusterMutation::Resource(ResourceMutation::DeleteResource(v)) => {
                LogApplyMutation::DeleteResource(v)
            }
            ClusterMutation::Namespace(NamespaceMutation::PutNamespace(v)) => {
                LogApplyMutation::PutNamespace(v)
            }
            ClusterMutation::Namespace(NamespaceMutation::DeleteNamespace { name }) => {
                LogApplyMutation::DeleteNamespace { name }
            }
            ClusterMutation::Namespace(NamespaceMutation::DeleteNamespaceContents { name }) => {
                LogApplyMutation::DeleteNamespaceContents { name }
            }
            ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(v)) => {
                LogApplyMutation::PutWatchEvent(v)
            }
            ClusterMutation::WatchHistory(WatchHistoryMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            }) => LogApplyMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            },
            ClusterMutation::Network(NetworkMutation::PutNodeSubnet(v)) => {
                LogApplyMutation::PutNodeSubnet(v)
            }
            ClusterMutation::Network(NetworkMutation::AllocateNodeSubnet(v)) => {
                LogApplyMutation::AllocateNodeSubnet(v)
            }
            ClusterMutation::Network(NetworkMutation::DeleteNodeSubnet { node_name }) => {
                LogApplyMutation::DeleteNodeSubnet { node_name }
            }
            ClusterMutation::Network(NetworkMutation::PutNodeDataplane(v)) => {
                LogApplyMutation::PutNodeDataplane(v)
            }
            ClusterMutation::Network(NetworkMutation::DeleteNodeDataplane { node_name }) => {
                LogApplyMutation::DeleteNodeDataplane { node_name }
            }
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::PutAppliedOutbox(v)) => {
                LogApplyMutation::PutAppliedOutbox(v)
            }
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::DeleteAppliedOutbox {
                idempotency_key,
            }) => LogApplyMutation::DeleteAppliedOutbox { idempotency_key },
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            }) => LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            },
            ClusterMutation::ClusterMeta(ClusterMetaMutation::AdvanceResourceVersion {
                resource_version,
            }) => LogApplyMutation::AdvanceResourceVersion { resource_version },
            ClusterMutation::ClusterMeta(ClusterMetaMutation::PutKlightsMeta { key, value }) => {
                LogApplyMutation::PutKlightsMeta { key, value }
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::PutPodCleanupIntent(v)) => {
                LogApplyMutation::PutPodCleanupIntent(v)
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntent(v)) => {
                LogApplyMutation::DeletePodCleanupIntent(v)
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntentsForNode {
                node_name,
            }) => LogApplyMutation::DeletePodCleanupIntentsForNode { node_name },
        }
    }
}

impl TryFrom<VersionedClusterMutation> for LogApplyMutation {
    type Error = anyhow::Error;

    fn try_from(value: VersionedClusterMutation) -> Result<Self, Self::Error> {
        if value.version != VersionedClusterMutation::CURRENT_VERSION {
            anyhow::bail!(
                "unsupported ClusterMutation version {} (current {})",
                value.version,
                VersionedClusterMutation::CURRENT_VERSION
            );
        }
        Ok(value.mutation.into())
    }
}
