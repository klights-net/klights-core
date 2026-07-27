#[cfg(test)]
use crate::replication::storage_wire_codec::{
    decode_outbox_payload_protobuf, encode_outbox_payload_protobuf,
};
#[cfg(test)]
use anyhow::Result;
use klights_cluster_core::{ResourcePreconditions, StorageCommand};

pub use klights_cluster_core::OutboxOperation;

/// Persisted scheduling class for node-local outbox rows. Lower values are
/// more urgent; the stable integer representation is part of node.db schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum OutboxPriorityClass {
    Lease = 0,
    NodeHealth = 1,
    Workload = 2,
    Diagnostic = 3,
}

impl OutboxPriorityClass {
    pub const fn persisted_value(self) -> i64 {
        self as i64
    }
}

/// Once diagnostic work reaches this age it joins the workload scheduling
/// class, bounding starvation without delaying leader-health traffic.
pub const OUTBOX_DIAGNOSTIC_AGING_MS: i64 = 30_000;

pub trait OutboxOperationExt: Sized {
    fn priority_class(self) -> OutboxPriorityClass;

    fn supersedable_pod_status(self) -> bool;

    /// Classify an outbound command before its payload becomes opaque to
    /// node-local persistence.
    fn classification(
        self,
        command: &StorageCommand,
    ) -> std::result::Result<
        klights_node_store::OutboxClassification,
        klights_node_store::DeliveryError,
    >;

    fn try_delivery_operation(
        self,
    ) -> std::result::Result<
        klights_leader_api::OutboxDeliveryOperation,
        klights_leader_api::OutboxDeliveryError,
    >;
}

impl OutboxOperationExt for OutboxOperation {
    fn priority_class(self) -> OutboxPriorityClass {
        match self.priority() {
            klights_cluster_core::OutboxPriority::Lease => OutboxPriorityClass::Lease,
            klights_cluster_core::OutboxPriority::NodeHealth => OutboxPriorityClass::NodeHealth,
            klights_cluster_core::OutboxPriority::Workload => OutboxPriorityClass::Workload,
            klights_cluster_core::OutboxPriority::Diagnostic => OutboxPriorityClass::Diagnostic,
        }
    }

    fn supersedable_pod_status(self) -> bool {
        self.is_supersedable_pod_status()
    }

    fn classification(
        self,
        command: &StorageCommand,
    ) -> std::result::Result<
        klights_node_store::OutboxClassification,
        klights_node_store::DeliveryError,
    > {
        use klights_node_store::{
            OutboxClassification, OutboxPriority, OutboxSequencePolicy, OutboxSupersedability,
            TerminalDeleteClassification,
        };

        let priority = match self.priority() {
            klights_cluster_core::OutboxPriority::Lease => OutboxPriority::Lease,
            klights_cluster_core::OutboxPriority::NodeHealth => OutboxPriority::NodeHealth,
            klights_cluster_core::OutboxPriority::Workload => OutboxPriority::Workload,
            klights_cluster_core::OutboxPriority::Diagnostic => OutboxPriority::Diagnostic,
        };
        let terminal_delete = if matches!(command, StorageCommand::FinalizeBoundPod { .. }) {
            TerminalDeleteClassification::ActorOwnedPodDelete
        } else {
            TerminalDeleteClassification::NotTerminalDelete
        };
        let supersedability = if self.supersedable_pod_status()
            && terminal_delete == TerminalDeleteClassification::NotTerminalDelete
        {
            OutboxSupersedability::PodStatus
        } else {
            OutboxSupersedability::Never
        };
        let sequence_policy = if self.uses_per_subject_sequence() {
            OutboxSequencePolicy::PerSubject
        } else {
            OutboxSequencePolicy::Unsequenced
        };

        OutboxClassification::try_new(priority, supersedability, terminal_delete, sequence_policy)
    }

    /// Convert the node-local scheduling classification into the public,
    /// transport-neutral delivery operation. Lease renewal intentionally has
    /// no durable-delivery representation: it owns a separate authenticated
    /// leader capability.
    fn try_delivery_operation(
        self,
    ) -> std::result::Result<
        klights_leader_api::OutboxDeliveryOperation,
        klights_leader_api::OutboxDeliveryError,
    > {
        klights_leader_api::OutboxDeliveryOperation::try_from(self)
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    #[test]
    fn operation_owns_persisted_priority_and_supersedable_semantics() {
        assert_eq!(
            OutboxOperation::LeaseRenew
                .priority_class()
                .persisted_value(),
            0
        );
        assert_eq!(
            OutboxOperation::NodeStatus
                .priority_class()
                .persisted_value(),
            1
        );
        assert_eq!(
            OutboxOperation::PodStatus
                .priority_class()
                .persisted_value(),
            2
        );
        assert_eq!(
            OutboxOperation::EventCreate
                .priority_class()
                .persisted_value(),
            3
        );

        for operation in [
            OutboxOperation::PodStatus,
            OutboxOperation::RuntimeReconcile,
            OutboxOperation::ProbeReadiness,
            OutboxOperation::DeadlineExceeded,
            OutboxOperation::ContainerStatusSnapshot,
            OutboxOperation::EphemeralContainerStatuses,
        ] {
            assert!(operation.supersedable_pod_status(), "{operation}");
        }
        for operation in [
            OutboxOperation::PodMetadata,
            OutboxOperation::NodeRegistration,
            OutboxOperation::NodeDataplane,
            OutboxOperation::NodeStatus,
            OutboxOperation::LeaseRenew,
            OutboxOperation::EventCreate,
        ] {
            assert!(!operation.supersedable_pod_status(), "{operation}");
        }
    }

    #[test]
    fn durable_delivery_conversion_is_exhaustive_and_rejects_lease_renewal() {
        for operation in OutboxOperation::ALL {
            if operation == OutboxOperation::LeaseRenew {
                assert!(
                    matches!(
                        operation.try_delivery_operation(),
                        Err(klights_leader_api::OutboxDeliveryError::InvalidRequest { .. })
                    ),
                    "lease renewal must remain on its focused renewal capability"
                );
                continue;
            }

            let delivery = operation
                .try_delivery_operation()
                .expect("every durable queue operation has one neutral delivery operation");
            assert_eq!(delivery.as_wire_name(), operation.as_str());
            assert_eq!(OutboxOperation::from(delivery), operation);
        }
    }

    #[test]
    fn producer_classification_is_explicit_before_payload_encoding() {
        use klights_node_store::{
            OutboxPriority, OutboxSequencePolicy, OutboxSupersedability,
            TerminalDeleteClassification,
        };

        let ordinary = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions::default(),
            observed_status_stamp: None,
        };
        let pod_status = OutboxOperation::PodStatus
            .classification(&ordinary)
            .expect("valid Pod status classification");
        assert_eq!(pod_status.priority(), OutboxPriority::Workload);
        assert_eq!(
            pod_status.supersedability(),
            OutboxSupersedability::PodStatus
        );
        assert_eq!(
            pod_status.terminal_delete(),
            TerminalDeleteClassification::NotTerminalDelete
        );
        assert_eq!(
            pod_status.sequence_policy(),
            OutboxSequencePolicy::PerSubject
        );

        let terminal = StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "pod-a".to_string(),
            pod_uid: "uid-a".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: 7,
        };
        let terminal = OutboxOperation::PodMetadata
            .classification(&terminal)
            .expect("valid actor-owned terminal delete classification");
        assert_eq!(
            terminal.terminal_delete(),
            TerminalDeleteClassification::ActorOwnedPodDelete
        );
        assert_eq!(terminal.supersedability(), OutboxSupersedability::Never);
        assert_eq!(terminal.sequence_policy(), OutboxSequencePolicy::PerSubject);

        let lease = OutboxOperation::LeaseRenew
            .classification(&ordinary)
            .expect("valid lease classification");
        assert_eq!(lease.priority(), OutboxPriority::Lease);
        assert_eq!(lease.sequence_policy(), OutboxSequencePolicy::Unsequenced);

        for operation in OutboxOperation::ALL {
            if operation != OutboxOperation::LeaseRenew {
                assert_eq!(
                    operation
                        .classification(&ordinary)
                        .expect("valid durable operation classification")
                        .sequence_policy(),
                    OutboxSequencePolicy::PerSubject,
                    "{operation}"
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxPayload {
    pub command: StorageCommand,
}

impl OutboxPayload {
    pub fn from_command(command: StorageCommand) -> Self {
        Self { command }
    }

    #[cfg(test)]
    pub fn encode_protobuf(&self) -> Result<Vec<u8>> {
        Ok(encode_outbox_payload_protobuf(
            &klights_cluster_core::OutboxPayload::new(self.command.clone()),
        )?)
    }

    #[cfg(test)]
    pub fn decode_protobuf(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            command: decode_outbox_payload_protobuf(bytes)?.into_command(),
        })
    }
}

/// Build the existing stale UID-bound Pod sentinel used to commit an outbox
/// ledger and exact stream watermark without mutating a Kubernetes resource.
/// The invalid namespace cannot collide with an API-created Pod.
pub(crate) fn terminal_decision_command(idempotency_key: &str) -> StorageCommand {
    StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("__klights-terminal-outbox__".to_string()),
        name: "decision".to_string(),
        status: serde_json::json!({}),
        expected_rv: None,
        preconditions: ResourcePreconditions::uid(idempotency_key),
        observed_status_stamp: None,
    }
}
