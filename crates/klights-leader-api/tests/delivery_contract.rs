use std::sync::Arc;

use klights_cluster_core::{OutboxApplyError, OutboxApplyOutcome, OutboxOperation};
use klights_leader_api::{
    LeaderOutboxDelivery, OutboxDeliveryError, OutboxDeliveryFuture, OutboxDeliveryOperation,
    OutboxDeliveryRequest, OutboxDeliveryResult,
};

struct ObjectSafeDelivery;

impl LeaderOutboxDelivery for ObjectSafeDelivery {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            let _ = request;
            OutboxDeliveryResult::try_applied(41)
        })
    }
}

fn assert_object_safe(_: &dyn LeaderOutboxDelivery) {}

#[test]
fn durable_delivery_maps_the_neutral_outbox_contract_at_the_boundary() {
    for operation in OutboxOperation::ALL {
        if operation == OutboxOperation::LeaseRenew {
            assert!(matches!(
                OutboxDeliveryOperation::try_from(operation),
                Err(OutboxDeliveryError::InvalidRequest { .. })
            ));
            continue;
        }

        let delivery = OutboxDeliveryOperation::try_from(operation)
            .expect("durable neutral operation has a leader delivery representation");
        assert_eq!(OutboxOperation::from(delivery), operation);
    }

    assert_eq!(
        OutboxDeliveryResult::from(OutboxApplyOutcome::Applied { applied_rv: 17 }),
        OutboxDeliveryResult::Applied { applied_rv: 17 }
    );
    assert_eq!(
        OutboxDeliveryResult::from(OutboxApplyOutcome::AlreadyApplied {
            applied_rv: Some(19),
        }),
        OutboxDeliveryResult::AlreadyApplied {
            applied_rv: Some(19),
        }
    );
    assert_eq!(
        OutboxDeliveryError::from(OutboxApplyError::UidMismatch {
            expected: "old".to_string(),
            actual: "new".to_string(),
        }),
        OutboxDeliveryError::UidMismatch {
            expected: "old".to_string(),
            actual: "new".to_string(),
        }
    );
}

#[test]
fn delivery_port_is_object_safe_and_values_are_send_sync() {
    assert_object_safe(&ObjectSafeDelivery);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OutboxDeliveryRequest>();
    assert_send_sync::<OutboxDeliveryResult>();
    assert_send_sync::<OutboxDeliveryError>();
}

#[test]
fn request_owns_payload_and_requires_complete_durable_identity() {
    let payload: Arc<[u8]> = Arc::from([1_u8, 2, 3]);
    let request = OutboxDeliveryRequest::try_new(
        "worker-a:pod-status:uid-a:41",
        OutboxDeliveryOperation::PodStatus,
        Arc::clone(&payload),
        "outbox-client-a",
        73,
        9,
    )
    .expect("valid delivery request");

    assert_eq!(request.idempotency_key(), "worker-a:pod-status:uid-a:41");
    assert_eq!(request.operation(), OutboxDeliveryOperation::PodStatus);
    assert!(Arc::ptr_eq(request.payload(), &payload));
    assert_eq!(request.client_id(), "outbox-client-a");
    assert_eq!(request.stream_id(), 73);
    assert_eq!(request.stream_sequence(), 9);
    assert_eq!(
        request.codec_version(),
        klights_cluster_core::COMMAND_CODEC_VERSION
    );

    for advertised in [
        klights_cluster_core::COMMAND_CODEC_VERSION - 1,
        klights_cluster_core::COMMAND_CODEC_VERSION + 1,
    ] {
        let incompatible_peer = OutboxDeliveryRequest::try_new_versioned(
            advertised,
            "worker-a:pod-status:uid-a:40",
            OutboxDeliveryOperation::PodStatus,
            Arc::clone(&payload),
            "outbox-client-a",
            73,
            8,
        )
        .expect("transport request preserves the sender codec for exact-version admission");
        assert_eq!(incompatible_peer.codec_version(), advertised);
    }

    for (field, result) in [
        (
            "delivery.idempotency_key",
            OutboxDeliveryRequest::try_new(
                "",
                OutboxDeliveryOperation::PodStatus,
                Arc::from([1_u8]),
                "client",
                1,
                1,
            ),
        ),
        (
            "delivery.payload",
            OutboxDeliveryRequest::try_new(
                "key",
                OutboxDeliveryOperation::PodStatus,
                Arc::from([]),
                "client",
                1,
                1,
            ),
        ),
        (
            "delivery.client_id",
            OutboxDeliveryRequest::try_new(
                "key",
                OutboxDeliveryOperation::PodStatus,
                Arc::from([1_u8]),
                "",
                1,
                1,
            ),
        ),
        (
            "delivery.stream_id",
            OutboxDeliveryRequest::try_new(
                "key",
                OutboxDeliveryOperation::PodStatus,
                Arc::from([1_u8]),
                "client",
                0,
                1,
            ),
        ),
        (
            "delivery.stream_sequence",
            OutboxDeliveryRequest::try_new(
                "key",
                OutboxDeliveryOperation::PodStatus,
                Arc::from([1_u8]),
                "client",
                1,
                0,
            ),
        ),
    ] {
        assert!(
            matches!(
                result,
                Err(OutboxDeliveryError::InvalidRequest {
                    field: actual,
                    ..
                }) if actual == field
            ),
            "expected validation failure for {field}"
        );
    }
}

#[test]
fn operation_wire_names_are_exact_and_lease_renew_is_not_delivery_authority() {
    let cases = [
        (OutboxDeliveryOperation::PodStatus, "PodStatus"),
        (
            OutboxDeliveryOperation::RuntimeReconcile,
            "RuntimeReconcile",
        ),
        (OutboxDeliveryOperation::ProbeReadiness, "ProbeReadiness"),
        (
            OutboxDeliveryOperation::DeadlineExceeded,
            "DeadlineExceeded",
        ),
        (
            OutboxDeliveryOperation::ContainerStatusSnapshot,
            "ContainerStatusSnapshot",
        ),
        (
            OutboxDeliveryOperation::EphemeralContainerStatuses,
            "EphemeralContainerStatuses",
        ),
        (OutboxDeliveryOperation::PodMetadata, "PodMetadata"),
        (
            OutboxDeliveryOperation::NodeRegistration,
            "NodeRegistration",
        ),
        (OutboxDeliveryOperation::NodeDataplane, "NodeDataplane"),
        (OutboxDeliveryOperation::NodeStatus, "NodeStatus"),
        (OutboxDeliveryOperation::EventCreate, "EventCreate"),
    ];

    for (operation, wire_name) in cases {
        assert_eq!(operation.as_wire_name(), wire_name);
        assert_eq!(
            OutboxDeliveryOperation::try_from_wire_name(wire_name).unwrap(),
            operation
        );
    }
    for forbidden in ["LeaseRenew", "", "UnknownOperation"] {
        assert!(matches!(
            OutboxDeliveryOperation::try_from_wire_name(forbidden),
            Err(OutboxDeliveryError::InvalidRequest {
                field: "delivery.operation",
                ..
            })
        ));
    }
}

#[test]
fn results_preserve_applied_and_already_applied_optional_rv_semantics() {
    let applied = OutboxDeliveryResult::try_applied(41).expect("positive applied RV");
    assert!(!applied.already_applied());
    assert_eq!(applied.resource_version(), Some(41));

    let duplicate_with_rv =
        OutboxDeliveryResult::try_already_applied(Some(41)).expect("positive duplicate RV");
    assert!(duplicate_with_rv.already_applied());
    assert_eq!(duplicate_with_rv.resource_version(), Some(41));

    let duplicate_without_rv =
        OutboxDeliveryResult::try_already_applied(None).expect("watermark-only duplicate");
    assert!(duplicate_without_rv.already_applied());
    assert_eq!(duplicate_without_rv.resource_version(), None);

    for invalid in [
        OutboxDeliveryResult::try_applied(0),
        OutboxDeliveryResult::try_applied(-1),
        OutboxDeliveryResult::try_already_applied(Some(0)),
        OutboxDeliveryResult::try_already_applied(Some(-1)),
    ] {
        assert!(matches!(
            invalid,
            Err(OutboxDeliveryError::CorruptResponse { .. })
        ));
    }
}

#[test]
fn terminal_and_retryable_delivery_failures_are_typed() {
    let codec = OutboxDeliveryError::codec_incompatible(
        klights_cluster_core::COMMAND_CODEC_VERSION + 1,
        klights_cluster_core::COMMAND_CODEC_VERSION,
    );
    assert!(codec.is_retryable());
    assert!(!codec.is_terminal());
    assert!(
        codec.to_string().contains("incompatible"),
        "a future peer may handle this retryable rejection by explicitly reconnecting in v3 mode"
    );

    let cases = [
        (OutboxDeliveryError::not_leader(), true, false),
        (
            OutboxDeliveryError::unavailable("transport is absent"),
            true,
            false,
        ),
        (OutboxDeliveryError::timeout(), true, false),
        (OutboxDeliveryError::cancelled(), true, false),
        (
            OutboxDeliveryError::invalid("delivery.payload", "malformed"),
            false,
            true,
        ),
        (
            OutboxDeliveryError::not_found("Pod default/web not found"),
            false,
            true,
        ),
        (
            OutboxDeliveryError::uid_mismatch("uid-old", "uid-new"),
            false,
            true,
        ),
        (
            OutboxDeliveryError::conflict("command is not authorized"),
            false,
            true,
        ),
        (
            OutboxDeliveryError::corrupt_response("unknown wire error type"),
            true,
            false,
        ),
    ];

    for (error, retryable, terminal) in cases {
        assert_eq!(error.is_retryable(), retryable, "{error}");
        assert_eq!(error.is_terminal(), terminal, "{error}");
    }
}
