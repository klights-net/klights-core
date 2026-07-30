//! Passive replication protocol and service skeleton (2A-4).
//!
//! Defines the replication protocol types and leader-side service that can
//! stream `StorageCommand + CommandMeta` to connected replicas.
//!
//! ## Design invariants
//! - Replication payload is `StorageCommand + CommandMeta` only (no backend-specific SQL/WAL).
//! - All tasks use `TaskSupervisor`; no direct `tokio::spawn`, sleeps, or intervals.
//! - Service is idle-silent when no replicas connect.
//! - Request/response types support JSON and protobuf codecs.

pub mod fanout;
#[cfg(test)]
#[path = "grpc/client/tests.rs"]
mod grpc_client_tests;
pub(crate) mod grpc_runtime_adapter;
pub mod log_apply_wire;
pub(crate) mod outbox_payload_codec;
pub(crate) mod outbox_response_wire;
pub mod service;
#[cfg(test)]
#[path = "grpc/snapshot_cache.rs"]
pub mod snapshot_cache;
#[cfg(test)]
pub mod test_proto_channel_sink;
pub use service::ReplicationService;

pub(crate) fn new_command_id() -> klights_cluster_core::CommandId {
    klights_cluster_core::CommandId(uuid::Uuid::new_v4().to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn legacy_tcp_transport_module_files_are_removed() {
        // Path-existence check stays here (no source-text scan).
        // The matching "no `pub mod {legacy};` declaration" invariant
        // is enforced by the base-repo source guard run by `./build.sh`.
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for path in [
            "src/replication/codec.rs",
            "src/replication/connector.rs",
            "src/replication/forwarder.rs",
            "src/replication/transport.rs",
        ] {
            assert!(
                !manifest_dir.join(path).exists(),
                "legacy TCP replication module must be removed: {path}"
            );
        }
    }
}
