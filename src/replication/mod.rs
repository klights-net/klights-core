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

pub mod apply;
pub mod grpc;
pub mod protocol;
pub mod service;
pub mod snapshot;

pub use service::ReplicationService;

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
