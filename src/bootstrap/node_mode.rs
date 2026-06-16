//! Process-startup node mode detection.
//!
//! The operating mode is decided once at process startup and never mutated
//! thereafter. The detected `NodeMode` is stored on `AppState` and read by
//! reference by every subsystem.
//!
//! The root and rootless variants are selected at this boundary without
//! breaking existing match arms thanks to `#[non_exhaustive]`.

use anyhow::Result;
use std::path::PathBuf;

/// Operating mode for the local klights node.
///
/// Marked `#[non_exhaustive]` so adding fields or future variants doesn't
/// force every consumer to update its match arms.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeMode {
    /// Process runs as root with the default encrypted WireGuard pod-CIDR
    /// dataplane in the host network namespace (explicit direct-route mode
    /// installs only kernel routes; VXLAN is dormant legacy, not used).
    Root,
    /// Process runs unprivileged inside a rootlesskit user namespace.
    /// `rootlesskit_pid` is the parent rootlesskit PID; `user_netns` is
    /// the path to `/proc/<pid>/ns/net`. Both are read from the
    /// `ROOTLESSKIT_*` env vars by [`NodeMode::detect`].
    ///
    /// Rootless peer metadata is accepted by the hybrid reconcilers; the
    /// rootless CNI datapath is provided by `RootlessNetworkPlane`; multinode
    /// rootless roles remain gated until WireGuard-over-pasta host-edge
    /// validation and hybrid endpoint hardening land.
    Rootless {
        rootlesskit_pid: u32,
        user_netns: PathBuf,
    },
}

impl NodeMode {
    /// Detect the runtime mode from CLI flags. Pure function — no I/O.
    ///
    /// `cli_rootless = true` constructs the `Rootless` variant; the
    /// rootlesskit env vars (`ROOTLESSKIT_PID`, `ROOTLESSKIT_STATE_DIR`)
    /// are read here so the bootstrap match-arm can dispatch on a fully
    /// populated `NodeMode` without further env lookups.
    pub fn detect(cli_rootless: bool) -> Result<Self> {
        if !cli_rootless {
            return Ok(NodeMode::Root);
        }

        // Defaults are loose placeholders until boot validation grows strict
        // rootlesskit detection. Multinode rootless roles remain gated before
        // join metadata is built, so these placeholders cannot silently join a
        // cluster as a rootless worker or replica learner.
        let rootlesskit_pid: u32 = std::env::var("ROOTLESSKIT_PID")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let user_netns = std::env::var("ROOTLESSKIT_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(format!("/proc/{rootlesskit_pid}/ns/net")));
        Ok(NodeMode::Rootless {
            rootlesskit_pid,
            user_netns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_mode_detect_default_returns_root() {
        let mode = NodeMode::detect(false).expect("default detect must succeed");
        assert_eq!(mode, NodeMode::Root);
    }

    #[test]
    fn test_node_mode_detect_rootless_returns_rootless_variant() {
        // This is only the detection contract; live rootless networking
        // validation happens later in bootstrap/networking.
        let mode = NodeMode::detect(true).expect("detect(true) must succeed");
        match mode {
            NodeMode::Rootless { .. } => {}
            other => panic!("expected Rootless, got {other:?}"),
        }
    }

    #[test]
    fn test_app_state_mode_is_root_after_bootstrap() {
        // Bootstrap is async + heavy; this test exercises the contract that
        // AppState carries a `mode` field that defaults to `Root` for the
        // unit-test fixture. The full wiring is verified by integration
        // tests in `validate.sh`.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let state = crate::api::test_support::build_test_app_state().await;
            assert_eq!(state.mode, NodeMode::Root);
        });
    }
}
