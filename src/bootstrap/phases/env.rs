//! Phase 1: Environment initialization.
//!
//! Tracing, root privilege check, process group isolation, SIGPIPE,
//! role validation. Pure setup — no I/O state.

use anyhow::Result;

use crate::bootstrap::{CliFlags, NodeRole};

/// Initialize tracing subscriber. Must be called before any other phase.
pub fn init_tracing(_cli: &CliFlags) {
    let namespace =
        std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string());
    crate::bootstrap::logging::init_tracing_from_env(&namespace);
}

/// Require root, create process group, ignore SIGPIPE, set namespace.
pub fn init_process(_cli: &CliFlags) -> Result<()> {
    // SAFETY: geteuid(2) is a thread-safe syscall with no preconditions and
    // returns the effective user id; it cannot fail or read invalid memory.
    if unsafe { libc::geteuid() } != 0 {
        tracing::error!("klights requires root privileges");
        tracing::error!("Run with: sudo ./klights");
        anyhow::bail!("must run as root");
    }
    // SAFETY: setsid() is safe to call — it creates a new session if this
    // process is not already a process group leader. ESRCH is harmless.
    unsafe { libc::setsid() };
    // SAFETY: setting SIGPIPE to SIG_IGN is safe and standard practice
    // for long-running server processes.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    Ok(())
}

/// Validate role constraints against the detected node mode.
pub fn validate_role(role: &NodeRole, node_mode: &crate::bootstrap::NodeMode) -> Result<()> {
    super::super::init::predicates::validate_rootless_multinode_support(role, node_mode)?;
    Ok(())
}
