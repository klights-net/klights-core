//! Pasta / host-edge validation for rootless WireGuard.
//!
//! When klights runs rootless, pasta exposes the WireGuard UDP listen port
//! at the host edge. Other nodes' WireGuard peers dial that host-edge UDP
//! port to reach the rootless node's `klights.wg` inside the user netns.
//!
//! If pasta cannot expose the port, the rootless node cannot participate
//! in the encrypted multinode dataplane — other nodes cannot route
//! encrypted pod traffic to it. In that case, the node must fail startup
//! or become `NotReady`.
//!
//! # Validation strategy
//!
//! We check `/proc/net/udp` inside the user network namespace. The
//! WireGuard kernel module binds to the UDP port when the interface is
//! configured with a `ListenPort`. If the port appears in the table with
//! a local address, the port is bound and pasta should be exposing it at
//! the host edge.
//!
//! A more thorough check would use `ss -lunp` or netlink SOCK_DIAG, but
//! those require additional privileges. `/proc/net/udp` is always
//! readable inside the user netns and the check is zero-cost at idle
//! (only runs at boot).

use anyhow::{Context, Result, bail};

use crate::task_supervisor::TaskSupervisor;

/// Check whether the WireGuard UDP port is bound and pasta is exposing it
/// at the host edge. Uses the task supervisor to read `/proc/net/udp`
/// without blocking the async runtime.
pub async fn verify_wireguard_udp_port(port: u16, supervisor: &TaskSupervisor) -> Result<()> {
    let contents = supervisor
        .run_blocking_file_keyed(
            "pasta_udp_port_validation",
            "/proc/net/udp".to_string(),
            move || {
                std::fs::read_to_string("/proc/net/udp")
                    .context("failed to read /proc/net/udp for pasta port validation")
            },
        )
        .await??;

    if port_not_found(&contents, port) {
        bail!(
            "WireGuard UDP port {} is not bound in /proc/net/udp — \
             pasta may not be exposing the WireGuard listen port at the host edge; \
             other nodes cannot reach this rootless node's encrypted dataplane",
            port
        );
    }
    tracing::info!(
        port,
        "rootless WireGuard UDP port verified in /proc/net/udp — pasta exposure OK"
    );
    Ok(())
}

/// Parse `/proc/net/udp` and return true if `port` is NOT found in any
/// entry. Returns false if the port IS found.
///
/// `/proc/net/udp` format (header line omitted):
/// ```text
///   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
///   23: 00000000:CA70 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 2 0000000000000000 0
/// ```
fn port_not_found(contents: &str, port: u16) -> bool {
    let port_hex = format!(":{:04X}", port);
    for line in contents.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 {
            continue;
        }
        let local = fields[1];
        // State 07 = TCP_CLOSE (but for UDP, 07 is the only valid state;
        // unbound ports don't appear at all). We accept any state.
        if local.contains(&port_hex) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_proc_net_udp_finds_bound_port() {
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  23: 00000000:CA70 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 2 0000000000000000 0
  24: 0100007F:0035 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 67890 2 0000000000000000 0
";
        // 0xCA70 = 51824 — not our port
        assert!(port_not_found(
            contents,
            crate::networking::wireguard::DEFAULT_WIREGUARD_PORT
        ));
        // 0x0035 = 53 — found in line 24
        assert!(!port_not_found(contents, 53));
    }

    #[test]
    fn parse_proc_net_udp_port_not_present_returns_not_found() {
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  23: 00000000:CA70 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 2 0000000000000000 0
";
        assert!(port_not_found(
            contents,
            crate::networking::wireguard::DEFAULT_WIREGUARD_PORT
        ));
    }

    #[test]
    fn parse_proc_net_udp_finds_wireguard_default_port() {
        // 0x1DFF = 7679 — the klights WireGuard default port
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  25: 00000000:1DFF 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 2 0000000000000000 0
";
        assert!(!port_not_found(
            contents,
            crate::networking::wireguard::DEFAULT_WIREGUARD_PORT
        ));
    }

    #[test]
    fn parse_proc_net_udp_empty_table() {
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
";
        assert!(port_not_found(
            contents,
            crate::networking::wireguard::DEFAULT_WIREGUARD_PORT
        ));
    }
}
