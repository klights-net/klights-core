//! Strongly-typed network primitives.
//!
//! Replaces string-passing for network identifiers with newtypes that
//! enforce validation at parse time and provide type-safe APIs.

use std::fmt;
use std::net::Ipv4Addr;

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

/// Identifier for a peer node from the perspective of `PeerRouter` /
/// `NetworkProvider::apply_peer_endpoint`.
///
/// Marked `#[non_exhaustive]` so adding the rootless variant doesn't
/// force every match arm in the codebase to add a wildcard.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeEndpoint {
    /// Peer reachable through the klights-managed WireGuard overlay.
    /// This is the default cross-node pod dataplane when encryption is
    /// enabled.
    WireGuard(crate::networking::wireguard::WireGuardPeerPlan),
    /// Peer reachable through an explicitly disabled-encryption direct route.
    /// This is operator-selected plaintext, never an implicit fallback.
    UnencryptedDirect(crate::networking::wireguard::UnencryptedPeerPlan),
    /// Peer reachable via host-port grafting on (node_ip, hostport_range).
    /// Used by hybrid clusters where one or more nodes run rootless.
    /// Root/rootless pod reachability is reconciled from `pod_endpoints`;
    /// this endpoint carries node-level metadata for the shared peer watch.
    Rootless {
        node_ip: std::net::IpAddr,
        hostport_range: HostPortRange,
    },
}

/// Inclusive range of host ports a rootless node uses to expose pods.
/// Phase 2 reconcilers allocate ports from this window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostPortRange {
    pub start: u16,
    pub end: u16,
}

impl HostPortRange {
    /// Parse `"start-end"` as published in the `klights.io/hostport-range`
    /// annotation. Empty input or any malformed shape returns an error so
    /// callers can choose to skip the peer.
    pub fn parse(s: &str) -> Result<Self, String> {
        let trimmed = s.trim();
        let (start_s, end_s) = trimmed
            .split_once('-')
            .ok_or_else(|| format!("HostPortRange must be 'start-end', got '{trimmed}'"))?;
        let start: u16 = start_s
            .parse()
            .map_err(|e| format!("HostPortRange start '{start_s}' invalid: {e}"))?;
        let end: u16 = end_s
            .parse()
            .map_err(|e| format!("HostPortRange end '{end_s}' invalid: {e}"))?;
        if start == 0 || end == 0 || start > end {
            return Err(format!(
                "HostPortRange '{trimmed}' must be non-zero and start <= end"
            ));
        }
        Ok(Self { start, end })
    }
}

impl fmt::Display for HostPortRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.start, self.end)
    }
}

const IFNAMSIZ: usize = 15;

/// A subnet allocated for pod IPs (per-node /24 in multinode, or wider when
/// the daemon owns the whole cluster CIDR alone).
///
/// Stored as u32 base address (host byte order) and prefix length.
/// The bridge IP is base+1; the pod IP range is base+2..=base+size-2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PodSubnet {
    base: u32,
    prefix: u8,
}

impl PodSubnet {
    /// Parse a CIDR string like "10.244.1.0/24" into a `PodSubnet`.
    ///
    /// Accepts prefixes /1 through /30 (must leave room for base+bridge+pod+broadcast).
    /// Strips host bits if the input is not aligned (e.g. `10.43.0.5/24` → `10.43.0.0/24`).
    pub fn parse(cidr: &str) -> Result<Self, String> {
        let (base_unaligned, prefix) = parse_cidr_components(cidr)?;
        if !(1..=30).contains(&prefix) {
            return Err(format!(
                "PodSubnet prefix must be in /1..=/30, got /{} in {}",
                prefix, cidr
            ));
        }
        let mask = mask_for_prefix(prefix);
        Ok(Self {
            base: base_unaligned & mask,
            prefix,
        })
    }

    /// Construct directly from validated parts (test-only).
    #[cfg(test)]
    pub fn from_parts(base: u32, prefix: u8) -> Self {
        Self {
            base: base & mask_for_prefix(prefix),
            prefix,
        }
    }

    pub fn bridge_ip(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.base + 1)
    }

    /// Inclusive `[first, last]` range of usable pod IPs (base+2 .. last-1).
    pub fn pod_ip_range(&self) -> (u32, u32) {
        let size = self.size();
        (self.base + 2, self.base + size - 2)
    }

    pub fn size(&self) -> u32 {
        1u32 << (32 - self.prefix as u32)
    }

    pub fn mask(&self) -> u32 {
        mask_for_prefix(self.prefix)
    }

    pub fn prefix(&self) -> u8 {
        self.prefix
    }

    pub fn base(&self) -> u32 {
        self.base
    }

    pub fn base_ip(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.base)
    }
}

impl fmt::Display for PodSubnet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", Ipv4Addr::from(self.base), self.prefix)
    }
}

impl ToSql for PodSubnet {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for PodSubnet {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let s = value.as_str()?;
        PodSubnet::parse(s).map_err(|e| FromSqlError::Other(Box::new(ParseError(e))))
    }
}

/// The cluster-wide CIDR (e.g., "10.244.0.0/16").
///
/// Used for VXLAN configuration, per-node subnet allocation, and routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ClusterCidr {
    base: u32,
    prefix: u8,
}

impl ClusterCidr {
    /// Parse a CIDR string like "10.244.0.0/16" into a `ClusterCidr`.
    ///
    /// Accepts /0 through /32. Host bits are kept on the `base` field; use
    /// `network()` for the masked prefix base.
    pub fn parse(cidr: &str) -> Result<Self, String> {
        let (base, prefix) = parse_cidr_components(cidr)?;
        if prefix > 32 {
            return Err(format!(
                "ClusterCidr prefix cannot exceed /32, got /{} in {}",
                prefix, cidr
            ));
        }
        Ok(Self { base, prefix })
    }

    #[cfg(test)]
    pub fn from_parts(base: u32, prefix: u8) -> Self {
        Self { base, prefix }
    }

    pub fn base(&self) -> u32 {
        self.base
    }

    pub fn network(&self) -> u32 {
        self.base & self.mask()
    }

    pub fn network_ip(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.network())
    }

    pub fn prefix(&self) -> u8 {
        self.prefix
    }

    pub fn mask(&self) -> u32 {
        mask_for_prefix(self.prefix)
    }
}

impl fmt::Display for ClusterCidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", Ipv4Addr::from(self.base), self.prefix)
    }
}

/// A bridge interface name (e.g., "klights").
///
/// Validated to be non-empty, ≤15 ASCII characters (Linux IFNAMSIZ),
/// with no `/` or NUL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BridgeName(String);

impl BridgeName {
    /// Strict parse: rejects names longer than IFNAMSIZ-1 (15 chars).
    /// Used for spec compliance — production code uses
    /// [`BridgeName::parse_truncating`] from `KlightsConfig::from_env`.
    pub fn parse(name: &str) -> Result<Self, String> {
        let trimmed = name.trim();
        validate_bridge_chars(trimmed)?;
        if trimmed.len() > IFNAMSIZ {
            return Err(format!(
                "Bridge name '{}' exceeds {} char limit",
                trimmed, IFNAMSIZ
            ));
        }
        Ok(BridgeName(trimmed.to_string()))
    }

    /// Tolerant parse for env-config: keeps the LAST 15 chars to preserve
    /// suffix uniqueness (e.g. `klights-developer-1` vs `-2`). Used by
    /// `KlightsConfig::from_env`.
    pub fn parse_truncating(name: &str) -> Result<Self, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("Bridge name cannot be empty".to_string());
        }
        let truncated = if trimmed.len() > IFNAMSIZ {
            &trimmed[trimmed.len() - IFNAMSIZ..]
        } else {
            trimmed
        };
        validate_bridge_chars(truncated)?;
        Ok(BridgeName(truncated.to_string()))
    }

    #[cfg(test)]
    pub fn new_unchecked(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for BridgeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BridgeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A node name (DNS-1123 label).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeName(String);

impl NodeName {
    pub fn parse(name: &str) -> Result<Self, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("Node name cannot be empty".to_string());
        }
        if trimmed.len() > 253 {
            return Err(format!(
                "Node name too long (max 253 chars), got {}",
                trimmed.len()
            ));
        }
        if !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        {
            return Err(format!(
                "Node name must be alphanumeric, hyphen, or dot, got: {}",
                trimmed
            ));
        }
        if trimmed.starts_with('-') || trimmed.ends_with('-') {
            return Err("Node name cannot start or end with hyphen".to_string());
        }
        Ok(NodeName(trimmed.to_string()))
    }

    #[cfg(test)]
    pub fn new_unchecked(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------- internal helpers ----------------

fn mask_for_prefix(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else if prefix >= 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - prefix as u32)
    }
}

fn parse_cidr_components(cidr: &str) -> Result<(u32, u8), String> {
    let (addr_str, prefix_str) = cidr
        .split_once('/')
        .ok_or_else(|| format!("CIDR must be in the form a.b.c.d/prefix, got: {}", cidr))?;
    let prefix: u8 = prefix_str
        .parse()
        .map_err(|_| format!("Invalid prefix length in CIDR: {}", cidr))?;
    let addr: Ipv4Addr = addr_str
        .parse()
        .map_err(|_| format!("Invalid IPv4 address in CIDR: {}", cidr))?;
    Ok((u32::from(addr), prefix))
}

fn validate_bridge_chars(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Bridge name cannot be empty".to_string());
    }
    if name.contains('/') {
        return Err(format!("Bridge name cannot contain '/', got: {}", name));
    }
    if name.contains('\0') {
        return Err("Bridge name cannot contain NUL character".to_string());
    }
    if !name.is_ascii() {
        return Err(format!("Bridge name must be ASCII, got: {}", name));
    }
    Ok(())
}

#[derive(Debug)]
struct ParseError(String);
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    // PodSubnet ---------------------------------------------------------
    #[test]
    fn pod_subnet_parses_24() {
        let s = PodSubnet::parse("10.244.1.0/24").unwrap();
        assert_eq!(s.prefix(), 24);
        assert_eq!(s.base(), u32::from(Ipv4Addr::new(10, 244, 1, 0)));
        assert_eq!(s.bridge_ip(), Ipv4Addr::new(10, 244, 1, 1));
        assert_eq!(
            s.pod_ip_range(),
            (
                u32::from(Ipv4Addr::new(10, 244, 1, 2)),
                u32::from(Ipv4Addr::new(10, 244, 1, 254))
            )
        );
        assert_eq!(s.mask(), 0xffff_ff00);
        assert_eq!(s.to_string(), "10.244.1.0/24");
    }

    #[test]
    fn pod_subnet_parses_17() {
        let s = PodSubnet::parse("10.43.0.0/17").unwrap();
        assert_eq!(s.prefix(), 17);
        assert_eq!(s.size(), 1 << 15);
        assert_eq!(s.bridge_ip(), Ipv4Addr::new(10, 43, 0, 1));
        let (first, last) = s.pod_ip_range();
        assert_eq!(first, u32::from(Ipv4Addr::new(10, 43, 0, 2)));
        assert_eq!(last, u32::from(Ipv4Addr::new(10, 43, 127, 254)));
    }

    #[test]
    fn pod_subnet_rejects_zero_and_31() {
        assert!(PodSubnet::parse("0.0.0.0/0").is_err());
        assert!(PodSubnet::parse("10.0.0.0/31").is_err());
        assert!(PodSubnet::parse("10.0.0.0/32").is_err());
    }

    #[test]
    fn pod_subnet_strips_host_bits() {
        let s = PodSubnet::parse("10.43.0.255/24").unwrap();
        assert_eq!(s.base(), u32::from(Ipv4Addr::new(10, 43, 0, 0)));
        assert_eq!(s.to_string(), "10.43.0.0/24");
    }

    #[test]
    fn pod_subnet_rejects_garbage() {
        assert!(PodSubnet::parse("not-cidr").is_err());
        assert!(PodSubnet::parse("10.43.0.0").is_err());
        assert!(PodSubnet::parse("10.43.0.0/abc").is_err());
    }

    #[test]
    fn pod_subnet_to_sql_round_trip() {
        let s = PodSubnet::parse("10.43.0.0/24").unwrap();
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t (s TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (?)", rusqlite::params![s])
            .unwrap();
        let got: PodSubnet = conn.query_row("SELECT s FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(s, got);
    }

    // ClusterCidr -------------------------------------------------------
    #[test]
    fn cluster_cidr_parses_16() {
        let c = ClusterCidr::parse("10.244.0.0/16").unwrap();
        assert_eq!(c.prefix(), 16);
        assert_eq!(c.network(), u32::from(Ipv4Addr::new(10, 244, 0, 0)));
        assert_eq!(c.mask(), 0xffff_0000);
        assert_eq!(c.to_string(), "10.244.0.0/16");
    }

    #[test]
    fn cluster_cidr_strips_host_bits_via_network() {
        let c = ClusterCidr::parse("10.43.0.255/24").unwrap();
        assert_eq!(c.network(), u32::from(Ipv4Addr::new(10, 43, 0, 0)));
    }

    #[test]
    fn cluster_cidr_slash_zero() {
        let c = ClusterCidr::parse("0.0.0.0/0").unwrap();
        assert_eq!(c.mask(), 0);
        assert_eq!(c.network(), 0);
    }

    #[test]
    fn cluster_cidr_slash_thirty_two() {
        let c = ClusterCidr::parse("192.168.1.5/32").unwrap();
        assert_eq!(c.mask(), 0xffff_ffff);
        assert_eq!(c.network(), u32::from(Ipv4Addr::new(192, 168, 1, 5)));
    }

    #[test]
    fn cluster_cidr_rejects_invalid() {
        assert!(ClusterCidr::parse("not").is_err());
        assert!(ClusterCidr::parse("10.0.0.0/33").is_err());
    }

    // BridgeName --------------------------------------------------------
    #[test]
    fn bridge_name_parse_strict_rejects_too_long() {
        // "Done when": BridgeName::parse("a".repeat(16)) returns Err.
        assert!(BridgeName::parse(&"a".repeat(16)).is_err());
        assert!(BridgeName::parse("klights12345678").is_ok());
    }

    #[test]
    fn bridge_name_parse_strict_accepts_15() {
        assert!(BridgeName::parse(&"a".repeat(15)).is_ok());
    }

    #[test]
    fn bridge_name_parse_truncating_keeps_last_15() {
        let n = BridgeName::parse_truncating(&"a".repeat(20)).unwrap();
        assert_eq!(n.as_str().len(), 15);
    }

    #[test]
    fn bridge_name_rejects_invalid() {
        assert!(BridgeName::parse("").is_err());
        assert!(BridgeName::parse_truncating("").is_err());
        assert!(BridgeName::parse("foo/bar").is_err());
        assert!(BridgeName::parse("foo\0bar").is_err());
        assert!(BridgeName::parse("naïve").is_err());
    }

    // NodeName ----------------------------------------------------------
    #[test]
    fn node_name_parses_valid() {
        assert!(NodeName::parse("node1").is_ok());
        assert!(NodeName::parse("node-1").is_ok());
        assert!(NodeName::parse("node.example.com").is_ok());
    }

    #[test]
    fn node_name_rejects_invalid() {
        assert!(NodeName::parse("").is_err());
        assert!(NodeName::parse("-leading").is_err());
        assert!(NodeName::parse("trailing-").is_err());
        assert!(NodeName::parse(&"a".repeat(254)).is_err());
        assert!(NodeName::parse("space here").is_err());
    }
}
