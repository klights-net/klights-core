//! Strongly-typed network primitives.
//!
//! Replaces string-passing for network identifiers with newtypes that
//! enforce validation at parse time and provide type-safe APIs.

use std::fmt;

const IFNAMSIZ: usize = 15;

/// A bridge interface name (e.g., "klights").
///
/// Validated to be non-empty, ≤15 ASCII characters (Linux IFNAMSIZ),
/// with no `/` or NUL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BridgeName(String);

impl BridgeName {
    /// Strict parse: rejects names longer than IFNAMSIZ-1 (15 chars).
    /// Used for spec compliance — production code uses
    /// [`BridgeName::parse_truncating`] at the bootstrap configuration edge.
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
    /// bootstrap configuration parsing.
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use klights_types::{ClusterCidr, NodeName, PodSubnet};

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
