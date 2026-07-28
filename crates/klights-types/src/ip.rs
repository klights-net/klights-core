use std::net::Ipv4Addr;

/// Derive the first usable IPv4 address from a CIDR (network address + 1).
///
/// Invalid CIDRs return `0.0.0.0`, preserving the legacy bootstrap helper's
/// fallback while keeping this pure policy value independent of networking,
/// controller, and root application owners.
pub fn first_usable_ipv4(cidr: &str) -> String {
    parse_network(cidr)
        .map(|network| Ipv4Addr::from(network.wrapping_add(1)).to_string())
        .unwrap_or_else(|| Ipv4Addr::UNSPECIFIED.to_string())
}

/// Derive the cluster DNS Service IP (network address plus ten).
///
/// The service CIDR is validated by root configuration before feature
/// construction, so malformed input remains a bootstrap programming error.
pub fn dns_service_ipv4(cidr: &str) -> String {
    let network_addr = cidr.split_once('/').map_or(cidr, |(address, _)| address);
    let mut octets = network_addr.split('.');
    let first = octets.next().expect("service CIDR has first octet");
    let second = octets.next().expect("service CIDR has second octet");
    let third = octets.next().expect("service CIDR has third octet");
    let fourth = octets
        .next()
        .expect("service CIDR has fourth octet")
        .parse::<u8>()
        .expect("service CIDR fourth octet is numeric");
    format!("{first}.{second}.{third}.{}", fourth + 10)
}

/// Convert a network-order `u32` IPv4 address to dotted-quad form.
pub fn ipv4_from_u32(ip: u32) -> String {
    Ipv4Addr::from(ip).to_string()
}

fn parse_network(cidr: &str) -> Option<u32> {
    let (address, prefix) = cidr.split_once('/')?;
    let address = u32::from(address.parse::<Ipv4Addr>().ok()?);
    let prefix = prefix.parse::<u8>().ok()?;
    if prefix > 32 {
        return None;
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    Some(address & mask)
}

#[cfg(test)]
mod tests {
    use super::{dns_service_ipv4, first_usable_ipv4, ipv4_from_u32};

    #[test]
    fn first_usable_ipv4_preserves_legacy_cidr_derivation() {
        let cases = [
            ("10.43.0.0/17", "10.43.0.1"),
            ("10.43.128.0/17", "10.43.128.1"),
            ("10.43.0.255/24", "10.43.0.1"),
            ("0.0.0.0/0", "0.0.0.1"),
            ("not-a-cidr", "0.0.0.0"),
            ("10.43.0.0/33", "0.0.0.0"),
        ];

        for (cidr, expected) in cases {
            assert_eq!(first_usable_ipv4(cidr), expected, "cidr={cidr}");
        }
    }

    #[test]
    fn formats_network_order_ipv4() {
        assert_eq!(
            ipv4_from_u32((192 << 24) | (168 << 16) | (1 << 8) | 1),
            "192.168.1.1"
        );
    }

    #[test]
    fn derives_cluster_dns_service_address() {
        assert_eq!(dns_service_ipv4("10.43.0.0/16"), "10.43.0.10");
    }
}
