//! Pure Service-CIDR address derivation shared by bootstrap and kubelet code.

/// Derive the Kubernetes API Service IP (the first usable address).
pub(crate) fn kubernetes_service_ip(service_cidr: &str) -> String {
    crate::utils::derive_first_ip(service_cidr)
}

/// Derive the cluster DNS Service IP (network address plus ten).
pub(crate) fn dns_service_ip(service_cidr: &str) -> String {
    let network_addr = service_cidr
        .split_once('/')
        .map_or(service_cidr, |(address, _)| address);
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
