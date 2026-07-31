//! Explicit opt-in test support for cross-crate integration tests.
//!
//! This module is absent from the normal production public API.

use anyhow::Result;
use rcgen::{Certificate, KeyPair};
use time::OffsetDateTime;

pub fn generate_ca_full_at(
    valid_at: OffsetDateTime,
) -> Result<(Certificate, KeyPair, String, String)> {
    crate::cert::generate_ca_full_at(valid_at)
}

#[allow(clippy::too_many_arguments)]
pub fn generate_server_cert_with_config_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<String>,
    node_name: &str,
    api_fqdn: Option<&str>,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_server_cert_with_config_at(
        ca_cert,
        ca_key,
        service_cidr,
        pod_subnet,
        host_ip,
        node_name,
        api_fqdn,
        valid_at,
    )
}

pub fn generate_server_cert_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_server_cert_at(ca_cert, ca_key, valid_at)
}

pub fn generate_admin_cert_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_admin_cert_at(ca_cert, ca_key, valid_at)
}
