//! Transport-neutral credential values consumed by authenticators.

/// DER-encoded client certificate presented by the authenticated transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientCertificate(pub Vec<u8>);
