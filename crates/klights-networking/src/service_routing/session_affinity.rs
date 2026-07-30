/// Load-balancing selection strategy for this Service.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub enum SessionAffinity {
    /// No affinity — random endpoint selection (probability ladder). Default.
    #[default]
    None,
    /// Source-IP–based affinity: `jhash ip saddr seed 0xcafe` hashes the
    /// client IP to a consistent endpoint index. Each client always hits the
    /// same backend across new connections (no conntrack required). This is
    /// the K8s `spec.sessionAffinity: ClientIP` contract: same client IP →
    /// same pod, deterministically.
    ClientIp,
}
