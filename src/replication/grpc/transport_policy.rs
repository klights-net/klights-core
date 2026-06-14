//! bug-grpc Pillar A1: a single value object owning **every** gRPC
//! transport tunable.
//!
//! Before this, dial timeouts, keepalive cadences, message-size limits, and
//! the per-call unary deadline were hand-rolled at each call site
//! (`channel_to_endpoint` in the replication client, a duplicated
//! `MAX_GRPC_MESSAGE_BYTES` in `kubelet::cri`, an unset server decode limit,
//! the lone `unary_deadline` field on the client). [`GrpcTransportPolicy`] is
//! constructed once and injected into the replication client, the CRI client,
//! the gRPC server, and (transitively, because it wraps the replication
//! client) the raft peer transport, so all four consume the same limits and
//! deadlines.
//!
//! The [`Default`] values reproduce the previously-scattered production
//! constants exactly, so threading the policy through is behaviour-preserving.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Endpoint;

/// Owns every transport tunable shared across the gRPC connection tree.
///
/// Clone is cheap (all fields are `Copy`); production wraps it in an `Arc`
/// (see [`SharedGrpcTransportPolicy`]) so a single instance is shared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrpcTransportPolicy {
    // --- dial ---
    /// TCP/TLS connect timeout for a fresh channel build.
    pub connect_timeout: Duration,
    /// Idle time before the first TCP keepalive probe.
    pub tcp_keepalive: Duration,
    /// Interval between TCP keepalive probes.
    pub tcp_keepalive_interval: Duration,
    /// Number of unacknowledged TCP keepalive probes before the socket is
    /// considered dead.
    pub tcp_keepalive_retries: u32,
    /// HTTP/2 PING keepalive interval.
    pub http2_keep_alive_interval: Duration,
    /// HTTP/2 PING keepalive ack timeout.
    pub http2_keep_alive_timeout: Duration,
    /// Whether HTTP/2 keepalive PINGs are sent while the connection is idle.
    pub keep_alive_while_idle: bool,

    // --- limits ---
    /// Max encoded protobuf message size, applied to both decode and encode
    /// on every tonic client and to the server's decode/encode limits.
    pub max_message_bytes: usize,

    // --- per-call ---
    /// Per-call deadline for non-streaming, non-Raft unary worker→leader
    /// RPCs (`apply_outbox`, `renew_node_lease`, reads, …). Bounds a
    /// keepalive-alive but response-wedged call so lane self-heal + durable
    /// retry can re-send on a fresh connection.
    pub unary_deadline: Duration,

    // --- tls ---
    /// Server-side TLS handshake timeout.
    pub tls_handshake_timeout: Duration,

    // --- lanes / streams ---
    /// Whether a transport-level RPC failure evicts the offending channel
    /// lane so the next call rebuilds a fresh connection.
    pub evict_lane_on_transport_error: bool,
    /// Independent connection count for the long-lived stream lane.
    pub stream_lane_pool_size: usize,
    /// Independent connection count for hot status/write unary RPCs.
    pub status_lane_pool_size: usize,
    /// Independent connection count for read/control unary RPCs.
    pub read_lane_pool_size: usize,
    /// Independent connection count for raft consensus RPCs.
    pub raft_lane_pool_size: usize,
    /// Cadence the server emits watch BOOKMARK heartbeats at, and from which
    /// the client derives its watch idle-reconnect timeout.
    pub watch_heartbeat_interval: Duration,
}

/// Which kind of gRPC channel a dial policy is being applied to.
///
/// klights-owned channels (worker↔leader, raft) terminate at another klights
/// process, so the aggressive inter-node HTTP/2 keepalive PING cadence is safe
/// — both ends agree on it. The kubelet→containerd channel terminates at a
/// third-party gRPC server (containerd) over a local unix socket, and must be
/// treated differently: see [`ChannelKind::ContainerdUds`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelKind {
    /// A klights-to-klights channel (worker→leader unary/stream, raft peers).
    InterNode,
    /// The kubelet→containerd CRI channel over the containerd unix socket.
    ///
    /// containerd's gRPC server enforces a minimum client-ping interval
    /// (gRPC-go's server keepalive enforcement defaults to `MinTime` = 5
    /// minutes). A `PullImage` is a *unary* RPC that streams no frames back
    /// until the whole image is pulled, so the server's "reset ping strikes
    /// when I send data" forgiveness never fires during a long pull. If
    /// klights sends its inter-node HTTP/2 keepalive PINGs (every 15s) on this
    /// channel, the strikes accumulate unchecked and containerd answers with
    /// `GOAWAY ENHANCE_YOUR_CALM "too_many_pings"`, tearing the connection
    /// down mid-pull — the in-flight `PullImage` then dies with a
    /// `BrokenPipe`/"stream closed because of a broken pipe" transport error.
    /// A local unix socket needs no application-level liveness PING anyway
    /// (the socket closes immediately if containerd dies), so this channel
    /// sends none. This restores the pre-`GrpcTransportPolicy` CRI dial
    /// behaviour, which set no HTTP/2 keepalive.
    ContainerdUds,
}

/// Shared, ref-counted handle to a single policy instance.
pub type SharedGrpcTransportPolicy = Arc<GrpcTransportPolicy>;

impl Default for GrpcTransportPolicy {
    fn default() -> Self {
        Self {
            // Matches the former hardcoded values in
            // `client::channel_to_endpoint`.
            connect_timeout: Duration::from_secs(10),
            tcp_keepalive: Duration::from_secs(15),
            tcp_keepalive_interval: Duration::from_secs(15),
            tcp_keepalive_retries: 3,
            http2_keep_alive_interval: Duration::from_secs(15),
            http2_keep_alive_timeout: Duration::from_secs(10),
            keep_alive_while_idle: true,
            // Former `MAX_GRPC_MESSAGE_BYTES` (client + cri): 32 MiB.
            max_message_bytes: 32 * 1024 * 1024,
            // Former `DEFAULT_UNARY_DEADLINE`.
            unary_deadline: Duration::from_secs(15),
            // Former `tls::TLS_HANDSHAKE_TIMEOUT`.
            tls_handshake_timeout: Duration::from_secs(10),
            evict_lane_on_transport_error: true,
            stream_lane_pool_size: 1,
            status_lane_pool_size: 4,
            read_lane_pool_size: 2,
            raft_lane_pool_size: 2,
            // Former `server::WATCH_HEARTBEAT_INTERVAL`.
            watch_heartbeat_interval: Duration::from_secs(20),
        }
    }
}

impl GrpcTransportPolicy {
    /// Production policy (the [`Default`]), wrapped in an `Arc` for sharing.
    pub fn shared_default() -> SharedGrpcTransportPolicy {
        Arc::new(Self::default())
    }

    /// Wrap this policy in an `Arc` for injection.
    pub fn shared(self) -> SharedGrpcTransportPolicy {
        Arc::new(self)
    }

    /// Client-initiated HTTP/2 keepalive PING settings (interval, ack timeout,
    /// while-idle) for the given channel kind, or `None` when the client must
    /// send **no** keepalive PINGs on that channel.
    ///
    /// Only [`ChannelKind::InterNode`] channels send PINGs;
    /// [`ChannelKind::ContainerdUds`] returns `None` (see that variant's docs
    /// for why containerd would otherwise `GOAWAY` long pulls).
    pub fn http2_keepalive(&self, kind: ChannelKind) -> Option<(Duration, Duration, bool)> {
        match kind {
            ChannelKind::InterNode => Some((
                self.http2_keep_alive_interval,
                self.http2_keep_alive_timeout,
                self.keep_alive_while_idle,
            )),
            ChannelKind::ContainerdUds => None,
        }
    }

    /// Apply every dial-level tunable to a tonic [`Endpoint`] builder. The
    /// single place dial knobs are set; the replication client and the CRI
    /// client both route through here so they cannot drift.
    ///
    /// `kind` selects the channel-appropriate keepalive behaviour: inter-node
    /// channels get the full TCP + HTTP/2 keepalive cadence; the containerd
    /// unix-socket channel gets neither (TCP keepalive is a no-op on AF_UNIX,
    /// and HTTP/2 PINGs trip containerd's too-many-pings enforcement).
    pub fn configure_endpoint(&self, endpoint: Endpoint, kind: ChannelKind) -> Endpoint {
        let endpoint = endpoint.connect_timeout(self.connect_timeout);
        let endpoint = match kind {
            ChannelKind::InterNode => endpoint
                .tcp_keepalive(Some(self.tcp_keepalive))
                .tcp_keepalive_interval(Some(self.tcp_keepalive_interval))
                .tcp_keepalive_retries(Some(self.tcp_keepalive_retries)),
            // AF_UNIX: TCP keepalive is meaningless on a unix socket.
            ChannelKind::ContainerdUds => endpoint,
        };
        match self.http2_keepalive(kind) {
            Some((interval, timeout, while_idle)) => endpoint
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                .keep_alive_while_idle(while_idle),
            None => endpoint,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_reproduces_former_scattered_constants() {
        let p = GrpcTransportPolicy::default();
        // Former client::channel_to_endpoint literals.
        assert_eq!(p.connect_timeout, Duration::from_secs(10));
        assert_eq!(p.tcp_keepalive, Duration::from_secs(15));
        assert_eq!(p.tcp_keepalive_interval, Duration::from_secs(15));
        assert_eq!(p.tcp_keepalive_retries, 3);
        assert_eq!(p.http2_keep_alive_interval, Duration::from_secs(15));
        assert_eq!(p.http2_keep_alive_timeout, Duration::from_secs(10));
        assert!(p.keep_alive_while_idle);
        // Former MAX_GRPC_MESSAGE_BYTES (client + cri).
        assert_eq!(p.max_message_bytes, 32 * 1024 * 1024);
        // Former DEFAULT_UNARY_DEADLINE.
        assert_eq!(p.unary_deadline, Duration::from_secs(15));
        // Former tls::TLS_HANDSHAKE_TIMEOUT.
        assert_eq!(p.tls_handshake_timeout, Duration::from_secs(10));
        assert!(p.evict_lane_on_transport_error);
        assert_eq!(p.stream_lane_pool_size, 1);
        assert_eq!(p.status_lane_pool_size, 4);
        assert_eq!(p.read_lane_pool_size, 2);
        assert_eq!(p.raft_lane_pool_size, 2);
        // Former server::WATCH_HEARTBEAT_INTERVAL.
        assert_eq!(p.watch_heartbeat_interval, Duration::from_secs(20));
    }

    #[test]
    fn configure_endpoint_applies_without_panicking_and_is_chainable() {
        // The Endpoint builder is opaque (no getters), so we assert the
        // factory accepts an endpoint and returns one — i.e. every knob set
        // is a method tonic still exposes. A behavioural assertion that the
        // connect timeout actually fires lives in the client integration
        // tests (`apply_outbox_aborts_on_per_call_deadline`).
        let policy = GrpcTransportPolicy {
            connect_timeout: Duration::from_millis(50),
            ..GrpcTransportPolicy::default()
        };
        for kind in [ChannelKind::InterNode, ChannelKind::ContainerdUds] {
            let endpoint = Endpoint::from_static("http://127.0.0.1:1");
            let _configured = policy.configure_endpoint(endpoint, kind);
        }
    }

    #[test]
    fn containerd_uds_sends_no_http2_keepalive_pings() {
        // Regression: the GrpcTransportPolicy unification (A1) routed the
        // kubelet→containerd channel through the inter-node keepalive cadence,
        // so klights PINGed containerd every 15s. containerd's server
        // keepalive enforcement GOAWAYed long unary PullImage calls with
        // too_many_pings, killing on-demand image pulls with a broken pipe.
        // The containerd UDS channel must send no client HTTP/2 keepalive.
        let p = GrpcTransportPolicy::default();
        assert!(
            p.http2_keepalive(ChannelKind::ContainerdUds).is_none(),
            "kubelet→containerd channel must not send client HTTP/2 keepalive PINGs"
        );
    }

    #[test]
    fn inter_node_keeps_http2_keepalive_pings() {
        let p = GrpcTransportPolicy::default();
        let (interval, timeout, while_idle) = p
            .http2_keepalive(ChannelKind::InterNode)
            .expect("inter-node channels still send HTTP/2 keepalive PINGs");
        assert_eq!(interval, Duration::from_secs(15));
        assert_eq!(timeout, Duration::from_secs(10));
        assert!(while_idle);
    }

    #[test]
    fn shared_helpers_wrap_in_arc() {
        let a = GrpcTransportPolicy::shared_default();
        let b = GrpcTransportPolicy::default().shared();
        assert_eq!(*a, *b);
    }
}
