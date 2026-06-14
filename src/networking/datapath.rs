//! `Datapath` — the narrow trait that carries pod-network setup, teardown,
//! and the orderly shutdown hook. Carved out of the umbrella
//! `NetworkProvider` (Task 4 of the network refactor) so callers depend
//! only on the surface they need.
//!
//! - Kubelet pod lifecycle paths take `&dyn Datapath` for `cni_add` /
//!   `cni_del`.
//! - The CNI RPC server binary takes `&dyn Datapath`.
//! - Shutdown sequencing calls `Datapath::shutdown` after the peer router
//!   has drained.
//!
//! Phase 2 rootless adds a `RootlessDatapath` that implements this trait
//! over pasta + bypass4netns; no caller signature changes.

use anyhow::Result;
use async_trait::async_trait;
use std::net::IpAddr;

use crate::networking::cni::PodNetwork;
use crate::networking::provider::CniAddRequest;

#[async_trait]
pub trait Datapath: Send + Sync + 'static {
    /// Wire pod network state for a freshly-created sandbox. Returns the
    /// allocated `PodNetwork` (pod IP).
    async fn cni_add(&self, request: CniAddRequest) -> Result<PodNetwork>;

    /// Tear down pod network state for a sandbox being deleted.
    async fn cni_del(&self, sandbox_id: &str) -> Result<()>;

    /// Host's primary underlay IP — the IP on the default-route
    /// interface. Cached at boot from the value supplied to
    /// `NetworkPlane::boot`; reading it is a no-I/O field load.
    async fn host_ip(&self) -> Result<IpAddr>;

    /// Node-local pod gateway IP for this datapath. On the leader this is the
    /// stable in-cluster address used by the default `kubernetes` Service
    /// Endpoints, so worker pods reach the apiserver through the pod dataplane
    /// instead of a public node endpoint.
    async fn pod_gateway_ip(&self) -> Result<IpAddr>;

    /// Drain and release datapath resources during process shutdown.
    async fn shutdown(&self) -> Result<()>;
}
