//! `ServiceRouter` — the narrow trait for klights service-routing
//! operations. Owns one nft `inet <table>`, one persistent netlink
//! socket, the coalescer worker, and the per-instance hostport state
//! that used to live in process-wide `OnceLock` registries.
//!
//! Carved out of the umbrella `NetworkProvider` (Task 5 of the network
//! refactor) so callers depend only on the surface they need and the
//! former process-wide runtime global plus hostport registries are gone.
//!
//! Construction is via [`crate::networking::service_routing::NftServiceRouter::boot`];
//! the resulting `Arc<dyn ServiceRouter>` is stashed on AppState.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::net::Ipv4Addr;

#[async_trait]
pub trait ServiceRouter: Send + Sync + 'static {
    /// Coalesced request to re-sync service routing rules from the
    /// datastore. Cheap and idempotent — many rapid calls collapse into
    /// one sync per coalescing window. Synchronous (no `await`); the
    /// actual sync runs on the coalescer worker.
    fn request_services_sync(&self);

    /// Synchronous service sync — rebuilds the services chain immediately
    /// from the datastore. Used by service reconcile paths that need the
    /// updated nft rules to take effect before returning (e.g. session
    /// affinity changes must be reflected before the PATCH response is
    /// sent, otherwise E2E tests checking immediately after the PATCH may
    /// see stale jhash rules).
    async fn sync_services_now(&self) -> Result<()>;

    /// Add a pod's hostPort declarations to the `hostports` chain. No-op
    /// for pods without hostPorts (legitimate "nothing to program").
    async fn add_hostport_rules(&self, pod: &Value, pod_ip: Ipv4Addr) -> Result<()>;

    /// Remove a pod's hostPort entries from the `hostports` chain on pod
    /// deletion. No-op if the pod never had a recorded IP (legitimate
    /// "nothing to program").
    async fn remove_hostport_rules(&self, pod: &Value) -> Result<()>;

    /// Drop the `inet <table>` table on shutdown. One-shot; missing
    /// tables are tolerated.
    async fn cleanup(&self) -> Result<()>;
}
