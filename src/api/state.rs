use crate::bootstrap::{NodeMode, NodeRole};
use crate::control_plane::client::LeaderApiClient;
use crate::controllers::crd::CrdRegistry;
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::kubelet::pod_creation_state::PodStartRetryTracker;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    /// Datastore abstraction handle (`Arc<dyn DatastoreBackend>`).
    pub db: DatastoreHandle,
    /// Authorization chain. Every API request is evaluated against this chain
    /// after authentication. Tests can inject a mock; production wires
    /// system:masters bypass → bootstrap CSR → node → RBAC → deny.
    pub authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer>,
    /// Structured Kubernetes audit event stream for authentication and
    /// authorization decisions.
    pub audit_sink: std::sync::Arc<dyn crate::audit::AuditSink>,
    pub rbac_policy_store: std::sync::Arc<dyn crate::auth::rbac_policy_store::RbacPolicyStore>,
    /// Kubelet-facing cluster-state API. Leader mode uses the in-process
    /// LocalApiClient; remote worker implementations arrive with T4.
    pub cluster_api: std::sync::Arc<dyn LeaderApiClient>,
    pub crd_registry: CrdRegistry,
    /// Operating mode detected once at process startup. Read by reference;
    /// no setter exists, so re-detection requires a process restart.
    pub mode: NodeMode,
    pub role: NodeRole,
    pub replication: Option<std::sync::Arc<crate::replication::ReplicationService>>,
    pub config: std::sync::Arc<crate::KlightsConfig>,
    /// App-owned networking surface — Datapath, PeerRouter, ServiceRouter,
    /// PodEndpointResolver in one struct. Folded together in Task 7 of
    /// the network refactor; replaces the previous separate
    /// `network: Arc<dyn NetworkProvider>` and
    /// `services: Arc<dyn ServiceRouter>` fields.
    pub network: std::sync::Arc<crate::networking::Network>,
    pub service_ipam: std::sync::Arc<crate::controllers::service::ServiceIpam>,
    /// Bootstrapped NodePort allocator.  P3b-1 routes Service create/update
    /// through `controller_dispatcher`, so the live allocator is the one
    /// the dispatcher's `ServiceController` holds (constructed from the same
    /// `Arc` in bootstrap).  This field is retained so test fixtures that
    /// build `AppState` directly can still wire it the old way.
    pub nodeport_alloc: std::sync::Arc<crate::controllers::service::NodePortAllocator>,
    pub cri: Option<std::sync::Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    pub controller_dispatcher: std::sync::Arc<crate::controller_dispatcher::ControllerDispatcher>,
    /// Centralized post-mutation side-effect dispatch (P3d-1). HTTP mutation
    /// handlers call [`SideEffectRegistry::run_hooks`] after their datastore
    /// write succeeds. Controller-like side effects enqueue reconcile intents
    /// through `controller_dispatcher` instead of running controllers inline.
    pub side_effects: std::sync::Arc<crate::side_effects::SideEffectRegistry>,
    /// Counters for side-effect and cascade-delete failures, exposed at `/metrics`.
    pub metrics: std::sync::Arc<crate::side_effects::SideEffectMetrics>,
    /// Per-process APIService client identity cache. Certificate rotation is
    /// picked up by restarting klights, matching the rest of bootstrap TLS.
    pub apiservice_proxy_identity_cache: std::sync::Arc<tokio::sync::OnceCell<reqwest::Identity>>,
    /// APIService aggregation cache. Backend specs are invalidated on
    /// APIService mutations; endpoint resolution stays per request so Service
    /// endpoint changes are observed without polling.
    pub apiservice_proxy_cache: std::sync::Arc<crate::api::apiservice_proxy::ApiServiceProxyCache>,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    /// Single-instance pod persistence boundary owned by the process.
    /// Routes pod persistence through repository traits instead of ad-hoc
    /// `db.{create,update,update_status_only,...}` calls against
    /// `("v1","Pod",...)` across kubelet, controllers, and API handlers.
    pub pod_repository: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    pub outbox: std::sync::Arc<crate::kubelet::outbox::Outbox>,
    /// Leader-local node heartbeat state. Worker Lease renewals update this
    /// in memory through the dedicated heartbeat path; node lifecycle writes
    /// cluster-visible Node status only when liveness actually transitions.
    pub node_lease_tracker: std::sync::Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    /// Pod lifecycle router — unified facade over actor or multiplex transport.
    pub pod_lifecycle_router:
        Option<std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>>,
    pub pod_probe_manager: Option<std::sync::Arc<crate::kubelet::ProbeManager>>,
    pub pod_lifecycle_rx: Option<
        std::sync::Arc<
            tokio::sync::Mutex<
                Option<tokio::sync::mpsc::Receiver<crate::kubelet::lifecycle::LifecycleCommand>>,
            >,
        >,
    >,
    /// Shared pod startup retry state for endpoint diagnostics.
    pub pod_start_retry_state: Option<PodStartRetryTracker>,
    /// Raft leadership flag for API request gating. `None` on single-node
    /// and worker boots (always allowed). `Some(receiver)` on raft
    /// controlplanes, including learner replicas — the middleware reads the current value to decide
    /// whether to serve K8s API requests or return 503.
    pub is_raft_leader_rx: Option<std::sync::Arc<crate::api::raft_proxy::RaftLeaderProxy>>,
    /// Optional OIDC token authenticator. When configured, validates bearer
    /// tokens against an external OIDC provider (Keycloak, Dex, Azure AD).
    pub oidc_authenticator: Option<Arc<dyn crate::auth::oidc::OidcValidator>>,
    /// Optional webhook token authenticator. When configured, validates bearer
    /// tokens by calling an external TokenReview webhook.
    pub webhook_authenticator: Option<Arc<crate::auth::webhook_auth::WebhookAuth>>,
    /// Cluster CA certificate PEM. Used by the leader to *cryptographically*
    /// re-authenticate a client certificate forwarded by a follower API proxy
    /// (the `x-remote-client-certificate` header): the leaf must carry a valid
    /// signature from this CA before its subject (CN/O, including
    /// `system:masters`) is trusted. `None` disables forwarded-cert auth (e.g.
    /// single-node or tests), in which case the leader falls back to the
    /// header-asserted requestheader identity.
    pub cluster_ca_pem: Option<Arc<String>>,
}

impl AppState {
    /// Borrow the datastore as a trait object for helpers that depend on the
    /// abstraction boundary rather than the concrete type.
    ///
    /// Equivalent to `&self.db as &dyn DatastoreBackend`.  Provided so call
    /// sites can be ported in-place as helper signatures migrate from
    /// `&Datastore` to `&dyn DatastoreBackend`.
    pub fn db_backend(&self) -> &dyn DatastoreBackend {
        self.db.as_ref()
    }
}
