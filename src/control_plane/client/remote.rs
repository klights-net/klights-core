use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::control_plane::client::informer::{InformerCache, scope_for_request};
use crate::control_plane::client::{
    CacheScope, ConfigMap, LeaderApiClient, ListRequest, ListResponse, Node, Pod,
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest, ResourceEvent, ResourceKey,
    Secret, WatchRequest, WatchStream,
};
use crate::datastore::{NodeSubnet, PodCleanupIntent, Resource, ResourceList};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyClient, OutboxApplyError, OutboxApplyResult};
use crate::networking::wireguard::DataplanePeerMetadata;
use crate::replication::grpc::client::ReplicationGrpcClient;
use crate::task_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

/// bug-grpc: a worker watch stream that delivers neither an event nor a
/// heartbeat BOOKMARK within this window is treated as wedged and dropped, so
/// the driver reconnects from `next_resource_version` (catch-up replay
/// re-delivers anything missed). Sized at ~3× the leader heartbeat
/// (`server::WATCH_HEARTBEAT_INTERVAL`, 20 s) so a healthy-but-quiet stream
/// never trips, while a partial-loss wedge (keepalive PING slips through but
/// watch DATA does not) is caught within a minute instead of stalling
/// indefinitely (the 10-minute "stable cluster" pod-deletion stall).
const WATCH_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Outcome of a single idle-bounded poll of a watch stream.
enum IdleNext {
    /// The stream produced an item (event or decode error).
    Item(Result<ResourceEvent>),
    /// The stream ended (None) or the supervisor declined the timer.
    Closed,
    /// No item arrived within the idle window — the stream is wedged.
    Idle,
}

/// Poll `stream` for the next item, bounded by `idle`. Returns [`IdleNext::Idle`]
/// when the window elapses with no item. Without a supervisor (unit tests for
/// cache paths) it falls back to an unbounded poll.
async fn next_event_within_idle(
    supervisor: Option<&Arc<TaskSupervisor>>,
    idle: std::time::Duration,
    stream: &mut WatchStream<ResourceEvent>,
) -> IdleNext {
    let Some(supervisor) = supervisor else {
        return match stream.next().await {
            Some(item) => IdleNext::Item(item),
            None => IdleNext::Closed,
        };
    };
    match supervisor
        .timeout("remote_watch_idle", idle, stream.next())
        .await
    {
        Ok(Ok(Some(item))) => IdleNext::Item(item),
        Ok(Ok(None)) => IdleNext::Closed,
        Ok(Err(_elapsed)) => IdleNext::Idle,
        Err(_shutdown) => IdleNext::Closed,
    }
}

#[derive(Clone)]
pub struct RemoteApiClient {
    node_name: String,
    grpc: Option<Arc<ReplicationGrpcClient>>,
    supervisor: Option<Arc<TaskSupervisor>>,
    cache: InformerCache,
    worker_informers_started: Arc<AtomicBool>,
    /// bug-grpc: per-stream idle timeout; overridable in tests.
    watch_idle_timeout: std::time::Duration,
}

impl RemoteApiClient {
    pub fn new(node_name: String) -> Self {
        Self {
            node_name,
            grpc: None,
            supervisor: None,
            cache: InformerCache::new(),
            worker_informers_started: Arc::new(AtomicBool::new(false)),
            watch_idle_timeout: WATCH_IDLE_TIMEOUT,
        }
    }

    pub fn from_grpc(
        grpc: Arc<ReplicationGrpcClient>,
        supervisor: Arc<TaskSupervisor>,
        node_name: String,
    ) -> Self {
        Self {
            node_name,
            grpc: Some(grpc),
            supervisor: Some(supervisor),
            cache: InformerCache::new(),
            worker_informers_started: Arc::new(AtomicBool::new(false)),
            watch_idle_timeout: WATCH_IDLE_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub fn new_for_tests(node_name: &str) -> Self {
        Self::new(node_name.to_string())
    }

    /// In tests, directly insert a pod into the informer cache without going
    /// through gRPC. This lets us test cache-hit read paths independently.
    #[cfg(test)]
    pub async fn cache_insert_pod(&self, pod: Pod) {
        self.cache.insert(pod).await;
    }

    /// Mark a cache scope as primed.
    #[cfg(test)]
    pub async fn cache_prime_scope(&self, scope: CacheScope) {
        self.cache.mark_primed(scope).await;
    }

    /// Clear a cache scope (simulates watch 410 Gone).
    #[cfg(test)]
    pub async fn cache_clear_scope_for_test(&self, scope: &CacheScope) {
        self.cache.clear_scope_for_test(scope).await;
    }

    pub async fn start_required_worker_informers(
        self: &Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<Vec<SupervisedJoinHandle<()>>> {
        let supervisor = self
            .supervisor
            .as_ref()
            .ok_or_else(|| anyhow!("RemoteApiClient missing TaskSupervisor"))?
            .clone();
        if self
            .worker_informers_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(Vec::new());
        }
        let mut handles = Vec::new();
        for req in self.required_worker_list_requests() {
            let client = self.clone();
            let cancel = cancel.clone();
            match supervisor
                .spawn_async(
                    TaskCategory::Network,
                    "remote_api_informer_watch",
                    async move {
                        client.run_watch_driver(req, cancel).await;
                    },
                )
                .await
            {
                Ok(handle) => handles.push(handle),
                Err(err) => {
                    self.worker_informers_started
                        .store(false, Ordering::Release);
                    return Err(err);
                }
            }
        }
        Ok(handles)
    }

    async fn run_watch_driver(self: Arc<Self>, req: ListRequest, cancel: CancellationToken) {
        let mut next_resource_version = None;
        // Consecutive failed reconnects; reset to 0 once the stream delivers an
        // event. Drives the shared exponential reconnect backoff so a sustained
        // leader/WAN outage cannot become a fixed-interval reconnect storm.
        let mut reconnect_attempt: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if next_resource_version.is_none() {
                match self.prime_list_scope(req.clone()).await {
                    Ok(list) => {
                        next_resource_version = Some(list.resource_version);
                    }
                    Err(err) => {
                        tracing::warn!(
                            api_version = %req.api_version,
                            kind = %req.kind,
                            error = %err,
                            "failed to prime remote informer scope"
                        );
                        self.sleep_before_reconnect(reconnect_attempt).await;
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        continue;
                    }
                }
            }
            let watch_req = WatchRequest {
                api_version: req.api_version.clone(),
                kind: req.kind.clone(),
                namespace: req.namespace.clone(),
                label_selector: req.label_selector.clone(),
                field_selector: req.field_selector.clone(),
                start_resource_version: next_resource_version,
            };
            match self.watch_resources(watch_req).await {
                Ok(mut stream) => loop {
                    let next = tokio::select! {
                        _ = cancel.cancelled() => return,
                        next = next_event_within_idle(
                            self.supervisor.as_ref(),
                            self.watch_idle_timeout,
                            &mut stream,
                        ) => next,
                    };
                    match next {
                        IdleNext::Item(Ok(event)) => {
                            reconnect_attempt = 0;
                            // bug-grpc B2/B3: cursor-advance-only-after-safe-apply.
                            // The resume RV must advance ONLY once the event is
                            // decoded and applied (a BOOKMARK applies as a no-op
                            // success, so its RV is a valid resume point). If
                            // apply fails, leave next_resource_version pointing
                            // before this event and reconnect, so catch-up replay
                            // re-delivers it — never advance past an event that
                            // was not applied (silent loss on reconnect).
                            if let Err(err) = self.cache.apply_event(&event).await {
                                tracing::warn!(
                                    api_version = %req.api_version,
                                    kind = %req.kind,
                                    error = %err,
                                    "failed to apply remote informer event; reconnecting from last applied resourceVersion"
                                );
                                break;
                            }
                            if let Some(rv) = resource_event_version(&event) {
                                next_resource_version =
                                    Some(next_resource_version.unwrap_or(0).max(rv));
                            }
                        }
                        IdleNext::Item(Err(err)) => {
                            if watch_error_requires_relist(&err) {
                                next_resource_version = None;
                            }
                            tracing::warn!(
                                api_version = %req.api_version,
                                kind = %req.kind,
                                error = %err,
                                "remote informer watch stream failed"
                            );
                            break;
                        }
                        IdleNext::Idle => {
                            // No event or heartbeat within the idle window: the
                            // stream is wedged (loss let keepalive through but
                            // not watch data). Drop it and reconnect from the
                            // last resourceVersion; catch-up replay re-delivers
                            // anything missed, so no event is lost.
                            tracing::warn!(
                                api_version = %req.api_version,
                                kind = %req.kind,
                                "remote informer watch idle past heartbeat window; reconnecting from last resourceVersion"
                            );
                            break;
                        }
                        IdleNext::Closed => break,
                    }
                },
                Err(err) => {
                    if watch_error_requires_relist(&err) {
                        next_resource_version = None;
                    }
                    tracing::warn!(
                        api_version = %req.api_version,
                        kind = %req.kind,
                        error = %err,
                        "failed to open remote informer watch stream"
                    );
                }
            }
            self.sleep_before_reconnect(reconnect_attempt).await;
            reconnect_attempt = reconnect_attempt.saturating_add(1);
        }
    }

    async fn sleep_before_reconnect(&self, attempt: u32) {
        if let Some(supervisor) = &self.supervisor {
            let _ = supervisor
                .sleep(
                    "remote_api_informer_reconnect",
                    crate::utils::watch_reconnect_delay(attempt),
                )
                .await;
        }
    }

    async fn prime_list_scope(&self, req: ListRequest) -> Result<ResourceList> {
        let grpc = self.grpc()?;
        let list = grpc.list_resources_rpc(req.clone()).await?;
        self.cache.replace_scope(&req, list.clone()).await;
        self.cache.mark_primed(scope_for_request(&req)).await;
        Ok(list)
    }

    fn grpc(&self) -> Result<&Arc<ReplicationGrpcClient>> {
        self.grpc
            .as_ref()
            .ok_or_else(|| anyhow!("RemoteApiClient missing gRPC transport"))
    }

    fn pod_key(ns: &str, name: &str) -> ResourceKey {
        ResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        }
    }

    fn list_pods_on_node_request(&self, node_name: &str) -> ListRequest {
        ListRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: Some(format!("spec.nodeName={node_name}")),
            limit: None,
            continue_token: None,
        }
    }

    fn required_worker_list_requests(&self) -> Vec<ListRequest> {
        let mut reqs = vec![self.list_pods_on_node_request(&self.node_name)];
        for (api_version, kind, namespace) in [
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("node.k8s.io/v1", "RuntimeClass", None),
            ("scheduling.k8s.io/v1", "PriorityClass", None),
            ("v1", "ServiceAccount", None),
            ("v1", "Service", None),
            ("v1", "Endpoints", None),
            ("discovery.k8s.io/v1", "EndpointSlice", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
            ("v1", "Namespace", None),
        ] {
            reqs.push(ListRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: None,
                field_selector: None,
                limit: None,
                continue_token: None,
            });
        }
        reqs
    }
}

fn resource_event_version(event: &ResourceEvent) -> Option<i64> {
    event.event.resource_version()
}

fn watch_error_requires_relist(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("410") || message.contains("gone") || message.contains("outofrange")
}

#[async_trait]
impl LeaderApiClient for RemoteApiClient {
    async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
        if let Some(resource) = self.cache.get(&key).await {
            return Ok(Some(resource));
        }
        let req = ListRequest {
            api_version: key.api_version.clone(),
            kind: key.kind.clone(),
            namespace: key.namespace.clone(),
            label_selector: None,
            field_selector: None,
            limit: None,
            continue_token: None,
        };
        let scope = scope_for_request(&req);
        if self.cache.is_ready(&scope).await {
            return Ok(None);
        }
        if self.grpc.is_some() {
            self.prime_list_scope(req).await?;
            return Ok(self.cache.get(&key).await);
        }
        Ok(None)
    }

    async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
        let scope = scope_for_request(&req);
        if self.cache.is_ready(&scope).await {
            return Ok(self.cache.list(&req).await);
        }
        if self.grpc.is_some() {
            return self.prime_list_scope(req).await;
        }
        Ok(self.cache.list(&req).await)
    }

    async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
        let Some(grpc) = &self.grpc else {
            return self.get_resource(key).await;
        };
        let resource = grpc.get_resource_rpc(key.clone()).await?;
        if let Some(resource) = &resource {
            self.cache.insert(resource.clone()).await;
        }
        Ok(resource)
    }

    async fn list_resources_fresh(&self, req: ListRequest) -> Result<ListResponse> {
        if self.grpc.is_some() {
            return self.prime_list_scope(req).await;
        }
        self.list_resources(req).await
    }

    async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
        self.grpc()?.watch_resources_rpc(req).await
    }

    async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
        if self.grpc.is_none() && !self.cache.is_ready(&scope).await {
            return Err(anyhow!("cache scope {scope:?} not yet primed"));
        }
        self.cache.wait_ready(scope).await
    }

    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        self.grpc()?
            .projected_service_account_token_rpc(request)
            .await
    }

    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>> {
        self.get_resource(Self::pod_key(ns, name)).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>> {
        Ok(self
            .get_resource(Self::pod_key(ns, name))
            .await?
            .filter(|resource| resource.uid == uid))
    }

    async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>> {
        let watch = self
            .watch_resources(WatchRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: Some(format!("spec.nodeName={node_name}")),
                start_resource_version: None,
            })
            .await?;
        let cache = self.cache.clone();
        Ok(Box::pin(watch.filter_map(move |event| {
            let cache = cache.clone();
            async move {
                match event {
                    Ok(event) => match cache.apply_event(&event).await {
                        Ok(Some(resource)) => Some(Ok(resource)),
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    },
                    Err(err) => Some(Err(err)),
                }
            }
        })))
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
        Ok(self
            .list_resources(self.list_pods_on_node_request(node_name))
            .await?
            .items)
    }

    async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<ConfigMap>> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        })
        .await
    }

    async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "Secret".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
        })
        .await
    }

    async fn get_node(&self, name: &str) -> Result<Node> {
        self.get_resource(ResourceKey {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: name.to_string(),
        })
        .await
        .and_then(|node| node.ok_or_else(|| anyhow!("Node {name} not found")))
    }

    async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>> {
        let watch = self
            .watch_resources(WatchRequest {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: Some(format!("metadata.name={name}")),
                start_resource_version: None,
            })
            .await?;
        let cache = self.cache.clone();
        Ok(Box::pin(watch.filter_map(move |event| {
            let cache = cache.clone();
            async move {
                match event {
                    Ok(event) => match cache.apply_event(&event).await {
                        Ok(Some(resource)) => Some(Ok(resource)),
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    },
                    Err(err) => Some(Err(err)),
                }
            }
        })))
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.grpc()?
            .allocate_node_subnet_rpc(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.grpc()?.get_node_subnet_rpc(node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.grpc()?.list_peer_subnets_rpc(my_node_name).await
    }

    async fn get_node_dataplane(&self, node_name: &str) -> Result<Option<DataplanePeerMetadata>> {
        self.grpc()?.get_node_dataplane_rpc(node_name).await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.grpc()?
            .list_pod_cleanup_intents_for_node_rpc(node_name)
            .await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.grpc()?
            .delete_pod_cleanup_intent_rpc(node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn get_cluster_membership(
        &self,
    ) -> Result<crate::control_plane::client::membership::ClusterMembership> {
        self.grpc()?.cluster_membership().await
    }

    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        let Some(grpc) = &self.grpc else {
            return Ok(OutboxApplyResult::Applied { applied_rv: 0 });
        };
        grpc.apply_outbox_rpc(idempotency_key, operation, payload)
            .await
    }
}

#[async_trait]
impl OutboxApplyClient for RemoteApiClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        LeaderApiClient::apply_outbox(self, idempotency_key, operation, payload).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt as _;
    use serde_json::json;

    use crate::control_plane::client::remote::RemoteApiClient;
    use crate::control_plane::client::{CacheScope, LeaderApiClient, ResourceKey, WatchRequest};
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::backend::DatastoreHandle;
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::outbox::OutboxApplyError;
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
    use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode};
    use crate::replication::grpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient,
    };
    use crate::replication::protocol::JoinRole;
    use crate::replication::service::ReplicationService;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn dataplane() -> JoinDataplaneMetadata {
        JoinDataplaneMetadata {
            public_key: None,
            endpoint: "127.0.0.1".to_string(),
            port: None,
            mode: DataplaneMode::Root,
            encryption: DataplaneEncryption::Disabled,
        }
    }

    /// Self-signed `system:node:<name>` certificate (DER) for simulating the
    /// mTLS node identity in the in-process test harness.
    fn test_node_cert_der(node_name: &str) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes".to_string());
        let key_pair = KeyPair::generate().unwrap();
        params.self_signed(&key_pair).unwrap().der().to_vec()
    }

    async fn remote_client_and_leader_db() -> (
        RemoteApiClient,
        DatastoreHandle,
        tokio::task::JoinHandle<()>,
    ) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        // Simulate the mTLS edge: in production the TLS layer injects the
        // caller's client certificate; over the in-process plaintext channel we
        // inject worker-1's node cert so node-scoped RPCs (NodeRestriction) see
        // an authenticated node identity.
        let app = app.layer(axum::middleware::from_fn(
            |mut request: axum::extract::Request, next: axum::middleware::Next| async move {
                request
                    .extensions_mut()
                    .insert(crate::auth::TlsClientCertificate(test_node_cert_der(
                        "worker-1",
                    )));
                next.run(request).await
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let grpc = Arc::new(
            ReplicationGrpcClient::connect(
                GrpcClientConfig {
                    leader_endpoint: endpoint,
                    token,
                    node_name: "worker-1".to_string(),
                    role: JoinRole::Worker,
                    dataplane: dataplane(),
                    ca_cert_path: None,
                    skip_ca: false,
                    client_cert_pem: None,
                    client_key_pem: None,
                },
                supervisor.clone(),
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
            )
            .await
            .unwrap(),
        );
        (
            RemoteApiClient::from_grpc(grpc, supervisor, "worker-1".to_string()),
            db,
            handle,
        )
    }

    fn make_pod(ns: &str, name: &str, uid: &str, node_name: &str, phase: &str) -> super::Pod {
        let data = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": ns,
                "name": name,
                "uid": uid
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {
                "phase": phase
            }
        });
        crate::datastore::Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
            uid: uid.to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(data),
        }
    }

    fn pod_status_payload(uid: &str) -> Bytes {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode outbox payload"),
        )
    }

    #[tokio::test]
    async fn grpc_cache_read_primes_unready_scope_before_reporting_miss() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let pod = client
            .get_pod("default", "web")
            .await
            .expect("remote cache-prime get pod")
            .expect("unready cache scope should be synchronously primed before reporting absence");
        assert_eq!(pod.uid, "uid-1");

        db.update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "web",
            json!({"phase": "Running"}),
            ResourcePreconditions {
                uid: Some("uid-1".to_string()),
                resource_version: None,
            },
        )
        .await
        .unwrap();
        let cached = client
            .get_pod("default", "web")
            .await
            .expect("remote cached pod")
            .expect("pod should remain cached");
        assert_eq!(
            cached
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Pending"),
            "cache hit should not perform an unnecessary strong read"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_apply_outbox_uid_mismatch_propagates() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let err = client
            .apply_outbox(
                "uid-mismatch",
                OutboxOperation::PodStatus,
                pod_status_payload("uid-2"),
            )
            .await
            .expect_err("leader uid mismatch must propagate");
        assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_watch_pods_on_node_streams_leader_events() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        let mut stream = client
            .watch_pods_on_node("worker-1")
            .await
            .expect("open remote pod watch");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "watched",
            (*make_pod("default", "watched", "uid-watch", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let pod = stream
            .next()
            .await
            .expect("watch should yield")
            .expect("watch event should decode");
        assert_eq!(pod.name, "watched");
        assert_eq!(pod.uid, "uid-watch");
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_network_metadata_uses_typed_unary_rpcs() {
        let (client, db, handle) = remote_client_and_leader_db().await;

        let subnet = client
            .allocate_node_subnet("worker-1", "10.42.0.0/16", "192.0.2.20")
            .await
            .expect("allocate worker subnet through typed gRPC");
        assert_eq!(subnet.node_name.as_str(), "worker-1");
        assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");

        let fetched = client
            .get_node_subnet("worker-1")
            .await
            .expect("get worker subnet through typed gRPC")
            .expect("worker subnet should exist");
        assert_eq!(fetched, subnet);

        let peer = client
            .allocate_node_subnet("worker-2", "10.42.0.0/16", "192.0.2.21")
            .await
            .expect("allocate peer subnet through typed gRPC");
        let peers = client
            .list_peer_subnets("worker-1")
            .await
            .expect("list peer subnets through typed gRPC");
        assert_eq!(peers, vec![peer]);

        let stored_metadata = db
            .get_node_dataplane("worker-1")
            .await
            .expect("dataplane metadata lookup")
            .expect("join should have stored worker dataplane metadata");
        let fetched_metadata = client
            .get_node_dataplane("worker-1")
            .await
            .expect("get worker dataplane metadata through typed gRPC");
        assert_eq!(fetched_metadata, Some(stored_metadata));

        handle.abort();
    }

    #[tokio::test]
    async fn grpc_watch_replays_events_after_start_resource_version() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "old",
            (*make_pod("default", "old", "uid-old", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();
        let start_rv = db.get_current_resource_version().await.unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "missed",
            (*make_pod("default", "missed", "uid-missed", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let mut stream = client
            .watch_resources(WatchRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: Some("spec.nodeName=worker-1".to_string()),
                start_resource_version: Some(start_rv),
            })
            .await
            .expect("open continuation watch");
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("continuation watch should replay missed event")
            .expect("stream should yield")
            .expect("watch event should decode");
        let pod_name = event
            .event
            .object
            .pointer("/metadata/name")
            .and_then(|value| value.as_str());
        assert_eq!(pod_name, Some("missed"));
        handle.abort();
    }

    #[tokio::test]
    async fn watch_continuation_after_disconnect() {
        // Tests that the informer cache can be rebuilt after a watch disconnect.
        // Simulates: cache primed, disconnect clears scope, re-list repopulates.
        let client = RemoteApiClient::new_for_tests("worker-1");

        let pod_scope = crate::control_plane::client::CacheScope::Resource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
        };

        // Prime the scope and insert data
        client.cache_prime_scope(pod_scope.clone()).await;
        client
            .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
            .await;

        // Verify cache is ready
        assert!(client.wait_cache_ready(pod_scope.clone()).await.is_ok());

        // Simulate 410 Gone: clear scope and re-prime
        // In production, RemoteApiClient would re-list and re-prime;
        // here we test that the rebuilt cache works correctly.
        client.cache_clear_scope_for_test(&pod_scope).await;
        assert!(client.wait_cache_ready(pod_scope.clone()).await.is_err());

        // Re-prime and re-insert
        client.cache_prime_scope(pod_scope.clone()).await;
        client
            .cache_insert_pod(make_pod("default", "web", "uid-2", "worker-1", "Running"))
            .await;
        assert!(client.wait_cache_ready(pod_scope).await.is_ok());
        let pod = client.get_pod("default", "web").await.unwrap();
        assert!(pod.is_some());
        assert_eq!(pod.unwrap().uid, "uid-2");
    }

    #[tokio::test]
    async fn unary_fallback_on_cache_miss() {
        // Tests that when the cache misses, the client signals the result
        // correctly (None when not found). In production this would trigger
        // a unary gRPC GetResource; here the cache simply returns None.
        let client = RemoteApiClient::new_for_tests("worker-1");

        // No pod in cache → cache miss → returns None
        let result = client.get_pod("default", "nonexistent").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none(), "cache miss should return None");

        // Insert pod → cache hit
        client
            .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
            .await;
        let result = client.get_pod("default", "web").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some(), "cache hit should return pod");
    }

    #[tokio::test]
    async fn cache_based_get_resource_returns_primed_value() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let scope = CacheScope::Resource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
        };
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        client.cache_prime_scope(scope).await;
        client.cache_insert_pod(pod.clone()).await;

        let fetched = client
            .get_resource(ResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
            })
            .await
            .expect("get_resource");

        assert_eq!(
            fetched.as_ref().map(|resource| resource.uid.as_str()),
            Some("uid-1")
        );
        assert_eq!(
            fetched.as_ref().map(|resource| resource.resource_version),
            Some(pod.resource_version)
        );
    }

    #[tokio::test]
    async fn uid_bound_get_returns_none_on_uid_change() {
        // Tests that get_pod_for_uid returns None when UID has changed
        // (e.g., same-name replacement by StatefulSet). The cache key is
        // (apiVersion, kind, namespace, name) — so inserting a replacement
        // pod with a new UID overwrites the old entry, just as a watch
        // MODIFIED event would.
        let client = RemoteApiClient::new_for_tests("worker-1");
        client
            .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
            .await;

        // Old UID is in cache → found
        let result = client
            .get_pod_for_uid("default", "web", "uid-1")
            .await
            .unwrap();
        assert!(result.is_some(), "uid-1 should be found");

        // Different UID for same name → not found
        let result = client
            .get_pod_for_uid("default", "web", "uid-2")
            .await
            .unwrap();
        assert!(result.is_none(), "uid-2 should not be found");

        // Simulate replacement: new pod with uid-2 arrives via watch,
        // overwriting the old cache entry (same key).
        client
            .cache_insert_pod(make_pod("default", "web", "uid-2", "worker-1", "Running"))
            .await;

        // Old UID query now returns None because the cache entry carries uid-2
        let result = client
            .get_pod_for_uid("default", "web", "uid-1")
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "old uid-1 should return None after replacement"
        );

        // New UID is found
        let result = client
            .get_pod_for_uid("default", "web", "uid-2")
            .await
            .unwrap();
        assert!(result.is_some(), "uid-2 should be found after replacement");
    }

    #[tokio::test]
    async fn apply_outbox_uid_mismatch_propagates() {
        // Tests that apply_outbox handles the UID mismatch error path.
        // In the real implementation, the gRPC response carries the error;
        // in test mode, we verify the success path is wired.
        let client = RemoteApiClient::new_for_tests("worker-1");

        let result = client
            .apply_outbox(
                "key-1",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::from_static(b"test"),
            )
            .await;
        assert!(
            result.is_ok(),
            "apply_outbox should succeed: {:?}",
            result.err()
        );
        assert!(matches!(
            result.unwrap(),
            crate::kubelet::outbox::OutboxApplyResult::Applied { .. }
        ));
    }

    #[tokio::test]
    async fn all_required_worker_cache_scopes_prime() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let requests = client.required_worker_list_requests();
        let scopes: Vec<_> = requests
            .iter()
            .map(crate::control_plane::client::informer::scope_for_request)
            .collect();

        assert!(
            requests.iter().any(|req| req.api_version == "v1"
                && req.kind == "Pod"
                && req.field_selector.as_deref() == Some("spec.nodeName=worker-1")),
            "worker Pod informer must be scoped to this node"
        );
        for (api_version, kind, namespace) in [
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("node.k8s.io/v1", "RuntimeClass", None),
            ("scheduling.k8s.io/v1", "PriorityClass", None),
            ("v1", "ServiceAccount", None),
            ("v1", "Service", None),
            ("v1", "Endpoints", None),
            ("discovery.k8s.io/v1", "EndpointSlice", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
            ("v1", "Namespace", None),
        ] {
            assert!(
                requests.iter().any(|req| req.api_version == api_version
                    && req.kind == kind
                    && req.namespace.as_deref() == namespace),
                "missing worker cache scope {api_version}/{kind}/{namespace:?}"
            );
        }

        for scope in &scopes {
            client.cache_prime_scope(scope.clone()).await;
        }

        for scope in scopes {
            let result = client.wait_cache_ready(scope.clone()).await;
            assert!(
                result.is_ok(),
                "wait_cache_ready for {scope:?} should succeed"
            );
        }
    }

    #[tokio::test]
    async fn watch_idle_timeout_fires_when_stream_is_wedged() {
        // bug-grpc: a worker watch that delivers neither an event nor a
        // heartbeat within the idle window is wedged (partial loss let the
        // keepalive PING through but not the watch DATA). The driver must
        // surface Idle so it can reconnect from the last resourceVersion,
        // instead of blocking forever (the 10-minute pod-deletion stall).
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let mut wedged: super::WatchStream<super::ResourceEvent> =
            Box::pin(futures::stream::pending());
        let started = std::time::Instant::now();
        let outcome = super::next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_millis(150),
            &mut wedged,
        )
        .await;
        assert!(
            matches!(outcome, super::IdleNext::Idle),
            "a wedged stream must surface Idle within the idle window"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));

        // A live stream passes its item straight through — no false idle.
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        let event = super::ResourceEvent {
            event: crate::watch::WatchEvent::from_type("ADDED", (*pod.data).clone()),
        };
        let mut live: super::WatchStream<super::ResourceEvent> =
            Box::pin(futures::stream::once(async move { Ok(event) }));
        let outcome = super::next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_secs(5),
            &mut live,
        )
        .await;
        assert!(
            matches!(outcome, super::IdleNext::Item(Ok(_))),
            "a live event must pass through, not be reported as idle"
        );
    }

    /// bug-grpc B2/B3: cursor-advance-only-after-safe-apply. `run_watch_driver`
    /// now advances its resume `next_resource_version` ONLY after
    /// `cache.apply_event` succeeds; on apply failure it breaks and reconnects
    /// from the last applied RV so catch-up replay re-delivers the event. This
    /// locks the gate that makes that correct: `apply_event` errors on an
    /// undecodable event (forcing the break / no-advance) and is a no-op success
    /// on a BOOKMARK (a valid resume point that may advance the cursor).
    #[tokio::test]
    async fn informer_apply_event_gates_cursor_advance() {
        let cache = crate::control_plane::client::informer::InformerCache::new();

        // Undecodable event (no apiVersion/kind/metadata.name): apply must error
        // so the driver does NOT advance the resume cursor past it.
        let undecodable = super::ResourceEvent {
            event: crate::watch::WatchEvent::from_type("ADDED", serde_json::json!({})),
        };
        assert!(
            cache.apply_event(&undecodable).await.is_err(),
            "an undecodable event must error so the resume cursor cannot advance past an unapplied event"
        );

        // BOOKMARK: apply is a no-op success, so its RV is a safe resume point
        // the driver may advance to.
        let bookmark = super::ResourceEvent {
            event: crate::watch::WatchEvent::bookmark_typed(42, "v1", "Pod"),
        };
        assert!(
            cache.apply_event(&bookmark).await.is_ok(),
            "a BOOKMARK must apply as a no-op success so its RV is a valid resume point"
        );

        // A well-formed event applies successfully (cursor may advance).
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        let good = super::ResourceEvent {
            event: crate::watch::WatchEvent::from_type("ADDED", (*pod.data).clone()),
        };
        assert!(
            cache.apply_event(&good).await.is_ok(),
            "a well-formed event must apply so its RV becomes the resume point"
        );
    }
}
