//! Focused controller/reconcile/effect fakes shared by cross-crate
//! integration tests.
//!
//! These fakes exist to prove the one-way Pod-to-Service reconcile edge and
//! the persistence-before-effects ordering required for actor-owned Pod
//! deletion; they hold no root datastore/sequencer/control-plane internals.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use klights_pod_api::{PodGetRequest, PodQuery};
use klights_reconcile_api::{
    ControllerReconcileSink, PodMutationReconcileRequest, PodMutationReconcileSink, ReconcileKey,
    ReconcileSinkError, ReconcileSinkFuture, ServiceReconcileKey, ServiceReconcileSink,
};

/// Deterministic Kubernetes identity source for controller and GC fixtures.
///
/// The fixture is test-support-only and keeps names plus RFC-4122-v4-shaped
/// UIDs reproducible without consulting root composition or ambient entropy.
#[derive(Default)]
pub struct DeterministicControllerIdentity {
    next: AtomicU64,
}

impl DeterministicControllerIdentity {
    pub fn with_start(value: u64) -> Self {
        Self {
            next: AtomicU64::new(value),
        }
    }

    fn generated_name(prefix: &str, value: u64) -> String {
        const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        const SUFFIX_SPACE: u64 = 36_u64.pow(5);
        let mut remaining = value % SUFFIX_SPACE;
        let mut suffix = [b'0'; 5];
        for slot in suffix.iter_mut().rev() {
            *slot = ALPHABET[(remaining % 36) as usize];
            remaining /= 36;
        }
        format!(
            "{prefix}{}",
            std::str::from_utf8(&suffix).expect("ASCII suffix")
        )
    }

    fn uuid_v4(value: u64) -> String {
        let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
        let second = (value >> 32) & 0xffff;
        let third = 0x4000 | ((value >> 48) & 0x0fff);
        let fourth = 0x8000 | ((value >> 60) & 0x000f);
        format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
    }
}

impl crate::ControllerIdentityGenerator for DeterministicControllerIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, Ordering::Relaxed);
        Self::generated_name(prefix, value)
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, Ordering::Relaxed);
        Self::uuid_v4(value)
    }
}

/// Canonical controller-runtime fixture retaining the exact dispatcher and
/// supervisor already assembled by the root. It deliberately exposes only
/// named queue/worker operations, never either inner port or a trait escape.
#[derive(Clone)]
pub struct ControllerRuntimeFixture {
    dispatcher: Arc<crate::ControllerDispatcher>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    execution_active: Arc<AtomicBool>,
}

/// Private RAII exclusion lease shared by every clone of the fixture.
///
/// Holding this lease means one deterministic drain or one supervised worker
/// owns the dispatcher execution path. Dropping it releases admission after
/// normal completion, cancellation, join failure, or panic unwinding.
struct ExecutionLease(Arc<AtomicBool>);

const MAX_DRAINED_KEYS: usize = 1024;

/// Private accounting for the bounded deterministic drain. It rejects before
/// taking a 1025th key, so the excess remains ready for the next execution.
#[derive(Default)]
struct DrainBudget {
    dispatched: usize,
}

impl DrainBudget {
    fn may_dispatch(&self) -> bool {
        self.dispatched < MAX_DRAINED_KEYS
    }

    fn record_dispatch(&mut self) {
        self.dispatched += 1;
    }
}

impl ExecutionLease {
    fn acquire(active: Arc<AtomicBool>) -> anyhow::Result<Self> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| anyhow::anyhow!("controller runtime fixture execution is busy"))?;
        Ok(Self(active))
    }
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl ControllerRuntimeFixture {
    pub fn new(
        dispatcher: Arc<crate::ControllerDispatcher>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            dispatcher,
            supervisor,
            execution_active: Arc::new(AtomicBool::new(false)),
        }
    }

    fn acquire_execution(&self) -> anyhow::Result<ExecutionLease> {
        if self.dispatcher.worker_running() {
            anyhow::bail!(
                "controller runtime fixture execution is busy: dispatcher worker is active"
            );
        }
        let lease = ExecutionLease::acquire(self.execution_active.clone())?;
        if self.dispatcher.worker_running() {
            drop(lease);
            anyhow::bail!(
                "controller runtime fixture execution is busy: dispatcher worker is active"
            );
        }
        Ok(lease)
    }

    pub async fn queued_reconcile_keys(&self) -> Vec<ReconcileKey> {
        klights_reconcile_api::ControllerDispatcherPort::pending_reconcile_keys(
            self.dispatcher.as_ref(),
        )
        .await
    }

    pub async fn spawn_worker(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<klights_supervisor::SupervisedJoinHandle<()>> {
        let lease = self.acquire_execution()?;
        let dispatcher = self.dispatcher.clone();
        self.supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "native_api_controller_worker",
                async move {
                    let _lease = lease;
                    dispatcher.run_worker_pool(1, cancel).await;
                },
            )
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn drain_ready(&self) -> anyhow::Result<Vec<ReconcileKey>> {
        let _lease = self.acquire_execution()?;
        let mut drained = Vec::new();
        let mut budget = DrainBudget::default();
        loop {
            if !budget.may_dispatch() {
                if self.queued_reconcile_keys().await.is_empty() {
                    return Ok(drained);
                }
                anyhow::bail!(
                    "controller runtime fixture drain bound exceeded after {MAX_DRAINED_KEYS} dispatched keys"
                );
            }
            let Some(key) = self
                .dispatcher
                .dispatch_one_ready_for_test_support()
                .await
                .map_err(|error| {
                    anyhow::anyhow!("controller runtime fixture dispatch failed: {error}")
                })?
            else {
                return Ok(drained);
            };
            budget.record_dispatch();
            if !drained.contains(&key) {
                drained.push(key);
            }
        }
    }
}

/// Exact endpoint/controller operations assembled over focused controller ports.
///
/// This fixture never exposes a datastore, router, Pod repository, or GC
/// implementation. The root may compose the ports, while endpoint behavior
/// remains owned here with the controller algorithms.
#[derive(Clone)]
pub struct EndpointReconcileFixture {
    endpoint_store: Arc<dyn crate::endpoints::EndpointReconcileStore>,
    pod_query: Arc<dyn PodQuery>,
    gc_store: Arc<dyn crate::gc::GcResourceStore>,
    non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    coordination: Arc<crate::ControllerCoordination>,
    identity: Arc<dyn crate::ControllerIdentityGenerator>,
}

/// Test-only, exact NodePort-range exhaustion control for Service API cases.
///
/// It exposes no allocator access and cannot reserve an arbitrary port.
#[derive(Clone)]
pub struct NodePortExhaustionFixture {
    allocator: Arc<crate::service::NodePortAllocator>,
}

impl NodePortExhaustionFixture {
    pub fn new(allocator: Arc<crate::service::NodePortAllocator>) -> Self {
        Self { allocator }
    }

    pub fn exhaust(&self) -> anyhow::Result<()> {
        for expected in 30000..=32767 {
            let allocated = self.allocator.allocate().map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                allocated == expected,
                "unexpected NodePort allocation {allocated}"
            );
        }
        Ok(())
    }
}

impl EndpointReconcileFixture {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_store: Arc<dyn crate::endpoints::EndpointReconcileStore>,
        pod_query: Arc<dyn PodQuery>,
        gc_store: Arc<dyn crate::gc::GcResourceStore>,
        non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
        coordination: Arc<crate::ControllerCoordination>,
        identity: Arc<dyn crate::ControllerIdentityGenerator>,
    ) -> Self {
        Self {
            endpoint_store,
            pod_query,
            gc_store,
            non_pod_finalization,
            coordination,
            identity,
        }
    }

    pub async fn reconcile_endpointslice(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        crate::endpoints::reconcile_endpointslice(
            self.endpoint_store.as_ref(),
            self.pod_query.as_ref(),
            service_name,
            service_uid,
            namespace,
            selector,
            ports,
        )
        .await
    }

    pub async fn reconcile_endpoints(
        &self,
        service_name: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
        publish_not_ready: bool,
    ) -> anyhow::Result<()> {
        crate::endpoints::reconcile_endpoints(
            self.endpoint_store.as_ref(),
            self.pod_query.as_ref(),
            service_name,
            namespace,
            selector,
            ports,
            publish_not_ready,
        )
        .await
    }

    pub async fn reconcile_service_endpoint_batch(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
        publish_not_ready: bool,
    ) -> anyhow::Result<()> {
        crate::endpoints::reconcile_service_endpoints_batch(
            self.endpoint_store.as_ref(),
            self.pod_query.as_ref(),
            crate::endpoints::ServiceEndpointBatchReconcileRequest {
                service_name,
                service_uid,
                namespace,
                selector,
                service_ports: ports,
                publish_not_ready,
            },
        )
        .await
    }

    /// Mirrors an Endpoints snapshot at the caller-supplied controller time.
    ///
    /// Tests must provide this value explicitly so the fixture has no ambient
    /// clock or service-locator dependency.
    pub async fn mirror_endpoints_at(
        &self,
        endpoints: &serde_json::Value,
        mirrored_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        crate::endpoints::mirror_endpoints_to_endpointslice_at(
            self.endpoint_store.as_ref(),
            endpoints,
            mirrored_at,
            self.identity.as_ref(),
        )
        .await
    }

    pub async fn cascade_delete_service(
        &self,
        owner_uid: &str,
        owner_name: &str,
        owner_namespace: &str,
    ) -> anyhow::Result<()> {
        let pod_delete = FailClosedGcPodDeleteSink;
        crate::gc::cascade_delete_with_uid(
            self.gc_store.as_ref(),
            owner_uid,
            "v1",
            owner_name,
            "Service",
            Some(owner_namespace.to_owned()),
            &pod_delete,
            self.non_pod_finalization.as_ref(),
            self.coordination.as_ref(),
        )
        .await
    }
}

/// Recording reconcile sink used by controller/reconcile scenarios.
///
/// It enforces the same one-way separation the production dispatcher
/// enforces: a `Service` reconcile key routed through the non-Service
/// [`ControllerReconcileSink`] edge is rejected rather than silently
/// recorded, so tests can prove Endpoints/EndpointSlice side effects never
/// feed back into Service reconciliation.
#[derive(Default)]
pub struct RecordingReconcileSink {
    keys: Mutex<Vec<ReconcileKey>>,
}

impl RecordingReconcileSink {
    fn record(&self, keys: impl IntoIterator<Item = ReconcileKey>) {
        let mut recorded = self
            .keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in keys {
            if !recorded.contains(&key) {
                recorded.push(key);
            }
        }
    }

    pub async fn enqueue_key(&self, key: ReconcileKey) {
        self.record([key]);
    }

    pub async fn pending_keys(&self) -> Vec<ReconcileKey> {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ControllerReconcileSink for RecordingReconcileSink {
    fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            if keys
                .iter()
                .any(|key| key.api_version() == "v1" && key.kind() == "Service")
            {
                return Err(ReconcileSinkError::unsupported_key(
                    "Service reconcile keys must use ServiceReconcileSink",
                ));
            }
            self.record(keys);
            Ok(())
        })
    }
}

impl ServiceReconcileSink for RecordingReconcileSink {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.record(
                keys.into_iter()
                    .map(ServiceReconcileKey::into_reconcile_key),
            );
            Ok(())
        })
    }
}

/// No-op Pod mutation reconcile sink for scenarios that must not observe any
/// Pod-to-Service side effect.
#[derive(Default)]
pub struct NoopPodMutationReconcile;

impl PodMutationReconcileSink for NoopPodMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: PodMutationReconcileRequest,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Pod mutation reconcile sink that counts every emitted effect.
#[derive(Default)]
pub struct CountingPodMutationReconcile {
    effects: AtomicUsize,
}

impl CountingPodMutationReconcile {
    pub fn effects(&self) -> usize {
        self.effects.load(Ordering::SeqCst)
    }
}

impl PodMutationReconcileSink for CountingPodMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: PodMutationReconcileRequest,
    ) -> ReconcileSinkFuture<'_> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// Side-effect hook that records, at the moment it fires, whether the
/// mutated Pod's row still exists and whether its original controller owner
/// reference is still `rs-x`. Scenarios use this to prove persistence lands
/// before reconcile side effects for both update and actor-owned delete
/// paths.
///
/// The lookup is a focused [`PodQuery`] read rather than a generic
/// datastore/resource handle: this hook only ever needs to answer "does the
/// mutated Pod still exist and who owns it," which is exactly the Pod-shaped
/// capability `PodQuery` already exposes.
pub struct RecordingPodDeleteHook {
    query: Arc<dyn PodQuery>,
    observed: Arc<tokio::sync::Mutex<Option<(bool, bool)>>>,
}

impl RecordingPodDeleteHook {
    pub fn new(
        query: Arc<dyn PodQuery>,
        observed: Arc<tokio::sync::Mutex<Option<(bool, bool)>>>,
    ) -> Self {
        Self { query, observed }
    }
}

#[async_trait::async_trait]
impl crate::side_effects::SideEffect for RecordingPodDeleteHook {
    fn name(&self) -> &'static str {
        "recording_pod_delete_hook"
    }

    async fn apply(&self, resource: &serde_json::Value) -> anyhow::Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default");
        let name = resource
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let exists = self
            .query
            .get_pod(PodGetRequest::try_by_name(namespace, name)?)
            .await
            .map_err(anyhow::Error::new)?
            .is_some();
        let original_owner = resource
            .pointer("/metadata/ownerReferences/0/name")
            .and_then(serde_json::Value::as_str)
            == Some("rs-x");
        *self.observed.lock().await = Some((exists, original_owner));
        Ok(())
    }
}

/// Storage-neutral recorder for the actor-owned GC Pod-delete boundary.
///
/// It deliberately records the complete [`klights_types::PodIdentity`]. In
/// particular, a same-name replacement must remain distinguishable by UID;
/// this fixture grants no resource-store or hard-delete capability.
#[derive(Default)]
pub struct RecordingGcPodDeleteSink {
    requests: Mutex<Vec<klights_types::PodIdentity>>,
}

impl RecordingGcPodDeleteSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn requests(&self) -> Vec<klights_types::PodIdentity> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn recorded_requests(&self) -> Vec<(String, String, String)> {
        self.requests()
            .into_iter()
            .map(|identity| (identity.namespace, identity.name, identity.uid))
            .collect()
    }

    pub fn has_request(&self, namespace: &str, name: &str, uid: &str) -> bool {
        self.requests().iter().any(|identity| {
            identity.namespace == namespace && identity.name == name && identity.uid == uid
        })
    }
}

impl klights_reconcile_api::GcPodDeleteSink for RecordingGcPodDeleteSink {
    fn request_gc_pod_delete(
        &self,
        request: klights_reconcile_api::GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        Box::pin(async move {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.into_identity());
            Ok(())
        })
    }
}

/// Rejects a Pod-delete request from a fixture that is intentionally exercising
/// only a non-Pod orphan/cascade path.
#[derive(Default)]
pub struct FailClosedGcPodDeleteSink;

impl klights_reconcile_api::GcPodDeleteSink for FailClosedGcPodDeleteSink {
    fn request_gc_pod_delete(
        &self,
        _request: klights_reconcile_api::GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        Box::pin(async {
            Err(klights_reconcile_api::GcPodDeleteError::unavailable(
                "non-Pod GC fixture must not request actor-owned Pod deletion",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ControllerRuntimeFixture, DeterministicControllerIdentity, DrainBudget,
        EndpointReconcileFixture, ExecutionLease, FailClosedGcPodDeleteSink, MAX_DRAINED_KEYS,
        NodePortExhaustionFixture, RecordingGcPodDeleteSink,
    };
    use crate::ControllerIdentityGenerator;
    use klights_reconcile_api::{GcPodDeleteRequest, GcPodDeleteSink};
    use klights_types::PodIdentity;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn endpoint_fixture_is_a_named_controller_owner_without_an_inner_escape() {
        fn accepts_fixture(_: Option<EndpointReconcileFixture>) {}
        accepts_fixture(None);
    }

    #[test]
    fn nodeport_exhaustion_fixture_consumes_only_the_exact_service_range() {
        let allocator = Arc::new(crate::service::NodePortAllocator::new());
        allocator.set_ready();
        let fixture = NodePortExhaustionFixture::new(allocator.clone());

        fixture.exhaust().expect("exhaust the NodePort range");
        assert!(allocator.allocate().is_err());
    }

    #[tokio::test]
    async fn gc_pod_sink_preserves_foreground_cascade_uid_for_same_name_replacement() {
        let sink = RecordingGcPodDeleteSink::default();
        for uid in ["old-uid", "replacement-uid"] {
            sink.request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
                "default", "web", uid,
            )))
            .await
            .expect("actor-owned Pod delete request");
        }

        assert_eq!(
            sink.requests(),
            vec![
                PodIdentity::new("default", "web", "old-uid"),
                PodIdentity::new("default", "web", "replacement-uid"),
            ]
        );
    }

    #[tokio::test]
    async fn gc_non_pod_cascade_fixture_fails_closed_if_it_is_asked_to_delete_a_pod() {
        let error = FailClosedGcPodDeleteSink
            .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
                "default",
                "unexpected-pod",
                "uid",
            )))
            .await
            .expect_err("non-Pod cascade must never hard-delete a Pod");

        assert!(matches!(
            error,
            klights_reconcile_api::GcPodDeleteError::Unavailable { .. }
        ));
    }

    #[test]
    fn deterministic_controller_identity_keeps_generated_names_and_uids_stable() {
        let identity = DeterministicControllerIdentity::default();
        assert_eq!(identity.generate_name("controller-"), "controller-00000");
        assert_eq!(identity.new_uid(), "00001000-0000-4000-8000-000000000000");
        assert_eq!(identity.generate_name("controller-"), "controller-00002");
    }

    #[test]
    fn deterministic_controller_identity_preserves_bit_packed_rfc4122_v4_at_large_values() {
        let identity = DeterministicControllerIdentity::with_start(u64::MAX - 1);
        for uid in [identity.new_uid(), identity.new_uid()] {
            assert_eq!(uid.len(), 36);
            assert_eq!(&uid[8..9], "-");
            assert_eq!(&uid[13..14], "-");
            assert_eq!(&uid[18..19], "-");
            assert_eq!(&uid[23..24], "-");
            assert_eq!(&uid[14..15], "4");
            assert!(matches!(&uid[19..20], "8" | "9" | "a" | "b"));
            assert!(uid.chars().enumerate().all(|(index, character)| {
                matches!(index, 8 | 13 | 18 | 23) || character.is_ascii_hexdigit()
            }));
        }
    }

    #[test]
    fn controller_runtime_fixture_is_a_canonical_owner_type() {
        fn accepts_fixture(_: &ControllerRuntimeFixture) {}
        let _ = accepts_fixture;
    }

    #[test]
    fn deterministic_controller_identity_instances_and_counter_wrap_remain_hermetic() {
        let first = DeterministicControllerIdentity::default();
        let second = DeterministicControllerIdentity::default();
        assert_eq!(first.generate_name("controller-"), "controller-00000");
        assert_eq!(first.generate_name("controller-"), "controller-00001");
        assert_eq!(second.generate_name("controller-"), "controller-00000");

        let wrapped = DeterministicControllerIdentity::with_start(u64::MAX);
        assert_eq!(wrapped.generate_name("controller-"), "controller-4sgsf");
        assert_eq!(wrapped.generate_name("controller-"), "controller-00000");
    }

    #[test]
    fn execution_lease_rejects_contention_and_releases_when_dropped() {
        let active = Arc::new(AtomicBool::new(false));
        let held = ExecutionLease::acquire(active.clone()).expect("first execution lease");
        assert!(ExecutionLease::acquire(active.clone()).is_err());
        drop(held);
        assert!(ExecutionLease::acquire(active).is_ok());
    }

    #[test]
    fn execution_lease_is_released_when_supervised_admission_rejects_before_start() {
        let active = Arc::new(AtomicBool::new(false));
        let lease = ExecutionLease::acquire(active.clone()).expect("execution lease");
        let never_started = async move {
            let _lease = lease;
            std::future::pending::<()>().await;
        };
        drop(never_started);
        assert!(
            ExecutionLease::acquire(active).is_ok(),
            "a rejected spawn drops its never-polled future and must not poison later admission"
        );
    }

    #[test]
    fn drain_budget_allows_exactly_1024_dispatches_and_preserves_the_1025th() {
        let mut budget = DrainBudget::default();
        let mut pending = MAX_DRAINED_KEYS + 1;
        let mut dispatched = 0;
        while budget.may_dispatch() && pending > 0 {
            pending -= 1;
            budget.record_dispatch();
            dispatched += 1;
        }
        assert_eq!(dispatched, MAX_DRAINED_KEYS);
        assert_eq!(pending, 1, "the bound rejects before taking the 1025th key");
        assert!(!budget.may_dispatch());
    }
}
