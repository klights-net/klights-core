//! Focused controller/reconcile/effect fakes shared by cross-crate
//! integration tests.
//!
//! These fakes exist to prove the one-way Pod-to-Service reconcile edge and
//! the persistence-before-effects ordering required for actor-owned Pod
//! deletion; they hold no root datastore/sequencer/control-plane internals.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use klights_pod_api::{PodGetRequest, PodQuery};
use klights_reconcile_api::{
    ControllerReconcileSink, PodMutationReconcileRequest, PodMutationReconcileSink, ReconcileKey,
    ReconcileSinkError, ReconcileSinkFuture, ServiceReconcileKey, ServiceReconcileSink,
};

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
