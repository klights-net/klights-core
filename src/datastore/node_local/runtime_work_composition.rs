//! Root composition for the passive node runtime-work implementation.

use klights_node_store::{
    OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup, PodRuntimeRecord, PodRuntimeStore,
    PodSlotAdmissionEventSource, PodSlotAdmissionRequest, PodSlotAdmissionResult,
    PodSlotAdmissionStore, PodSlotClearResult, PodSlotEventSubscription, PodSlotMutationResult,
    PodWorkqueueClaimRequest, PodWorkqueueEnqueue, PodWorkqueueLease, PodWorkqueueLeaseToken,
    PodWorkqueueMutationOutcome, PodWorkqueueRequeue, PodWorkqueueStore, ProbeKey, ProbeResult,
    ProbeState, ProbeStateStore, RuntimeNamespace, RuntimePodUid, RuntimeWorkFuture,
};

use crate::bootstrap::node_store::NodeLocalStores;

impl PodRuntimeStore for NodeLocalStores {
    fn admit_pod_runtime(&self, admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().admit_pod_runtime(admission)
    }

    fn record_owned_sandbox(&self, sandbox: OwnedPodSandbox) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().record_owned_sandbox(sandbox)
    }

    fn record_cgroup(&self, cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().record_cgroup(cgroup)
    }

    fn delete_pod_runtime_for_uid(&self, pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().delete_pod_runtime_for_uid(pod_uid)
    }

    fn get_pod_runtime(
        &self,
        pod_uid: RuntimePodUid,
    ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>> {
        self.runtime_work_ref().get_pod_runtime(pod_uid)
    }

    fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        self.runtime_work_ref().list_pod_runtime()
    }

    fn list_pod_runtime_by_namespace(
        &self,
        namespace: RuntimeNamespace,
    ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        self.runtime_work_ref()
            .list_pod_runtime_by_namespace(namespace)
    }
}

impl ProbeStateStore for NodeLocalStores {
    fn record_probe_result(&self, result: ProbeResult) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().record_probe_result(result)
    }

    fn get_probe_state(&self, key: ProbeKey) -> RuntimeWorkFuture<'_, Option<ProbeState>> {
        self.runtime_work_ref().get_probe_state(key)
    }
}

impl PodWorkqueueStore for NodeLocalStores {
    fn enqueue_work(&self, entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().enqueue_work(entry)
    }

    fn ensure_work_if_absent(&self, entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, bool> {
        self.runtime_work_ref().ensure_work_if_absent(entry)
    }

    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>> {
        self.runtime_work_ref().peek_next_due_ms()
    }

    fn claim_due_work_with_lease(
        &self,
        request: PodWorkqueueClaimRequest,
    ) -> RuntimeWorkFuture<'_, Option<PodWorkqueueLease>> {
        self.runtime_work_ref().claim_due_work_with_lease(request)
    }

    fn acknowledge_work(
        &self,
        token: PodWorkqueueLeaseToken,
    ) -> RuntimeWorkFuture<'_, PodWorkqueueMutationOutcome> {
        self.runtime_work_ref().acknowledge_work(token)
    }

    fn requeue_work(
        &self,
        request: PodWorkqueueRequeue,
    ) -> RuntimeWorkFuture<'_, PodWorkqueueMutationOutcome> {
        self.runtime_work_ref().requeue_work(request)
    }
}

impl PodSlotAdmissionStore for NodeLocalStores {
    fn try_admit(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotAdmissionResult> {
        self.runtime_work_ref().try_admit(request)
    }

    fn mark_terminating(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotMutationResult> {
        self.runtime_work_ref().mark_terminating(request)
    }

    fn clear_if_uid(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotClearResult> {
        self.runtime_work_ref().clear_if_uid(request)
    }
}

impl PodSlotAdmissionEventSource for NodeLocalStores {
    fn subscribe(&self) -> Box<dyn PodSlotEventSubscription> {
        self.runtime_work_ref().subscribe()
    }
}
