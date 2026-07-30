//! Root composition for the passive node runtime-work implementation.

use klights_node_store::{
    DueTimeMs, OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup, PodRuntimeRecord,
    PodRuntimeStore, PodSlotAdmissionEventSource, PodSlotAdmissionRequest, PodSlotAdmissionResult,
    PodSlotAdmissionStore, PodSlotClearResult, PodSlotEventSubscription, PodSlotMutationResult,
    PodWorkqueueEnqueue, PodWorkqueueEntry, PodWorkqueueStore, ProbeKey, ProbeResult, ProbeState,
    ProbeStateStore, RuntimeNamespace, RuntimePodUid, RuntimeWorkFuture,
};

use super::SqliteNodeLocalDb;

impl PodRuntimeStore for SqliteNodeLocalDb {
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

impl ProbeStateStore for SqliteNodeLocalDb {
    fn record_probe_result(&self, result: ProbeResult) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().record_probe_result(result)
    }

    fn get_probe_state(&self, key: ProbeKey) -> RuntimeWorkFuture<'_, Option<ProbeState>> {
        self.runtime_work_ref().get_probe_state(key)
    }
}

impl PodWorkqueueStore for SqliteNodeLocalDb {
    fn enqueue_work(&self, entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()> {
        self.runtime_work_ref().enqueue_work(entry)
    }

    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>> {
        self.runtime_work_ref().peek_next_due_ms()
    }

    fn claim_due_work(&self, now: DueTimeMs) -> RuntimeWorkFuture<'_, Option<PodWorkqueueEntry>> {
        self.runtime_work_ref().claim_due_work(now)
    }
}

impl PodSlotAdmissionStore for SqliteNodeLocalDb {
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

impl PodSlotAdmissionEventSource for SqliteNodeLocalDb {
    fn subscribe(&self) -> Box<dyn PodSlotEventSubscription> {
        self.runtime_work_ref().subscribe()
    }
}
