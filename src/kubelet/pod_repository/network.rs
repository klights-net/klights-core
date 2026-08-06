//! `PodNetworkService` — read-side only wrapper around the CRI/CNI-assigned
//! pod IP. Holds only the focused node cache and assignment waiter plus the
//! app-owned supervisor for the bounded RunPodSandbox/row-visibility wait.
//!
//! Does NOT call `cni_add` / `cni_del`. Teardown stays in
//! `src/networking/cni.rs` because that single call preserves the
//! retry-on-veth-failure invariant.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::kubelet::context::HostIpState;
use klights_kubelet::pod_startup_error::PodStartupErrorKind;
use klights_network_api::{PodNetworkAssignmentKey, PodNetworkAssignmentWaiter};
use klights_node_store::{PodNetworkCache, SandboxKey};
use klights_supervisor::TaskSupervisor;

use super::types::PodNetworkAssignment;

// Keep a bounded, event-driven wait long enough for delayed CNI assignment
// visibility under scheduler pressure, while avoiding retry-style polling.
const ASSIGNMENT_WAIT: Duration = Duration::from_secs(30);

pub(crate) struct PodNetworkService {
    cache: Arc<dyn PodNetworkCache>,
    supervisor: Arc<TaskSupervisor>,
    assignment_waiter: Arc<dyn PodNetworkAssignmentWaiter>,
    host_ip: HostIpState,
}

impl PodNetworkService {
    pub(crate) fn new(
        cache: Arc<dyn PodNetworkCache>,
        supervisor: Arc<TaskSupervisor>,
        assignment_waiter: Arc<dyn PodNetworkAssignmentWaiter>,
        host_ip: HostIpState,
    ) -> Self {
        Self {
            cache,
            supervisor,
            assignment_waiter,
            host_ip,
        }
    }

    /// Read the IP assignment CRI/CNI produced.
    ///
    /// `host_network=true` returns `(host_ip, host_ip)` without touching
    /// the DB — host-network pods share the node's address.
    ///
    /// Otherwise reads the `pod_network` row written by the klights CNI shim
    /// during containerd `RunPodSandbox`. The read subscribes before checking
    /// the DB so a concurrent CNI write cannot publish between a miss and the
    /// wait registration.
    pub(crate) async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment> {
        let host_ip = self.host_ip.current().to_string();
        if host_network {
            return Ok(PodNetworkAssignment {
                pod_ip: host_ip.clone(),
                host_ip,
            });
        }

        let key = PodNetworkAssignmentKey::try_new(sandbox_id, namespace, pod_name, pod_uid)
            .map_err(anyhow::Error::new)?;
        let mut subscription = self
            .assignment_waiter
            .subscribe(key)
            .map_err(anyhow::Error::new)?;

        if let Some(assignment) = self
            .lookup_assignment(sandbox_id, namespace, pod_name, pod_uid, &host_ip)
            .await?
        {
            return Ok(assignment);
        }
        let wait_result = self
            .supervisor
            .timeout(
                "pod_network_assignment_wait",
                ASSIGNMENT_WAIT,
                subscription.wait(),
            )
            .await?;
        match wait_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(anyhow::Error::new(error).context(format!(
                    "pod network assignment bus closed for sandbox {sandbox_id} or pod {namespace}/{pod_name} uid {pod_uid}"
                )));
            }
            Err(_) => {
                return Err(anyhow::Error::new(PodStartupErrorKind::NetworkAssignmentTimedOut)
                    .context(format!(
                        "pod network assignment wait timed out for sandbox {sandbox_id} or pod {namespace}/{pod_name} uid {pod_uid}"
                    )));
            }
        }

        let assignment = self
            .lookup_assignment(sandbox_id, namespace, pod_name, pod_uid, &host_ip)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "pod network assignment notification arrived without row for sandbox {sandbox_id} or pod {namespace}/{pod_name} uid {pod_uid}"
                )
            })?;
        Ok(assignment)
    }

    async fn lookup_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_ip: &str,
    ) -> Result<Option<PodNetworkAssignment>> {
        let pod = klights_types::PodIdentity::new(namespace, pod_name, pod_uid);
        if let Some(row) = self
            .cache
            .get_network_for_assignment(SandboxKey::try_new(sandbox_id)?, pod.clone())
            .await?
        {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr().to_string(),
                host_ip: host_ip.to_string(),
            }));
        }
        if let Some(row) = self.cache.get_network_for_pod(pod).await? {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr().to_string(),
                host_ip: host_ip.to_string(),
            }));
        }
        Ok(None)
    }
}
