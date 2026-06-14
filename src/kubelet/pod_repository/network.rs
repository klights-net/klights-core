//! `PodNetworkService` — read-side only wrapper around the CRI/CNI-assigned
//! pod IP. Holds `DatastoreHandle` (strictly to call `db.get_pod_network(...)`,
//! a non-Pod-kind table read) and `Arc<TaskSupervisor>` (for the bounded
//! event wait that covers the RunPodSandbox/row-visibility race).
//!
//! Does NOT call `cni_add` / `cni_del`. Teardown stays in
//! `src/networking/cni.rs` because that single call preserves the
//! retry-on-veth-failure invariant.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_manager::get_cached_host_ip;
use crate::kubelet::pod_startup_error::PodStartupErrorKind;
use crate::networking::pod_network_events::{PodNetworkEvents, PodNetworkKey};
use crate::task_supervisor::TaskSupervisor;

use super::types::PodNetworkAssignment;

// Keep a bounded, event-driven wait long enough for delayed CNI assignment
// visibility under scheduler pressure, while avoiding retry-style polling.
const ASSIGNMENT_WAIT: Duration = Duration::from_secs(30);

pub(super) struct PodNetworkService {
    db: DatastoreHandle,
    supervisor: Arc<TaskSupervisor>,
    network_events: PodNetworkEvents,
}

impl PodNetworkService {
    pub(super) fn new(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        network_events: PodNetworkEvents,
    ) -> Self {
        Self {
            db,
            supervisor,
            network_events,
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
    pub(super) async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment> {
        let host_ip = get_cached_host_ip().to_string();
        if host_network {
            return Ok(PodNetworkAssignment {
                pod_ip: host_ip.clone(),
                host_ip,
            });
        }

        let key = PodNetworkKey::new(sandbox_id, namespace, pod_name, pod_uid);
        let notify = self.network_events.subscribe(&key).await;

        if let Some(assignment) = self
            .lookup_assignment(sandbox_id, namespace, pod_name, pod_uid, &host_ip)
            .await?
        {
            self.network_events.remove(&key).await;
            return Ok(assignment);
        }
        let timed_out = self
            .supervisor
            .timeout(
                "pod_network_assignment_wait",
                ASSIGNMENT_WAIT,
                notify.notified(),
            )
            .await?
            .is_err();
        if timed_out {
            self.network_events.remove(&key).await;
            return Err(anyhow::Error::new(PodStartupErrorKind::NetworkAssignmentTimedOut).context(
                format!(
                    "pod network assignment wait timed out for sandbox {sandbox_id} or pod {namespace}/{pod_name} uid {pod_uid}"
                ),
            ));
        }

        let assignment = self
            .lookup_assignment(sandbox_id, namespace, pod_name, pod_uid, &host_ip)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "pod network assignment notification arrived without row for sandbox {sandbox_id} or pod {namespace}/{pod_name} uid {pod_uid}"
                )
            })?;
        self.network_events.remove(&key).await;
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
        if let Some(row) = self.db.get_pod_network(sandbox_id).await? {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr,
                host_ip: host_ip.to_string(),
            }));
        }
        if pod_uid.is_empty() {
            return Ok(None);
        }
        if let Some(row) = self
            .db
            .get_pod_network_for_pod(namespace, pod_name, pod_uid)
            .await?
        {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr,
                host_ip: host_ip.to_string(),
            }));
        }
        Ok(None)
    }
}
