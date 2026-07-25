use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;

/// UID-bearing identity key for runtime operations.
/// Every mutating runtime call below the API admission layer must carry
/// one of these; name-only lookup is forbidden.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodRuntimeKey {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

impl PodRuntimeKey {
    pub fn new(namespace: &str, name: &str, uid: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        }
    }

    pub fn volume_dir_id(&self) -> String {
        pod_volume_dir_id(&self.namespace, &self.name, &self.uid)
    }
}

pub fn pod_volume_dir_id(namespace: &str, name: &str, uid: &str) -> String {
    format!("{namespace}_{name}_{uid}")
}

impl From<&PodLifecycleKey> for PodRuntimeKey {
    fn from(key: &PodLifecycleKey) -> Self {
        Self {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
        }
    }
}

/// Outcome of a pod start attempt through the runtime service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartResult {
    /// Pod started successfully. `sandbox_id` is the recorded CRI sandbox ID
    /// if the runtime recorded one; `None` means actor state should already
    /// have it (e.g. from a previous start_pod call).
    Started { sandbox_id: Option<String> },
    /// Pod start was cancelled before completion.
    Cancelled,
    /// Pod start failed with a retryable error (e.g. image pull, CRI
    /// unavailable). The actor may retry after a backoff.
    Failed(String),
    /// Pod start failed with a terminal error (e.g. InvalidPodSpec,
    /// InitContainerFailed with restartPolicy=Never). The actor must
    /// not retry and should transition the pod to Failed phase.
    Terminal(String),
}

/// Outcome of actor-owned pod deletion finalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodDeletionFinalizeResult {
    /// Pod row was deleted or was already gone.
    DeletedOrAlreadyGone,
    /// Stable UID-bound finalization is durable in the node outbox. The actor
    /// waits for the committed `DELETED` watch instead of entering retry.
    Queued,
    /// Finalizers are still pending; deletion was deferred.
    FinalizersPending,
}

/// Outcome of startup finalization. `Unconfirmed` keeps the actor's
/// startup-finalized bit false so the next Running+podIP watch echo can retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodFinalizeStartupResult {
    Confirmed { sandbox_id: String },
    Unconfirmed,
}

/// Typed error raised by runtime cleanup paths (e.g. `PodRuntimeService::stop_pod`)
/// when the local node does not own a Pod's runtime.
///
/// This MUST NOT be classified as a retryable kubelet-lifecycle failure: the
/// local node has no CRI/CNI/volume state for a Pod it does not own, so retrying
/// `StopPod` locally can never succeed and would spin the lifecycle actor
/// forever. Row cleanup is owned by `PodStore::delete_unscheduled_with_uid`
/// (unscheduled Pods, HR#11 exception) or the owning node's lifecycle actor
/// (node-assigned Pods).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodOwnershipError {
    /// Node performing the (refused) cleanup.
    pub local_node: String,
    /// Node that owns the Pod, or `None` when `spec.nodeName` is absent (the
    /// Pod was never scheduled / picked up by any kubelet).
    pub target_node: Option<String>,
}

impl PodOwnershipError {
    /// Build the ownership error from the local node name and the Pod's
    /// `spec.nodeName` value (parsed from the raw Pod JSON).
    pub fn from_pod_node_name(local_node: impl Into<String>, pod: &serde_json::Value) -> Self {
        let target_node = pod
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str())
            .map(|s| s.to_string());
        Self {
            local_node: local_node.into(),
            target_node,
        }
    }
}

impl std::fmt::Display for PodOwnershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.target_node {
            Some(target) => write!(
                f,
                "pod runtime is owned by node {target}, not by local node {}",
                self.local_node
            ),
            None => write!(
                f,
                "pod has no assigned node; local node {} cannot own runtime cleanup",
                self.local_node
            ),
        }
    }
}

impl std::error::Error for PodOwnershipError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_key_preserves_uid_and_volume_directory_identity() {
        let key = PodRuntimeKey::new("default", "web", "uid-1");

        assert_eq!(key.namespace, "default");
        assert_eq!(key.name, "web");
        assert_eq!(key.uid, "uid-1");
        assert_eq!(key.volume_dir_id(), "default_web_uid-1");
        assert_eq!(pod_volume_dir_id("ns", "pod", "uid"), "ns_pod_uid");
    }

    #[test]
    fn runtime_key_from_lifecycle_key_preserves_uid() {
        let lifecycle_key = PodLifecycleKey {
            namespace: "ns".to_string(),
            name: "pod".to_string(),
            uid: "uid-a".to_string(),
        };

        assert_eq!(
            PodRuntimeKey::from(&lifecycle_key),
            PodRuntimeKey::new("ns", "pod", "uid-a")
        );
    }

    #[test]
    fn ownership_error_extracts_assigned_node_and_formats_message() {
        let pod = serde_json::json!({
            "spec": {
                "nodeName": "worker-a"
            }
        });

        let error = PodOwnershipError::from_pod_node_name("worker-b", &pod);
        assert_eq!(error.target_node.as_deref(), Some("worker-a"));
        assert_eq!(
            error.to_string(),
            "pod runtime is owned by node worker-a, not by local node worker-b"
        );
    }

    #[test]
    fn ownership_error_handles_unscheduled_pod() {
        let error = PodOwnershipError::from_pod_node_name("worker-b", &serde_json::json!({}));

        assert_eq!(error.target_node, None);
        assert_eq!(
            error.to_string(),
            "pod has no assigned node; local node worker-b cannot own runtime cleanup"
        );
    }
}
