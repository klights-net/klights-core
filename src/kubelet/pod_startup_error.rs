#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartupRetryPolicy {
    Retry,
    FailPod,
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartupErrorKind {
    #[cfg(test)]
    ImagePull,
    #[cfg(test)]
    InitContainerFailed {
        exit_code: i32,
    },
    #[cfg(test)]
    MissingProjectedSource,
    #[cfg(test)]
    CniUnavailable,
    NetworkAssignmentTimedOut,
    #[cfg(test)]
    CriUnavailable,
    PodDisappeared,
    #[cfg(test)]
    InvalidPodSpec,
    /// Per-container configuration error (invalid subPath, runAsNonRoot
    /// mismatch, etc.) that create_run has already surfaced into the pod's
    /// `status.containerStatuses[].state.waiting` with the appropriate
    /// CreateContainerConfigError reason. Treat as Skip so the upstream
    /// `mark_pod_failed` path does NOT overwrite the per-container status
    /// or flip the pod phase to Failed — upstream K8s leaves such pods
    /// in Pending so clients (and conformance `WaitForPodContainerToFail`)
    /// can observe the CreateContainerConfigError reason.
    #[cfg(test)]
    ContainerConfigError,
}

impl std::fmt::Display for PodStartupErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(test)]
            Self::ImagePull => write!(f, "image pull failed"),
            #[cfg(test)]
            Self::InitContainerFailed { exit_code } => {
                write!(f, "init container failed with exit code {exit_code}")
            }
            #[cfg(test)]
            Self::MissingProjectedSource => write!(f, "projected volume source is missing"),
            #[cfg(test)]
            Self::CniUnavailable => write!(f, "cni plugin is unavailable"),
            Self::NetworkAssignmentTimedOut => write!(f, "pod network assignment timed out"),
            #[cfg(test)]
            Self::CriUnavailable => write!(f, "cri runtime is unavailable"),
            Self::PodDisappeared => write!(f, "pod disappeared during startup"),
            #[cfg(test)]
            Self::InvalidPodSpec => write!(f, "invalid pod spec"),
            #[cfg(test)]
            Self::ContainerConfigError => {
                write!(f, "container configuration error (pod stays Pending)")
            }
        }
    }
}

impl std::error::Error for PodStartupErrorKind {}

#[cfg(test)]
impl PodStartupErrorKind {
    pub fn retry_policy(&self, restart_policy: &str) -> PodStartupRetryPolicy {
        match self {
            Self::PodDisappeared => PodStartupRetryPolicy::Skip,
            Self::InitContainerFailed { .. } if restart_policy == "Never" => {
                PodStartupRetryPolicy::FailPod
            }
            Self::InvalidPodSpec => PodStartupRetryPolicy::FailPod,
            Self::InitContainerFailed { .. } => PodStartupRetryPolicy::Skip,
            Self::ContainerConfigError => PodStartupRetryPolicy::Skip,
            Self::ImagePull
            | Self::MissingProjectedSource
            | Self::CniUnavailable
            | Self::NetworkAssignmentTimedOut
            | Self::CriUnavailable => PodStartupRetryPolicy::Retry,
        }
    }
}
