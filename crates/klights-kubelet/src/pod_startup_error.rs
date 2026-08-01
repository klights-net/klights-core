#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartupRetryPolicy {
    Retry,
    FailPod,
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartupErrorKind {
    #[cfg(any(test, feature = "test-support"))]
    ImagePull,
    #[cfg(any(test, feature = "test-support"))]
    InitContainerFailed {
        exit_code: i32,
    },
    #[cfg(any(test, feature = "test-support"))]
    MissingProjectedSource,
    #[cfg(any(test, feature = "test-support"))]
    CniUnavailable,
    NetworkAssignmentTimedOut,
    #[cfg(any(test, feature = "test-support"))]
    CriUnavailable,
    PodDisappeared,
    #[cfg(any(test, feature = "test-support"))]
    InvalidPodSpec,
    /// Per-container configuration error (invalid subPath, runAsNonRoot
    /// mismatch, etc.) that create_run has already surfaced into the pod's
    /// `status.containerStatuses[].state.waiting` with the appropriate
    /// CreateContainerConfigError reason. Treat as Skip so the upstream
    /// `mark_pod_failed` path does NOT overwrite the per-container status
    /// or flip the pod phase to Failed — upstream K8s leaves such pods
    /// in Pending so clients (and conformance `WaitForPodContainerToFail`)
    /// can observe the CreateContainerConfigError reason.
    #[cfg(any(test, feature = "test-support"))]
    ContainerConfigError,
}

impl std::fmt::Display for PodStartupErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(test, feature = "test-support"))]
            Self::ImagePull => write!(f, "image pull failed"),
            #[cfg(any(test, feature = "test-support"))]
            Self::InitContainerFailed { exit_code } => {
                write!(f, "init container failed with exit code {exit_code}")
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::MissingProjectedSource => write!(f, "projected volume source is missing"),
            #[cfg(any(test, feature = "test-support"))]
            Self::CniUnavailable => write!(f, "cni plugin is unavailable"),
            Self::NetworkAssignmentTimedOut => write!(f, "pod network assignment timed out"),
            #[cfg(any(test, feature = "test-support"))]
            Self::CriUnavailable => write!(f, "cri runtime is unavailable"),
            Self::PodDisappeared => write!(f, "pod disappeared during startup"),
            #[cfg(any(test, feature = "test-support"))]
            Self::InvalidPodSpec => write!(f, "invalid pod spec"),
            #[cfg(any(test, feature = "test-support"))]
            Self::ContainerConfigError => {
                write!(f, "container configuration error (pod stays Pending)")
            }
        }
    }
}

impl std::error::Error for PodStartupErrorKind {}

#[cfg(any(test, feature = "test-support"))]
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
