#[cfg(test)]
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum RestartReason {
    StartupProbe,
    LivenessProbe,
}

#[derive(Debug, Clone)]
pub enum LifecycleCommand {
    ReadinessChanged {
        pod_uid: String,
        namespace: String,
        pod_name: String,
        container_name: String,
        ready: bool,
    },
    RestartRequested {
        pod_uid: String,
        namespace: String,
        pod_name: String,
        container_name: String,
        reason: RestartReason,
    },
    StartupPassed {
        pod_uid: String,
        namespace: String,
        pod_name: String,
        container_name: String,
    },
}

impl LifecycleCommand {
    pub fn target(&self) -> (&str, &str, &str) {
        match self {
            Self::ReadinessChanged {
                pod_uid,
                namespace,
                pod_name,
                ..
            }
            | Self::RestartRequested {
                pod_uid,
                namespace,
                pod_name,
                ..
            }
            | Self::StartupPassed {
                pod_uid,
                namespace,
                pod_name,
                ..
            } => (namespace, pod_name, pod_uid),
        }
    }
}

#[cfg(test)]
pub struct RestartBackoff;

#[cfg(test)]
impl RestartBackoff {
    /// K8s-style restart delay: 10s, 20s, 40s, 80s, 160s, 300s.
    pub fn delay_after(failures: u32) -> Duration {
        if failures == 0 {
            return Duration::from_secs(0);
        }

        let exponent = (failures - 1).min(5);
        let delay = 10_u64.saturating_mul(1_u64 << exponent);
        Duration::from_secs(delay.min(300))
    }
}

#[cfg(test)]
mod tests {
    use super::RestartBackoff;
    use std::time::Duration;

    #[test]
    fn restart_delay_follows_k8s_backoff_sequence() {
        assert_eq!(RestartBackoff::delay_after(0), Duration::from_secs(0));
        assert_eq!(RestartBackoff::delay_after(1), Duration::from_secs(10));
        assert_eq!(RestartBackoff::delay_after(2), Duration::from_secs(20));
        assert_eq!(RestartBackoff::delay_after(3), Duration::from_secs(40));
        assert_eq!(RestartBackoff::delay_after(4), Duration::from_secs(80));
        assert_eq!(RestartBackoff::delay_after(5), Duration::from_secs(160));
        assert_eq!(RestartBackoff::delay_after(6), Duration::from_secs(300));
        assert_eq!(RestartBackoff::delay_after(7), Duration::from_secs(300));
    }
}
