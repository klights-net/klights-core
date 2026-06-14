use serde::Serialize;
use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Ord, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum TaskCategory {
    Background,
    File,
    Db,
    Timer,
    /// Networking-owned long-running work (rtnetlink connection drivers,
    /// service-routing coalescer worker, future rootless sidecars).
    /// Distinct from `Background` so operators can tune networking
    /// concurrency independently and the spawn guard can enforce that
    /// `src/networking/` only spawns through this category.
    Network,
    /// Bounded slot for deferred Pod-delete / cascade retry work. Other
    /// retry kinds (namespace termination etc.) run on Background; this
    /// category is dedicated to pod cleanup so its concurrency can be
    /// tuned without affecting unrelated retry loops.
    PodDeleteWorkqueue,
    /// Long-lived per-pod lifecycle actor loops. Unlimited by default because
    /// the registry owns the explicit max-live-actors cap.
    PodLifecycleActor,
    /// Short-lived pod lifecycle mutation work such as start/stop/finalize.
    PodLifecycleWork,
    /// Short-lived probe execution work, separate from startup bursts.
    PodProbe,
    Others,
}

impl TaskCategory {
    pub const fn all() -> [Self; 10] {
        [
            Self::Background,
            Self::File,
            Self::Db,
            Self::Timer,
            Self::Network,
            Self::PodDeleteWorkqueue,
            Self::PodLifecycleActor,
            Self::PodLifecycleWork,
            Self::PodProbe,
            Self::Others,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCategoryConfig {
    pub background: usize,
    pub file: usize,
    pub db: usize,
    pub timer: usize,
    pub network: usize,
    pub pod_delete_workqueue: usize,
    pub pod_lifecycle_actor: usize,
    pub pod_lifecycle_work: usize,
    pub pod_probe: usize,
    pub others: usize,
}

impl Default for TaskCategoryConfig {
    fn default() -> Self {
        Self {
            background: 0,
            file: 3,
            db: 1,
            timer: 0,
            network: 256,
            pod_delete_workqueue: 10,
            pod_lifecycle_actor: 0,
            pod_lifecycle_work: 16,
            pod_probe: 64,
            others: 0,
        }
    }
}

impl TaskCategoryConfig {
    pub const fn limit_for(&self, category: TaskCategory) -> usize {
        match category {
            TaskCategory::Background => self.background,
            TaskCategory::File => self.file,
            TaskCategory::Db => self.db,
            TaskCategory::Timer => self.timer,
            TaskCategory::Network => self.network,
            TaskCategory::PodDeleteWorkqueue => self.pod_delete_workqueue,
            TaskCategory::PodLifecycleActor => self.pod_lifecycle_actor,
            TaskCategory::PodLifecycleWork => self.pod_lifecycle_work,
            TaskCategory::PodProbe => self.pod_probe,
            TaskCategory::Others => self.others,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let mut cfg = Self::default();
        cfg.background = parse_limit("KLIGHTS_TASK_BACKGROUND", cfg.background)?;
        cfg.file = parse_limit("KLIGHTS_TASK_FILE", cfg.file)?;
        cfg.db = parse_limit("KLIGHTS_TASK_DB", cfg.db)?;
        cfg.timer = parse_limit("KLIGHTS_TASK_TIMER", cfg.timer)?;
        cfg.network = parse_limit("KLIGHTS_TASK_NETWORK", cfg.network)?;
        cfg.pod_delete_workqueue = parse_limit(
            "KLIGHTS_TASK_POD_DELETE_WORKQUEUE",
            cfg.pod_delete_workqueue,
        )?;
        cfg.pod_lifecycle_actor =
            parse_limit("KLIGHTS_TASK_POD_LIFECYCLE_ACTOR", cfg.pod_lifecycle_actor)?;
        cfg.pod_lifecycle_work =
            parse_limit("KLIGHTS_TASK_POD_LIFECYCLE_WORK", cfg.pod_lifecycle_work)?;
        cfg.pod_probe = parse_limit("KLIGHTS_TASK_POD_PROBE", cfg.pod_probe)?;
        cfg.others = parse_limit("KLIGHTS_TASK_OTHERS", cfg.others)?;
        Ok(cfg)
    }
}

fn parse_limit(var: &str, default: usize) -> anyhow::Result<usize> {
    let Some(raw) = env::var_os(var) else {
        return Ok(default);
    };
    let raw = raw.to_string_lossy();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    let parsed = trimmed
        .parse::<usize>()
        .map_err(|e| anyhow::anyhow!("invalid {}='{}': {}", var, trimmed, e))?;
    Ok(parsed)
}
