use std::path::{Path, PathBuf};

pub const DEFAULT_MAX_SIZE: u64 = 10 * 1024 * 1024;
pub const DEFAULT_MAX_FILES: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRotationPolicy {
    max_size: u64,
    max_files: usize,
}

impl LogRotationPolicy {
    pub fn new(max_size: u64, max_files: usize) -> Option<Self> {
        (max_size > 0 && max_files >= 2).then_some(Self {
            max_size,
            max_files,
        })
    }

    pub fn max_size(self) -> u64 {
        self.max_size
    }

    pub fn max_files(self) -> usize {
        self.max_files
    }
}

impl Default for LogRotationPolicy {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SIZE,
            max_files: DEFAULT_MAX_FILES,
        }
    }
}

/// Pre-computed plan for one container's log rotation. Pure data: the
/// caller (under a supervised filesystem boundary) executes the
/// `remove_oldest`, `renames`, and `current_to_one` operations in order.
#[derive(Debug, PartialEq, Eq)]
pub struct RotationPlan {
    pub remove_oldest: PathBuf,
    pub renames: Vec<(PathBuf, PathBuf)>,
    pub current_to_one: (PathBuf, PathBuf),
}

/// Compute the rotation plan for `log_path`, given its current size and
/// the per-container retention limits. Returns `None` if rotation is not
/// needed (file under threshold) or the plan cannot be derived (no
/// parent / non-UTF-8 stem). No filesystem syscalls are performed.
pub fn build_rotation_plan(
    log_path: &Path,
    current_size: u64,
    max_size: u64,
    max_files: usize,
) -> Option<RotationPlan> {
    if current_size < max_size || max_files < 2 {
        return None;
    }
    let base = log_path.parent()?;
    let stem = log_path.file_stem()?.to_str()?;
    let oldest = base.join(format!("{stem}.{}.log", max_files - 1));
    let mut renames = Vec::with_capacity(max_files.saturating_sub(2));
    for i in (1..max_files - 1).rev() {
        let src = base.join(format!("{stem}.{i}.log"));
        let dst = base.join(format!("{stem}.{}.log", i + 1));
        renames.push((src, dst));
    }
    let current_to_one = (log_path.to_path_buf(), base.join(format!("{stem}.1.log")));
    Some(RotationPlan {
        remove_oldest: oldest,
        renames,
        current_to_one,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn test_build_rotation_plan_under_threshold_returns_none() {
        let plan = build_rotation_plan(&p("/log/c/0.log"), 1024, 10 * 1024 * 1024, 5);
        assert!(plan.is_none());
    }

    #[test]
    fn test_build_rotation_plan_over_threshold_returns_full_plan() {
        let plan = build_rotation_plan(&p("/log/c/0.log"), 11 * 1024 * 1024, 10 * 1024 * 1024, 5)
            .expect("over-threshold plan");
        assert_eq!(plan.remove_oldest, p("/log/c/0.4.log"));
        assert_eq!(
            plan.renames,
            vec![
                (p("/log/c/0.3.log"), p("/log/c/0.4.log")),
                (p("/log/c/0.2.log"), p("/log/c/0.3.log")),
                (p("/log/c/0.1.log"), p("/log/c/0.2.log")),
            ],
        );
        assert_eq!(
            plan.current_to_one,
            (p("/log/c/0.log"), p("/log/c/0.1.log"))
        );
    }

    #[test]
    fn test_build_rotation_plan_max_files_two_only_renames_current() {
        let plan = build_rotation_plan(&p("/c/x.log"), 100, 50, 2).expect("plan");
        assert_eq!(plan.remove_oldest, p("/c/x.1.log"));
        assert!(plan.renames.is_empty());
        assert_eq!(plan.current_to_one, (p("/c/x.log"), p("/c/x.1.log")));
    }

    #[test]
    fn test_build_rotation_plan_max_files_below_two_returns_none() {
        let plan = build_rotation_plan(&p("/c/x.log"), 100, 50, 1);
        assert!(plan.is_none());
    }

    #[test]
    fn log_rotation_policy_has_stable_defaults() {
        let policy = LogRotationPolicy::default();
        assert_eq!(policy.max_size(), 10 * 1024 * 1024);
        assert_eq!(policy.max_files(), 5);
    }

    #[test]
    fn log_rotation_policy_rejects_non_rotating_limits() {
        assert_eq!(LogRotationPolicy::new(0, 5), None);
        assert_eq!(LogRotationPolicy::new(1024, 1), None);
        assert_eq!(
            LogRotationPolicy::new(1024, 3),
            Some(LogRotationPolicy {
                max_size: 1024,
                max_files: 3,
            })
        );
    }
}
