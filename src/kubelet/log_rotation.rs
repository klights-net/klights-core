use std::path::{Path, PathBuf};

const DEFAULT_MAX_SIZE: u64 = 10 * 1024 * 1024;
const DEFAULT_MAX_FILES: usize = 5;

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

pub fn get_max_log_size() -> u64 {
    std::env::var("KLIGHTS_LOG_MAX_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_SIZE)
}

pub fn get_max_log_files() -> usize {
    std::env::var("KLIGHTS_LOG_MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_FILES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
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
    fn test_get_max_log_size_uses_default() {
        let _guard = env_lock();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_LOG_MAX_SIZE") };
        assert_eq!(get_max_log_size(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_get_max_log_size_uses_env_var() {
        let _guard = env_lock();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_LOG_MAX_SIZE", "5242880") };
        assert_eq!(get_max_log_size(), 5242880);
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_LOG_MAX_SIZE") };
    }

    #[test]
    fn test_get_max_log_files_uses_default() {
        let _guard = env_lock();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_LOG_MAX_FILES") };
        assert_eq!(get_max_log_files(), 5);
    }

    #[test]
    fn test_get_max_log_files_uses_env_var() {
        let _guard = env_lock();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_LOG_MAX_FILES", "10") };
        assert_eq!(get_max_log_files(), 10);
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_LOG_MAX_FILES") };
    }
}
