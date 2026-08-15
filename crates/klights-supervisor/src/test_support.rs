//! Focused constructors for tests that exercise supervised process boundaries.

use std::sync::Arc;

/// Construct a file/process executor with an isolated test supervisor.
pub fn file_process_executor() -> crate::FileProcessExecutor {
    crate::FileProcessExecutor::new(Arc::new(crate::TaskSupervisor::new(
        crate::TaskCategoryConfig::default(),
    )))
}
