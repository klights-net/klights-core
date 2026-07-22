//! Observable task and shutdown status values.

use super::category::TaskCategory;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ActiveTaskStatus {
    pub id: u64,
    pub category: TaskCategory,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskCategoryStatus {
    pub category: TaskCategory,
    pub limit: usize,
    pub active: usize,
    pub queued: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DbQueryLoggingStatus {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ShutdownReport {
    pub total_managed: usize,
    pub joined: usize,
    pub aborted: usize,
    /// Abort requests that reached a confirmed terminal task outcome.
    pub abort_confirmed: usize,
    pub timed_out: bool,
    pub remaining_active: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskOutcome {
    Completed,
    Panicked,
    CallerAborted,
    ShutdownAborted,
    RuntimeCancelled,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskOutcomeStatus {
    pub id: u64,
    pub category: TaskCategory,
    pub name: String,
    pub outcome: TaskOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAdmissionError {
    ShuttingDown,
    CategoryClosed(TaskCategory),
}

impl fmt::Display for TaskAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => formatter.write_str("task supervisor is shutting down"),
            Self::CategoryClosed(category) => {
                write!(formatter, "task category semaphore is closed: {category:?}")
            }
        }
    }
}

impl std::error::Error for TaskAdmissionError {}

#[derive(Debug)]
pub struct TaskJoinError {
    inner: tokio::task::JoinError,
}

impl TaskJoinError {
    pub(crate) fn new(inner: tokio::task::JoinError) -> Self {
        Self { inner }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    pub fn is_panic(&self) -> bool {
        self.inner.is_panic()
    }

    pub fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        self.inner.into_panic()
    }
}

impl fmt::Display for TaskJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl std::error::Error for TaskJoinError {}

#[derive(Debug, Clone)]
pub(super) struct ActiveTask {
    pub id: u64,
    pub category: TaskCategory,
    pub name: String,
}

impl ActiveTask {
    pub fn to_status(&self) -> ActiveTaskStatus {
        ActiveTaskStatus {
            id: self.id,
            category: self.category,
            name: self.name.clone(),
        }
    }
}
