use super::category::TaskCategory;
use serde::Serialize;

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
    pub timed_out: bool,
    pub remaining_active: usize,
}

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
