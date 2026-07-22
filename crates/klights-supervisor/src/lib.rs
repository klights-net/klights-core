//! Supervised asynchronous task primitives for klights.

mod category;
mod supervisor;
mod task;

pub use category::{TaskCategory, TaskCategoryConfig};
pub use supervisor::{SupervisedJoinHandle, TaskSupervisor};
pub use task::{
    ActiveTaskStatus, DbQueryLoggingStatus, ShutdownReport, TaskAdmissionError, TaskCategoryStatus,
    TaskJoinError, TaskOutcome, TaskOutcomeStatus,
};

#[cfg(test)]
mod tests;
