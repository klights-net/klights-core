//! Supervised asynchronous task primitives for klights.

mod category;
mod sqlite;
pub mod sqlite_open;
mod supervisor;
mod task;

pub use category::{TaskCategory, TaskCategoryConfig};
pub use sqlite::{DbError, DbExecutor};
pub use sqlite_open::{KeySource, OpenOpts, OpenPath, PragmaProfile, SqliteOpenError};
pub use supervisor::{
    CryptoExecutor, FileProcessExecutor, ProcessError, ProcessShutdownPolicy, SupervisedChild,
    SupervisedJoinHandle, TaskSupervisor,
};
pub use task::{
    ActiveTaskStatus, DbQueryLoggingStatus, ShutdownReport, TaskAdmissionError, TaskCategoryStatus,
    TaskJoinError, TaskOutcome, TaskOutcomeStatus,
};

#[cfg(test)]
mod tests;
