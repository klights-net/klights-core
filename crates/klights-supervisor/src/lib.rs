//! Supervised asynchronous task primitives for klights.

mod category;
mod clock;
pub mod reconnect_backoff;
pub mod runtime_fs;
mod sqlite;
pub mod sqlite_open;
mod supervisor;
mod task;

pub use category::{TaskCategory, TaskCategoryConfig};
pub use clock::{SystemWallClock, WallClock};
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
