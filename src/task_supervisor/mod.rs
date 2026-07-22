pub mod api;

// TRANSITION(Phase 7.3): remove these root compatibility reexports after
// application consumers import the leaf lifecycle contract directly.
pub use klights_supervisor::{
    ProcessError, ProcessShutdownPolicy, SupervisedChild, SupervisedJoinHandle, TaskCategory,
    TaskCategoryConfig, TaskSupervisor,
};
