pub mod api;

// TRANSITION(Phase 7.3): remove these four root compatibility reexports after
// application consumers import the leaf lifecycle contract directly.
pub use klights_supervisor::{
    SupervisedJoinHandle, TaskCategory, TaskCategoryConfig, TaskSupervisor,
};
