mod allocator;
pub mod cli;
mod cluster_engine;
pub mod paths;
pub mod pidfile;
pub mod shutdown;
pub mod version;

#[cfg(test)]
mod cluster_engine_composition_tests;
// Deployment script invariants are covered by the base-repo source guard run
// as part of `./build.sh`.

mod bootstrap;
#[cfg(test)]
extern crate self as klights;
#[cfg(test)]
mod shutdown_test;

pub use bootstrap::config::KlightsConfig;

#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Binary entry wrapper retained for the package binary target.
pub fn main_entry() {
    bootstrap::entry::main_entry();
}
