mod allocator;
pub(crate) mod cli;
mod cluster_engine;
pub(crate) mod paths;
pub(crate) mod pidfile;
pub(crate) mod shutdown;
pub(crate) mod version;

#[cfg(test)]
mod cluster_engine_composition_tests;
// Deployment script invariants are covered by the base-repo source guard run
// as part of `./build.sh`.

mod bootstrap;
#[cfg(test)]
extern crate self as klights;
#[cfg(test)]
mod shutdown_test;

pub(crate) use bootstrap::config::KlightsConfig;

#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Binary entry wrapper retained for the package binary target.
pub fn main_entry() {
    bootstrap::entry::main_entry();
}
