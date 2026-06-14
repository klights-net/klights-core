//! redb open options.
//!
//! Blocking filesystem and database-open work lives in `open_boundary.rs`,
//! where constructors run it through `TaskSupervisor`.

use std::path::PathBuf;

/// Options for opening a redb database.
#[derive(Debug, Clone)]
pub struct RedbOpenOpts {
    /// Path to the `state.redb` file.
    pub path: PathBuf,
    /// Cache size in bytes.  Default 40 MB to match the SQLite plaintext
    /// `cache_size` profile.
    pub cache_size: usize,
}
