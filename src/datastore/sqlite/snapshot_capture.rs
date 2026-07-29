//! Root compatibility exports for the passive SQLite recovery capture.

pub(super) use super::recovery::capture::SqliteSnapshotFactory;
#[cfg(test)]
pub(crate) use super::recovery::capture::install_snapshot_capture_page_pause;
