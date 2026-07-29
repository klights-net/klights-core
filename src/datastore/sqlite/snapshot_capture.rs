//! Root test hook for the passive SQLite recovery capture.

#[cfg(test)]
pub(crate) use klights_cluster_datastore::sqlite::recovery::install_snapshot_capture_page_pause;
