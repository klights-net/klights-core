//! Watch broadcast mode probe for the SQLite backend.
//!
//! DSB-04 formalizes the watch broadcast path. The system runs in
//! `PostCommitOnly` mode — every CRUD method calls
//! `create_pending_watch_event()` inside the DB transaction and then
//! `broadcast_watch_event()` after the DB call returns (post-commit).
//!
//! The `WatchBroadcastMode` enum in `backend.rs` carries the mode
//! variants. This module provides the runtime probe that selects which
//! mode the SQLite backend uses.

#[cfg(test)]
use crate::datastore::backend::WatchBroadcastMode;

/// Returns the current watch broadcast mode for the SQLite backend.
///
/// Today this always returns `PostCommitOnly` because the SQLite
/// `update_hook` has been fully replaced by post-commit broadcast in
/// the CRUD mutation methods. The deprecated `HookOnly` and
/// `HookWithDedup` variants exist only for backward-compat docs.
///
/// When Phase 3 Raft lands, a future `RaftApply` variant will be
/// selectable via configuration, and the FSM apply hook will call
/// `publish_pending()` instead of the CRUD methods.
#[cfg(test)]
pub fn current_broadcast_mode() -> WatchBroadcastMode {
    WatchBroadcastMode::PostCommitOnly
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_mode_is_post_commit_only() {
        assert_eq!(current_broadcast_mode(), WatchBroadcastMode::PostCommitOnly);
    }
}
