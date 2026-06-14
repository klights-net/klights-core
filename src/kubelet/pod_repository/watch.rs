//! `PodWatchService` — exposes a watch subscription over the underlying
//! `PodStore`'s broadcast channel. Implementation lands in Task 3.

use std::sync::Arc;

use super::store::PodStore;

pub(super) struct PodWatchService {
    _store: Arc<PodStore>,
}

impl PodWatchService {
    pub(super) fn new(store: Arc<PodStore>) -> Self {
        Self { _store: store }
    }
}
