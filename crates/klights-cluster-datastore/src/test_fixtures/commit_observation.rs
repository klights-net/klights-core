#![cfg(any(test, feature = "test-support"))]

use std::any::Any;
use std::sync::{Arc, Mutex};

use klights_cluster_store::{CommitObservationSink, StagedPostCommit};

#[derive(Default)]
pub(crate) struct RecordingCommitSink {
    observations: Mutex<Vec<StagedPostCommit>>,
}

impl RecordingCommitSink {
    pub(crate) fn observations(&self) -> Vec<StagedPostCommit> {
        self.observations.lock().unwrap().clone()
    }
}

impl CommitObservationSink for RecordingCommitSink {
    fn observe(&self, observations: &[StagedPostCommit]) {
        self.observations
            .lock()
            .unwrap()
            .extend_from_slice(observations);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn new_sink() -> Arc<dyn CommitObservationSink> {
    Arc::new(RecordingCommitSink::default())
}

pub(crate) fn recorded_observations(sink: &dyn CommitObservationSink) -> Vec<StagedPostCommit> {
    sink.as_any()
        .downcast_ref::<RecordingCommitSink>()
        .expect("destination persistence recording sink")
        .observations()
}
