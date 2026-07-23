#![cfg(test)]

use anyhow::Result;
use tokio::sync::mpsc;

use crate::log_apply::LogApplyCommit;

type StreamItem =
    std::result::Result<crate::replication::grpc::generated::ReplicationEntry, tonic::Status>;

pub struct TestProtoChannelSink {
    tx: Option<mpsc::Sender<StreamItem>>,
}

impl TestProtoChannelSink {
    pub fn new(tx: mpsc::Sender<StreamItem>) -> Self {
        Self { tx: Some(tx) }
    }

    pub fn finish(&mut self) {
        self.tx.take();
    }
}

impl crate::datastore::snapshot_export::SnapshotCommitSink for TestProtoChannelSink {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            return Ok(());
        };
        let item = crate::replication::grpc::log_apply_commit_to_proto(&commit)
            .map_err(|error| tonic::Status::internal(error.to_string()));
        tx.send(item)
            .await
            .map_err(|error| anyhow::anyhow!("snapshot test receiver dropped: {error}"))
    }

    fn finish(&mut self) -> Result<()> {
        self.finish();
        Ok(())
    }
}
