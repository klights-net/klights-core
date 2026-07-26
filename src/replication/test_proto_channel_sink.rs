#![cfg(test)]

use anyhow::Result;
use tokio::sync::mpsc;

use crate::log_apply::SnapshotRestoreOperation;

type StreamItem = std::result::Result<SnapshotRestoreOperation, tonic::Status>;

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
    async fn push(&mut self, operation: SnapshotRestoreOperation) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            return Ok(());
        };
        tx.send(Ok(operation))
            .await
            .map_err(|error| anyhow::anyhow!("snapshot test receiver dropped: {error}"))
    }

    fn finish(&mut self) -> Result<()> {
        self.finish();
        Ok(())
    }
}
