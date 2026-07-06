//! memory-improvement.md §10 P1 — streaming snapshot sink.
//!
//! `SnapshotCommitChannelSink` is a [`SnapshotCommitSink`](crate::replication::snapshot::SnapshotCommitSink)
//! that converts each emitted [`LogApplyCommit`](crate::log_apply::LogApplyCommit) into a
//! `ReplicationEntry` proto (via [`log_apply_commit_to_proto`](crate::replication::grpc::log_apply_commit_to_proto))
//! and pushes it straight into a bounded `mpsc` channel feeding the gRPC
//! `SnapshotStream`.
//!
//! This replaces the legacy "materialize the entire snapshot into one
//! `Arc<Vec<LogApplyCommit>>` then clone it per request" path. With a bounded
//! channel, peak resident memory is O(channel capacity) regardless of how many
//! rows the snapshot spans — the producer awaits when the consumer (the
//! outbound gRPC stream) is slow, instead of buffering the whole snapshot.

use anyhow::Result;
use tokio::sync::mpsc;

use crate::log_apply::LogApplyCommit;
use crate::replication::snapshot::SnapshotCommitSink;

/// Error forwarded to the snapshot stream when a commit cannot be converted to
/// its protobuf form. Conversion failures are non-fatal for the channel itself;
/// the streamed item carries the error and the sink stops accepting further
/// commits.
type StreamItem =
    std::result::Result<crate::replication::grpc::generated::ReplicationEntry, tonic::Status>;

/// Snapshot sink that streams each commit to the wire as a protobuf.
///
/// Construct it with the sending half of the bounded channel the gRPC handler
/// returns as its `SnapshotStream`. Call [`SnapshotCommitSink::finish`] (or drop
/// the sink) to close the channel so the receiver terminates after the last
/// commit.
pub struct SnapshotCommitChannelSink {
    tx: Option<mpsc::Sender<StreamItem>>,
}

impl SnapshotCommitChannelSink {
    /// Wrap the sending half of the snapshot response channel.
    ///
    /// The channel MUST be bounded: backpressure against a slow gRPC consumer is
    /// the whole point (an unbounded channel would let the producer race ahead
    /// and re-materialize the snapshot on the heap).
    pub fn new(tx: mpsc::Sender<StreamItem>) -> Self {
        Self { tx: Some(tx) }
    }
}

impl SnapshotCommitSink for SnapshotCommitChannelSink {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            // Already finished: the stream has been torn down. Nothing more to
            // do; treat as success so the emitter can wind down cleanly.
            return Ok(());
        };
        let proto = crate::replication::grpc::log_apply_commit_to_proto(&commit)
            .map_err(|e| anyhow::anyhow!("snapshot commit proto encode failed: {e}"));
        let item = match proto {
            Ok(proto) => Ok(proto),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        };
        // `send` awaits when the channel is full, giving real backpressure.
        tx.send(item)
            .await
            .map_err(|e| anyhow::anyhow!("snapshot stream receiver dropped: {e}"))?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        // Drop the sender so the receiver observes end-of-stream after draining.
        self.tx.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_apply::LogApplyCommit;
    use crate::replication::grpc::log_apply_commit_from_proto;

    #[tokio::test]
    async fn channel_sink_round_trips_commits_and_closes_on_finish() {
        let (tx, mut rx) = mpsc::channel::<StreamItem>(8);
        let mut sink = SnapshotCommitChannelSink::new(tx);

        let commits = vec![
            LogApplyCommit::new(1, Vec::new()),
            LogApplyCommit::new(2, Vec::new()),
            LogApplyCommit::new(3, Vec::new()),
        ];
        for commit in commits {
            sink.push(commit).await.unwrap();
        }
        sink.finish().unwrap();

        let mut rvs = Vec::new();
        while let Some(item) = rx.recv().await {
            let proto = item.expect("encode must succeed");
            let commit = log_apply_commit_from_proto(proto).unwrap();
            rvs.push(commit.resource_version);
        }
        assert_eq!(rvs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn channel_sink_backpressures_when_channel_full() {
        // Capacity 1: with the receiver not draining, the second push must
        // await (pending) instead of buffering unboundedly. We poll it with a
        // yield_now to prove it is suspended, then drain to complete it, all
        // inside a block so the pinned borrow ends before `finish`.
        let (tx, mut rx) = mpsc::channel::<StreamItem>(1);
        let mut sink = SnapshotCommitChannelSink::new(tx);

        sink.push(LogApplyCommit::new(1, Vec::new())).await.unwrap(); // fills the 1 slot
        {
            let mut pushed = std::pin::pin!(sink.push(LogApplyCommit::new(2, Vec::new())));
            tokio::select! {
                biased;
                _ = &mut pushed => panic!("push must backpressure while the channel is full"),
                _ = tokio::task::yield_now() => {}
            }
            // Drain one item; the pending push can now complete.
            let _item = rx.recv().await.unwrap();
            pushed.await.unwrap();
        }
        sink.finish().unwrap();
        let _item = rx.recv().await.unwrap();
        assert!(rx.recv().await.is_none(), "channel must close after finish");
    }
}
