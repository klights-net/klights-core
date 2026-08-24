use super::*;

use klights_node_api::{
    NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession, NodeExecSyncRequest,
    NodeExecSyncResult, NodeExecTarget,
};
use klights_replication::authority::WatchLeaderAuthority;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct RecordingNodeExec {
    sync_calls: AtomicUsize,
}

impl NodeExec for RecordingNodeExec {
    fn exec_sync(&self, _request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        self.sync_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(NodeExecSyncResult::success(Vec::new(), Vec::new(), 0)) })
    }

    fn open_exec(&self, _request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async {
            Err(klights_node_api::ExecSetupError::unavailable(
                "unused by test",
            ))
        })
    }
}

fn request() -> NodeExecSyncRequest {
    NodeExecSyncRequest::try_new(
        NodeExecTarget::try_new("worker-1", "default", "sonobuoy", "containerd://test").unwrap(),
        vec!["true".to_string()],
        30,
    )
    .unwrap()
}

#[tokio::test]
async fn node_exec_follows_current_authority_on_every_call() {
    let local = Arc::new(RecordingNodeExec::default());
    let remote = Arc::new(RecordingNodeExec::default());
    let (authority, publisher) = WatchLeaderAuthority::channel(true, None);
    let routed = AuthorityRoutedNodeExec::new(local.clone(), remote.clone(), authority);

    routed.exec_sync(request()).await.unwrap();
    publisher
        .publish(false, Some("https://cp3:7679".to_string()))
        .await;
    routed.exec_sync(request()).await.unwrap();

    assert_eq!(local.sync_calls.load(Ordering::Relaxed), 1);
    assert_eq!(remote.sync_calls.load(Ordering::Relaxed), 1);
}
