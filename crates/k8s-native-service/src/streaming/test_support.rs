//! WebSocket streaming fixtures owned by the native-service test-support API.

use std::sync::Arc;

use klights_node_api::NodeExec;
use klights_supervisor::TaskSupervisor;

/// Feature-only WebSocket executor for remote unary exec tests.
///
/// The fixture intentionally owns only the native streaming adaptation. Its
/// caller supplies the already-composed node execution port and supervisor;
/// replication follower registration remains root composition test coverage.
#[derive(Clone)]
pub struct RemoteExecSyncWebSocketFixture {
    node_exec: Arc<dyn NodeExec>,
    task_supervisor: Arc<TaskSupervisor>,
}

impl RemoteExecSyncWebSocketFixture {
    pub fn new(node_exec: Arc<dyn NodeExec>, task_supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            node_exec,
            task_supervisor,
        }
    }

    pub async fn run<S>(
        self,
        io: S,
        target: crate::streaming::ExecTarget,
        subprotocol: String,
        node_name: String,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        crate::streaming::handle_remote_exec_websocket_sync(
            socket,
            crate::streaming::RemoteExecWebSocketSyncRequest {
                node_exec: self.node_exec,
                target,
                subprotocol,
                node_name,
                task_supervisor: self.task_supervisor,
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt as _;
    use klights_node_api::{
        ExecSetupError, NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession,
        NodeExecSyncRequest, NodeExecSyncResult,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use tokio_tungstenite::tungstenite::{Message, protocol::Role};

    use super::RemoteExecSyncWebSocketFixture;

    struct FixedExec;

    impl NodeExec for FixedExec {
        fn exec_sync(
            &self,
            _request: NodeExecSyncRequest,
        ) -> NodeExecFuture<'_, NodeExecSyncResult> {
            Box::pin(async { Ok(NodeExecSyncResult::success(b"stdout".to_vec(), vec![], 0)) })
        }

        fn open_exec(
            &self,
            _request: NodeExecRequest,
        ) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
            Box::pin(async { Err(ExecSetupError::unavailable("unused stream")) })
        }
    }

    #[test]
    fn remote_exec_sync_fixture_is_owned_by_native_streaming_support() {
        let _ = std::any::type_name::<RemoteExecSyncWebSocketFixture>();
    }

    #[tokio::test]
    async fn remote_exec_sync_fixture_emits_v5_stdout_status_and_close() {
        let fixture = RemoteExecSyncWebSocketFixture::new(
            Arc::new(FixedExec),
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        );
        let (server_io, client_io) = tokio::io::duplex(4096);
        let mut client =
            tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None)
                .await;
        let server = tokio::spawn(fixture.run(
            server_io,
            crate::streaming::ExecTarget {
                namespace: "default".to_string(),
                pod_name: "pod".to_string(),
                container_id: "container".to_string(),
                command: vec!["true".to_string()],
            },
            "v5.channel.k8s.io".to_string(),
            "worker".to_string(),
        ));

        let stdout = client.next().await.unwrap().unwrap();
        let Message::Binary(stdout) = stdout else {
            panic!("expected stdout frame");
        };
        assert_eq!(stdout.as_ref(), b"\x01stdout");
        let status = client.next().await.unwrap().unwrap();
        let Message::Binary(status) = status else {
            panic!("expected Status frame");
        };
        assert_eq!(status.first(), Some(&3));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&status[1..]).unwrap()["status"],
            "Success"
        );
        assert!(matches!(client.next().await, Some(Ok(Message::Close(_)))));
        server.await.unwrap();
    }
}
