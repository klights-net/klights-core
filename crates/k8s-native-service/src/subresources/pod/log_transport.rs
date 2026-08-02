//! Kubernetes Pod-log transport adaptation over the existing node API.

use std::sync::Arc;

use axum::body::Body;
use futures::SinkExt;
use klights_node_api::{NodeLog, NodeLogRequest};

use crate::AppError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeLogOrigin {
    Local,
    Remote,
}

fn log_app_error_message(error: &AppError) -> &str {
    match error {
        AppError::Internal(message)
        | AppError::InternalError(message)
        | AppError::BadGateway(message) => message,
        _ => "pod log transport failed",
    }
}

impl NodeLogOrigin {
    fn setup_error(self, error: impl std::fmt::Display) -> AppError {
        match self {
            Self::Local => AppError::Internal(format!("pod log request failed: {error}")),
            Self::Remote => AppError::BadGateway(format!("remote pod log request failed: {error}")),
        }
    }

    fn terminal_error(self, error: impl std::fmt::Display) -> AppError {
        match self {
            Self::Local => AppError::Internal(error.to_string()),
            Self::Remote => AppError::Internal(format!("remote pod log error: {error}")),
        }
    }

    fn terminal_stream_error(self, error: impl std::fmt::Display) -> std::io::Error {
        let message = match self {
            Self::Local => error.to_string(),
            Self::Remote => format!("remote pod log error: {error}"),
        };
        std::io::Error::other(message)
    }

    fn receive_stream_error(self, error: impl std::fmt::Display) -> std::io::Error {
        let message = match self {
            Self::Local => error.to_string(),
            Self::Remote => format!("remote pod log stream failed: {error}"),
        };
        std::io::Error::other(message)
    }
}

pub(crate) async fn read_log_bytes(
    node_log: &dyn NodeLog,
    request: NodeLogRequest,
    origin: NodeLogOrigin,
) -> Result<Vec<u8>, AppError> {
    let result = node_log
        .read_logs(request)
        .await
        .map_err(|error| origin.setup_error(error))?;
    let (content, terminal_error) = result.into_parts();
    match terminal_error {
        Some(error) => Err(origin.terminal_error(error)),
        None => Ok(content),
    }
}

pub(crate) async fn open_log_body(
    node_log: Arc<dyn NodeLog>,
    request: NodeLogRequest,
    origin: NodeLogOrigin,
) -> Result<Body, AppError> {
    let mut session = node_log
        .open_logs(request)
        .await
        .map_err(|error| origin.setup_error(error))?;
    let stream = async_stream::stream! {
        loop {
            match session.recv_frame().await {
                Ok(Some(event)) => {
                    let terminal = event.is_terminal();
                    let (content, terminal_error, _) = event.into_parts();
                    if let Some(error) = terminal_error {
                        let _ = session.cancel().await;
                        yield Err(origin.terminal_stream_error(error));
                        break;
                    }
                    if !content.is_empty() {
                        yield Ok::<_, std::io::Error>(content);
                    }
                    if terminal {
                        let _ = session.cancel().await;
                        break;
                    }
                }
                Ok(None) => {
                    let _ = session.cancel().await;
                    break;
                }
                Err(error) => {
                    let _ = session.cancel().await;
                    yield Err(origin.receive_stream_error(error));
                    break;
                }
            }
        }
    };
    Ok(Body::from_stream(stream))
}

pub(crate) struct PodLogWebSocketRequest {
    pub node_log: Arc<dyn NodeLog>,
    pub request: NodeLogRequest,
    pub origin: NodeLogOrigin,
    pub follow: bool,
    /// Local `previous=true` WebSockets close without consulting the runtime;
    /// the remote path preserves its existing finite RPC request.
    pub skip_previous_read: bool,
    /// The current remote WebSocket path exposes its terminal error as one
    /// binary line; the current local path logs the error and closes silently.
    pub send_terminal_error: bool,
}

pub(crate) async fn serve_log_websocket<S>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    request: PodLogWebSocketRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let PodLogWebSocketRequest {
        node_log,
        request,
        origin,
        follow,
        skip_previous_read,
        send_terminal_error,
    } = request;

    let result = if skip_previous_read && request.options().previous() == Some("true") {
        Ok(())
    } else if follow {
        match node_log.open_logs(request).await {
            Ok(mut session) => {
                let mut result = Ok(());
                loop {
                    match session.recv_frame().await {
                        Ok(Some(event)) => {
                            let terminal = event.is_terminal();
                            let (content, terminal_error, _) = event.into_parts();
                            if !content.is_empty()
                                && socket.send(Message::Binary(content.into())).await.is_err()
                            {
                                let _ = session.cancel().await;
                                return;
                            }
                            if let Some(error) = terminal_error {
                                result = Err(origin.terminal_error(error));
                                break;
                            }
                            if terminal {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            result = Err(origin.terminal_error(error));
                            break;
                        }
                    }
                }
                let _ = session.cancel().await;
                result
            }
            Err(error) => Err(origin.setup_error(error)),
        }
    } else {
        match read_log_bytes(node_log.as_ref(), request, origin).await {
            Ok(content) => {
                if !content.is_empty()
                    && socket.send(Message::Binary(content.into())).await.is_err()
                {
                    return;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    };

    if let Err(error) = result {
        tracing::warn!(error = ?error, "pod log websocket failed");
        if send_terminal_error {
            let mut body = log_app_error_message(&error).as_bytes().to_vec();
            body.push(b'\n');
            let _ = socket.send(Message::Binary(body.into())).await;
        }
    }

    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::StreamExt;
    use klights_node_api::{
        BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeLogEvent,
        NodeLogFuture, NodeLogOptions, NodeLogResult, NodeLogSetupError, NodeLogTarget,
        NodeLogTerminalError,
    };
    use tokio::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    struct FakeNodeLog {
        finite: StdMutex<Option<Result<NodeLogResult, NodeLogSetupError>>>,
        stream:
            StdMutex<Option<Result<Vec<Result<NodeLogEvent, ByteStreamError>>, NodeLogSetupError>>>,
    }

    impl FakeNodeLog {
        fn finite(result: Result<NodeLogResult, NodeLogSetupError>) -> Self {
            Self {
                finite: StdMutex::new(Some(result)),
                stream: StdMutex::new(None),
            }
        }

        fn stream(events: Vec<Result<NodeLogEvent, ByteStreamError>>) -> Self {
            Self {
                finite: StdMutex::new(None),
                stream: StdMutex::new(Some(Ok(events))),
            }
        }
    }

    impl NodeLog for FakeNodeLog {
        fn read_logs(&self, _request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
            let result = self
                .finite
                .lock()
                .expect("finite fake mutex poisoned")
                .take()
                .expect("unexpected finite log call");
            Box::pin(async move { result })
        }

        fn open_logs(
            &self,
            _request: NodeLogRequest,
        ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
            let result = self
                .stream
                .lock()
                .expect("stream fake mutex poisoned")
                .take()
                .expect("unexpected streaming log call");
            Box::pin(async move {
                result.map(|events| {
                    Box::new(FakeLogSession {
                        events: Mutex::new(events.into()),
                        cancelled: AtomicBool::new(false),
                    }) as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>
                })
            })
        }
    }

    struct FakeLogSession {
        events: Mutex<VecDeque<Result<NodeLogEvent, ByteStreamError>>>,
        cancelled: AtomicBool,
    }

    impl BoundedByteStream for FakeLogSession {
        type Frame = NodeLogEvent;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(8, 4096).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn send_frame(&self, _frame: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
            Box::pin(async { Err(ByteStreamError::closed("receive-only fake")) })
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
            Box::pin(async move {
                match self.events.lock().await.pop_front() {
                    Some(Ok(event)) => Ok(Some(event)),
                    Some(Err(error)) => Err(error),
                    None => Ok(None),
                }
            })
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.cancelled.store(true, Ordering::Release);
                Ok(())
            })
        }
    }

    fn request(follow: bool) -> NodeLogRequest {
        NodeLogRequest::new(
            NodeLogTarget::try_new("node-a", "default", "pod-a", "uid-a", "main").unwrap(),
            NodeLogOptions::new(
                follow.then(|| "true".to_string()),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
    }

    #[tokio::test]
    async fn phase17c_node_log_finite_and_follow_transport_preserve_terminal_results() {
        let finite = FakeNodeLog::finite(Ok(NodeLogResult::success(b"finite\n".to_vec())));
        assert_eq!(
            read_log_bytes(&finite, request(false), NodeLogOrigin::Local)
                .await
                .unwrap(),
            b"finite\n"
        );

        let terminal = FakeNodeLog::finite(Ok(NodeLogResult::failed(
            Vec::new(),
            NodeLogTerminalError::new("read failed"),
        )));
        let error = read_log_bytes(&terminal, request(false), NodeLogOrigin::Remote)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Internal(message) if message == "remote pod log error: read failed"
        ));

        let follow = Arc::new(FakeNodeLog::stream(vec![
            Ok(NodeLogEvent::data(b"one ".to_vec())),
            Ok(NodeLogEvent::complete(b"two\n".to_vec())),
        ]));
        let body = open_log_body(follow, request(true), NodeLogOrigin::Local)
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(bytes.as_ref(), b"one two\n");
    }

    #[tokio::test]
    async fn phase17c_node_log_websocket_preserves_follow_and_error_close_modes() {
        async fn exchange(
            node_log: Arc<dyn NodeLog>,
            origin: NodeLogOrigin,
            send_terminal_error: bool,
        ) -> Vec<Message> {
            let (server_io, client_io) = tokio::io::duplex(4096);
            let server = tokio_tungstenite::WebSocketStream::from_raw_socket(
                server_io,
                tokio_tungstenite::tungstenite::protocol::Role::Server,
                None,
            )
            .await;
            let mut client = tokio_tungstenite::WebSocketStream::from_raw_socket(
                client_io,
                tokio_tungstenite::tungstenite::protocol::Role::Client,
                None,
            )
            .await;
            let serve = serve_log_websocket(
                server,
                PodLogWebSocketRequest {
                    node_log,
                    request: request(true),
                    origin,
                    follow: true,
                    skip_previous_read: false,
                    send_terminal_error,
                },
            );
            let receive = async move {
                let mut messages = Vec::new();
                while let Some(message) = client.next().await {
                    let message = message.unwrap();
                    let closed = matches!(message, Message::Close(_));
                    messages.push(message);
                    if closed {
                        break;
                    }
                }
                messages
            };
            let (_, messages) = tokio::join!(serve, receive);
            messages
        }

        let success = exchange(
            Arc::new(FakeNodeLog::stream(vec![
                Ok(NodeLogEvent::data(b"stream\n".to_vec())),
                Ok(NodeLogEvent::terminal()),
            ])),
            NodeLogOrigin::Local,
            false,
        )
        .await;
        assert!(matches!(&success[0], Message::Binary(bytes) if bytes.as_ref() == b"stream\n"));
        assert!(matches!(&success[1], Message::Close(_)));

        for (origin, send_terminal_error, expected_messages) in [
            (NodeLogOrigin::Local, false, 1),
            (NodeLogOrigin::Remote, true, 2),
        ] {
            let messages = exchange(
                Arc::new(FakeNodeLog::stream(vec![Ok(NodeLogEvent::failed(
                    Vec::new(),
                    NodeLogTerminalError::new("stream failed"),
                ))])),
                origin,
                send_terminal_error,
            )
            .await;
            assert_eq!(messages.len(), expected_messages);
            if send_terminal_error {
                assert!(
                    matches!(&messages[0], Message::Binary(bytes) if bytes.ends_with(b"stream failed\n"))
                );
            }
            assert!(matches!(messages.last(), Some(Message::Close(_))));
        }
    }
}
