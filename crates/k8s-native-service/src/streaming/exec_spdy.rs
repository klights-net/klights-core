//! Kubernetes SPDY exec/attach channel adaptation.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::spdy_framing::{SpdyConnection, SpdyFrame, StreamType};
use super::*;
use klights_node_api::{
    ExecStreamChannel, ExecStreamOptions as NodeExecStreamOptions, NodeExec, NodeExecRequest,
    NodeExecSession, NodeExecTarget,
};

const SPDY_UPGRADE_VALUE: &str = "SPDY/3.1";
const SPDY_PROTOCOL_HEADER: &str = "X-Stream-Protocol-Version";
const SPDY_EXEC_DATA_CHUNK_BYTES: usize = 64 * 1024;
const OPTIONAL_STREAM_NEGOTIATION_GRACE: std::time::Duration =
    std::time::Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub struct SpdyExecStreamRequest {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
    pub attach: bool,
}

pub struct RemoteExecSpdyRequest {
    pub node_exec: Arc<dyn NodeExec>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub node_name: String,
    pub target: ExecTarget,
    pub stream_request: SpdyExecStreamRequest,
}

#[derive(Debug, Default, Clone)]
pub struct SpdyClientStreams {
    stdin: Option<u32>,
    stdout: Option<u32>,
    stderr: Option<u32>,
    error: Option<u32>,
    resize: Option<u32>,
}

impl SpdyClientStreams {
    fn insert(&mut self, stream_id: u32, stream_type: StreamType) {
        match stream_type {
            StreamType::Stdin => self.stdin = Some(stream_id),
            StreamType::Stdout => self.stdout = Some(stream_id),
            StreamType::Stderr => self.stderr = Some(stream_id),
            StreamType::Error => self.error = Some(stream_id),
            StreamType::Resize => self.resize = Some(stream_id),
            StreamType::Data => {}
        }
    }

    fn stream_id_for(&self, stream_type: StreamType) -> Option<u32> {
        match stream_type {
            StreamType::Stdin => self.stdin,
            StreamType::Stdout => self.stdout,
            StreamType::Stderr => self.stderr,
            StreamType::Error => self.error,
            StreamType::Resize => self.resize,
            StreamType::Data => None,
        }
    }

    fn has_expected(&self, req: SpdyExecStreamRequest) -> bool {
        (!req.stdin || self.stdin.is_some())
            && (!req.stdout || self.stdout.is_some())
            && (!req.stderr || req.tty || self.stderr.is_some())
            && (!req.tty || self.resize.is_some())
    }
}

pub(super) fn is_spdy_upgrade(headers: &header::HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(SPDY_UPGRADE_VALUE))
        .unwrap_or(false)
}

pub(super) fn negotiate_spdy_subprotocol(headers: &header::HeaderMap) -> String {
    const PREFERRED: &[&str] = &[
        "v4.channel.k8s.io",
        "v3.channel.k8s.io",
        "v2.channel.k8s.io",
        "channel.k8s.io",
    ];

    let mut offered = Vec::new();
    for value in headers.get_all(SPDY_PROTOCOL_HEADER) {
        if let Ok(raw) = value.to_str() {
            offered.extend(raw.split(',').map(str::trim).filter(|s| !s.is_empty()));
        }
    }

    for preferred in PREFERRED {
        if offered.iter().any(|offered| offered == preferred) {
            return (*preferred).to_string();
        }
    }

    "v4.channel.k8s.io".to_string()
}

fn spdy_stream_type_from_headers(headers: &HashMap<String, String>) -> Option<StreamType> {
    let raw = headers.iter().find_map(|(key, value)| {
        if key.eq_ignore_ascii_case("streamtype") {
            Some(value.as_str())
        } else {
            None
        }
    })?;
    raw.split('\0')
        .find_map(|value| match value.to_ascii_lowercase().as_str() {
            "stdin" => Some(StreamType::Stdin),
            "stdout" => Some(StreamType::Stdout),
            "stderr" => Some(StreamType::Stderr),
            "error" => Some(StreamType::Error),
            "resize" => Some(StreamType::Resize),
            "data" => Some(StreamType::Data),
            _ => None,
        })
}

pub async fn collect_spdy_client_streams<S>(
    client_spdy: &mut SpdyConnection,
    client_stream: &mut S,
    request: SpdyExecStreamRequest,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<SpdyClientStreams>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut streams = SpdyClientStreams::default();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    while !streams.has_expected(request) {
        tokio::select! {
            timer = task_supervisor.sleep_until("spdy_exec_stream_negotiation", deadline) => {
                timer?;
                anyhow::bail!(
                    "timed out waiting for SPDY exec streams: request={:?}, streams={:?}",
                    request,
                    streams
                );
            }
            frame = client_spdy.read_frame(client_stream) => {
                let frame = frame?;
                frame.trace_control_metadata();
                match frame {
                    SpdyFrame::SynStream { stream_id, headers } => {
                        if let Some(stream_type) = spdy_stream_type_from_headers(&headers) {
                            streams.insert(stream_id, stream_type);
                            client_spdy.write_syn_reply(client_stream, stream_id).await?;
                        } else {
                            tracing::debug!(stream_id, ?headers, "SPDY exec client stream missing streamType");
                        }
                    }
                    SpdyFrame::Ping { id } => {
                        client_spdy.write_ping(client_stream, id).await?;
                    }
                    SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. } => {}
                    SpdyFrame::GoAway | SpdyFrame::RstStream { .. } => {
                        anyhow::bail!("SPDY exec client closed before stream negotiation completed");
                    }
                    SpdyFrame::Data { .. } | SpdyFrame::SynReply { .. } | SpdyFrame::Unknown => {}
                }
            }
        }
    }

    let optional_deadline = tokio::time::Instant::now() + OPTIONAL_STREAM_NEGOTIATION_GRACE;
    while streams.error.is_none() {
        tokio::select! {
            timer = task_supervisor.sleep_until("spdy_exec_optional_stream_negotiation", optional_deadline) => {
                timer?;
                break;
            }
            frame = client_spdy.read_frame(client_stream) => {
                match frame {
                    Ok(SpdyFrame::SynStream { stream_id, headers }) => {
                        if let Some(stream_type) = spdy_stream_type_from_headers(&headers) {
                            let is_error_stream = stream_type == StreamType::Error;
                            streams.insert(stream_id, stream_type);
                            client_spdy.write_syn_reply(client_stream, stream_id).await?;
                            if is_error_stream {
                                break;
                            }
                        } else {
                            tracing::debug!(stream_id, ?headers, "SPDY exec optional client stream missing streamType");
                        }
                    }
                    Ok(SpdyFrame::Ping { id }) => {
                        client_spdy.write_ping(client_stream, id).await?;
                    }
                    Ok(SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. }) => {}
                    Ok(SpdyFrame::GoAway | SpdyFrame::RstStream { .. }) => break,
                    Ok(SpdyFrame::Data { .. } | SpdyFrame::SynReply { .. } | SpdyFrame::Unknown) => {}
                    Err(err) => {
                        tracing::debug!("SPDY exec optional stream negotiation ended: {}", err);
                        break;
                    }
                }
            }
        }
    }

    Ok(streams)
}

pub async fn write_spdy_exec_channel_frame<S>(
    client_spdy: &SpdyConnection,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    channel: StreamType,
    data: &[u8],
    fin: bool,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if let Some(stream_id) = streams.stream_id_for(channel) {
        if data.is_empty() {
            client_spdy
                .write_data_frame(client_stream, stream_id, data, fin)
                .await?;
        } else {
            let chunk_count = data.len().div_ceil(SPDY_EXEC_DATA_CHUNK_BYTES);
            for (index, chunk) in data.chunks(SPDY_EXEC_DATA_CHUNK_BYTES).enumerate() {
                client_spdy
                    .write_data_frame(
                        client_stream,
                        stream_id,
                        chunk,
                        fin && index + 1 == chunk_count,
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct SpdyExecOutputState {
    stdout_finished: bool,
    stderr_finished: bool,
    error_finished: bool,
}

impl SpdyExecOutputState {
    fn is_finished(&self, channel: StreamType) -> bool {
        match channel {
            StreamType::Stdout => self.stdout_finished,
            StreamType::Stderr => self.stderr_finished,
            StreamType::Error => self.error_finished,
            StreamType::Stdin | StreamType::Resize | StreamType::Data => false,
        }
    }

    fn record_fin(&mut self, channel: StreamType) {
        match channel {
            StreamType::Stdout => self.stdout_finished = true,
            StreamType::Stderr => self.stderr_finished = true,
            StreamType::Error => self.error_finished = true,
            StreamType::Stdin | StreamType::Resize | StreamType::Data => {}
        }
    }
}

async fn write_spdy_exec_output<S>(
    client_spdy: &SpdyConnection,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    state: &mut SpdyExecOutputState,
    channel: StreamType,
    data: &[u8],
    fin: bool,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    anyhow::ensure!(
        !state.is_finished(channel),
        "SPDY exec runtime emitted data after {channel:?} FIN"
    );
    write_spdy_exec_channel_frame(client_spdy, client_stream, streams, channel, data, fin).await?;
    if fin {
        state.record_fin(channel);
    }
    Ok(())
}

async fn finish_spdy_exec_output_streams<S>(
    client_spdy: &SpdyConnection,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    state: &mut SpdyExecOutputState,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    for channel in [StreamType::Stdout, StreamType::Stderr] {
        if streams.stream_id_for(channel).is_some() && !state.is_finished(channel) {
            write_spdy_exec_output(
                client_spdy,
                client_stream,
                streams,
                state,
                channel,
                &[],
                true,
            )
            .await?;
        }
    }
    Ok(())
}

async fn write_spdy_exec_error<S>(
    client_spdy: &SpdyConnection,
    client_stream: &mut S,
    streams: &SpdyClientStreams,
    state: &mut SpdyExecOutputState,
    message: String,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    finish_spdy_exec_output_streams(client_spdy, client_stream, streams, state).await?;
    let payload = serde_json::json!({
        "metadata": {},
        "status": "Failure",
        "message": message,
        "details": {"causes": []}
    })
    .to_string();
    write_spdy_exec_output(
        client_spdy,
        client_stream,
        streams,
        state,
        StreamType::Error,
        payload.as_bytes(),
        true,
    )
    .await
}

async fn bridge_remote_exec_full_duplex<S>(
    mut client_spdy: SpdyConnection,
    client_stream: S,
    streams: SpdyClientStreams,
    mut session: Box<dyn NodeExecSession>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut client_read, mut client_write) = tokio::io::split(client_stream);
    let client_writer = SpdyConnection::new();
    let mut pending_input: Option<klights_node_api::NodeExecFrame> = None;
    let mut pending_output: Option<klights_node_api::NodeExecFrame> = None;
    let mut output_state = SpdyExecOutputState::default();

    let relay_result = loop {
        tokio::select! {
            result = async {
                let frame = pending_input.as_ref().expect("guarded pending input");
                session.send_frame(frame.clone()).await
            }, if pending_input.is_some() => {
                if let Err(error) = result {
                    break Err(error.into());
                }
                pending_input = None;
            }
            result = async {
                let frame: &klights_node_api::NodeExecFrame =
                    pending_output.as_ref().expect("guarded pending output");
                let channel = match frame.channel() {
                    ExecStreamChannel::Stdout => Some(StreamType::Stdout),
                    ExecStreamChannel::Stderr => Some(StreamType::Stderr),
                    ExecStreamChannel::Error => Some(StreamType::Error),
                    ExecStreamChannel::Stdin | ExecStreamChannel::Resize => None,
                };
                if let Some(channel) = channel {
                    let terminal = frame.is_terminal();
                    if terminal {
                        finish_spdy_exec_output_streams(
                            &client_writer,
                            &mut client_write,
                            &streams,
                            &mut output_state,
                        ).await?;
                    }
                    write_spdy_exec_output(
                        &client_writer,
                        &mut client_write,
                        &streams,
                        &mut output_state,
                        channel,
                        frame.data(),
                        frame.fin() || terminal,
                    ).await?;
                }
                Ok::<bool, anyhow::Error>(frame.is_terminal())
            }, if pending_output.is_some() => {
                let terminal = match result {
                    Ok(terminal) => terminal,
                    Err(error) => {
                        break Err(error);
                    }
                };
                pending_output = None;
                if terminal {
                    break Ok(());
                }
            }
            inbound = client_spdy.read_frame(&mut client_read), if pending_input.is_none() => {
                let inbound = match inbound {
                    Ok(frame) => frame,
                    Err(error) => {
                        break Err(error);
                    }
                };
                match inbound {
                    SpdyFrame::Data { stream_id, data, fin } => {
                        let channel = if Some(stream_id) == streams.stdin {
                            Some(ExecStreamChannel::Stdin)
                        } else if Some(stream_id) == streams.resize {
                            Some(ExecStreamChannel::Resize)
                        } else {
                            None
                        };
                        if let Some(channel) = channel {
                            pending_input = Some(klights_node_api::NodeExecFrame::new(
                                channel, data, fin,
                            ));
                        }
                    }
                    SpdyFrame::Ping { id } => {
                        if let Err(error) = client_writer.write_ping(&mut client_write, id).await {
                            break Err(error);
                        }
                    }
                    SpdyFrame::RstStream { .. } | SpdyFrame::GoAway => {
                        break Ok(());
                    }
                    SpdyFrame::SynReply { .. }
                    | SpdyFrame::Settings
                    | SpdyFrame::WindowUpdate { .. }
                    | SpdyFrame::Unknown
                    | SpdyFrame::SynStream { .. } => {}
                }
            }
            outbound = session.recv_frame(), if pending_output.is_none() => {
                let outbound = match outbound {
                    Ok(frame) => frame,
                    Err(error) => {
                        let runtime_error = anyhow::Error::from(error);
                        if let Err(write_error) = write_spdy_exec_error(
                            &client_writer,
                            &mut client_write,
                            &streams,
                            &mut output_state,
                            format!("remote exec runtime failed: {runtime_error}"),
                        ).await {
                            break Err(write_error);
                        }
                        break Err(runtime_error);
                    }
                };
                match outbound {
                    Some(frame) => pending_output = Some(frame),
                    None => {
                        let error = anyhow::anyhow!(
                            "remote exec runtime ended before terminal status"
                        );
                        if let Err(write_error) = write_spdy_exec_error(
                            &client_writer,
                            &mut client_write,
                            &streams,
                            &mut output_state,
                            error.to_string(),
                        ).await {
                            break Err(write_error);
                        }
                        break Err(error);
                    }
                }
            }
        }
    };

    let _ = session.cancel().await;
    let flush_result = client_write.flush().await;
    let shutdown_result = client_write.shutdown().await;
    match (relay_result, flush_result, shutdown_result) {
        (Err(error), _, _) => Err(error),
        (Ok(()), Err(error), _) | (Ok(()), Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(()), Ok(())) => Ok(()),
    }
}

pub(super) async fn handle_remote_exec_spdy<S>(mut client_stream: S, request: RemoteExecSpdyRequest)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let RemoteExecSpdyRequest {
        node_exec,
        task_supervisor,
        node_name,
        target,
        stream_request: request,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let mut client_spdy = SpdyConnection::new();
    let streams = match collect_spdy_client_streams(
        &mut client_spdy,
        &mut client_stream,
        request,
        task_supervisor.as_ref(),
    )
    .await
    {
        Ok(streams) => streams,
        Err(err) => {
            tracing::error!("Remote SPDY exec stream negotiation failed: {}", err);
            let _ = client_stream.shutdown().await;
            return;
        }
    };

    let target = match NodeExecTarget::try_new(
        node_name,
        namespace.clone(),
        pod_name.clone(),
        container_id.clone(),
    ) {
        Ok(target) => target,
        Err(error) => {
            tracing::error!(%error, "Remote SPDY exec target validation failed");
            let _ = client_stream.shutdown().await;
            return;
        }
    };
    let options =
        NodeExecStreamOptions::new(request.stdin, request.stdout, request.stderr, request.tty);
    let node_request = if request.attach {
        NodeExecRequest::attach(target, options)
    } else {
        NodeExecRequest::exec(target, command.clone(), options)
    };
    let session = node_exec.open_exec(node_request).await;

    match session {
        Ok(session) => {
            if let Err(err) =
                bridge_remote_exec_full_duplex(client_spdy, client_stream, streams, session).await
            {
                tracing::error!(
                    "Remote SPDY exec failed: pod={}/{}, container={}, error={}",
                    namespace,
                    pod_name,
                    container_id,
                    err
                );
            }
            tracing::info!("Remote SPDY exec completed: pod={}/{}", namespace, pod_name);
            return;
        }
        Err(err) => {
            tracing::error!("Remote SPDY exec stream open failed: {}", err);
            let mut output_state = SpdyExecOutputState::default();
            let _ = write_spdy_exec_error(
                &client_spdy,
                &mut client_stream,
                &streams,
                &mut output_state,
                err.to_string(),
            )
            .await;
        }
    }

    let _ = client_stream.shutdown().await;
    tracing::info!("Remote SPDY exec completed: pod={}/{}", namespace, pod_name);
}

pub(super) fn spdy_switching_protocols_response(subprotocol: String) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, SPDY_UPGRADE_VALUE)
        .header(header::CONNECTION, "Upgrade")
        .header(SPDY_PROTOCOL_HEADER, subprotocol)
        .body(axum::body::Body::empty())
        .map_err(|err| AppError::Internal(format!("Failed to build SPDY response: {err}")))
}

#[cfg(test)]
mod remote_full_duplex_tests {
    use super::*;
    use klights_node_api::{
        BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeExecFrame,
    };
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, ReadBuf};
    use tokio::sync::mpsc;

    const EXPECTED_MAX_DATA_CHUNK_BYTES: usize = 64 * 1024;

    struct ShutdownTrackedIo<S> {
        inner: S,
        shutdown_called: std::sync::Arc<AtomicBool>,
    }

    impl<S> ShutdownTrackedIo<S> {
        fn new(inner: S, shutdown_called: std::sync::Arc<AtomicBool>) -> Self {
            Self {
                inner,
                shutdown_called,
            }
        }
    }

    impl<S: AsyncRead + Unpin> AsyncRead for ShutdownTrackedIo<S> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl<S: AsyncWrite + Unpin> AsyncWrite for ShutdownTrackedIo<S> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            self.shutdown_called.store(true, Ordering::Release);
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[derive(Debug)]
    struct PeerDataFrame {
        stream_id: u32,
        data: Vec<u8>,
        fin: bool,
    }

    async fn read_peer_data_frame<S>(stream: &mut S) -> std::io::Result<Option<PeerDataFrame>>
    where
        S: AsyncRead + Unpin,
    {
        let mut header = [0_u8; 8];
        let first = stream.read(&mut header[..1]).await?;
        if first == 0 {
            return Ok(None);
        }
        stream.read_exact(&mut header[1..]).await?;
        assert_eq!(header[0] & 0x80, 0, "peer expected a SPDY DATA frame");
        let stream_id = u32::from_be_bytes([header[0] & 0x7f, header[1], header[2], header[3]]);
        let fin = header[4] & 0x01 != 0;
        let length = u32::from_be_bytes([0, header[5], header[6], header[7]]) as usize;
        let mut data = vec![0_u8; length];
        stream.read_exact(&mut data).await?;
        Ok(Some(PeerDataFrame {
            stream_id,
            data,
            fin,
        }))
    }

    struct FakeSession {
        input_tx: mpsc::Sender<NodeExecFrame>,
        output_rx: tokio::sync::Mutex<mpsc::Receiver<NodeExecFrame>>,
        cancelled: std::sync::Arc<AtomicBool>,
    }

    impl BoundedByteStream for FakeSession {
        type Frame = NodeExecFrame;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(1, 1).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.input_tx
                    .send(frame)
                    .await
                    .map_err(|_| ByteStreamError::closed("input closed"))
            })
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
            Box::pin(async move { Ok(self.output_rx.lock().await.recv().await) })
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.cancelled.store(true, Ordering::Release);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn remote_spdy_relays_input_and_output_while_input_is_backpressured() {
        let (server_io, mut client_io) = tokio::io::duplex(4096);
        let (input_tx, mut input_rx) = mpsc::channel(1);
        input_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                b"occupied".to_vec(),
                false,
            ))
            .await
            .unwrap();
        let (output_tx, output_rx) = mpsc::channel(1);
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let session = Box::new(FakeSession {
            input_tx,
            output_rx: tokio::sync::Mutex::new(output_rx),
            cancelled: cancelled.clone(),
        });
        let streams = SpdyClientStreams {
            stdin: Some(1),
            stdout: Some(3),
            stderr: None,
            error: Some(5),
            resize: None,
        };
        let relay = tokio::spawn(bridge_remote_exec_full_duplex(
            SpdyConnection::new(),
            server_io,
            streams,
            session,
        ));
        let client_spdy = SpdyConnection::new();
        client_spdy
            .write_data_frame(&mut client_io, 1, b"blocked-input", false)
            .await
            .unwrap();
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdout,
                b"output-progress".to_vec(),
                false,
            ))
            .await
            .unwrap();

        let mut decoder = SpdyConnection::new();
        let frame = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            decoder.read_frame(&mut client_io),
        )
        .await
        .expect("output must progress while input delivery is backpressured")
        .unwrap();
        assert!(
            matches!(frame, SpdyFrame::Data { stream_id: 3, ref data, fin: false } if data == b"output-progress")
        );

        assert_eq!(input_rx.recv().await.unwrap().data(), b"occupied");
        assert_eq!(input_rx.recv().await.unwrap().data(), b"blocked-input");
        drop(client_io);
        assert!(relay.await.unwrap().is_err());
        assert!(cancelled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn sonobuoy_stdout_is_chunked_byte_exact_and_closed_before_status_and_eof() {
        let payload_len = 8 * 1024 * 1024 + 37;
        let payload: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();
        let (server_io, mut peer_io) = tokio::io::duplex(128 * 1024);
        let shutdown_called = std::sync::Arc::new(AtomicBool::new(false));
        let server_io = ShutdownTrackedIo::new(server_io, shutdown_called.clone());
        let (input_tx, _input_rx) = mpsc::channel(1);
        let (output_tx, output_rx) = mpsc::channel(3);
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdout,
                payload.clone(),
                false,
            ))
            .await
            .unwrap();
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Stdout,
                Vec::new(),
                true,
            ))
            .await
            .unwrap();
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Error,
                br#"{"status":"Success","metadata":{}}"#.to_vec(),
                true,
            ))
            .await
            .unwrap();
        drop(output_tx);
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let session = Box::new(FakeSession {
            input_tx,
            output_rx: tokio::sync::Mutex::new(output_rx),
            cancelled: cancelled.clone(),
        });
        let streams = SpdyClientStreams {
            stdin: None,
            stdout: Some(3),
            stderr: None,
            error: Some(5),
            resize: None,
        };
        let relay = tokio::spawn(bridge_remote_exec_full_duplex(
            SpdyConnection::new(),
            server_io,
            streams,
            session,
        ));

        let mut received = Vec::with_capacity(payload_len);
        let mut saw_stdout_fin = false;
        let mut saw_status_fin = false;
        while let Some(frame) = read_peer_data_frame(&mut peer_io).await.unwrap() {
            assert!(
                frame.data.len() <= EXPECTED_MAX_DATA_CHUNK_BYTES,
                "SPDY DATA payload exceeded the bounded chunk policy: {} bytes",
                frame.data.len()
            );
            match frame.stream_id {
                3 => {
                    assert!(!saw_stdout_fin, "stdout data followed stdout FIN");
                    received.extend_from_slice(&frame.data);
                    if frame.fin {
                        saw_stdout_fin = true;
                    }
                }
                5 => {
                    assert!(saw_stdout_fin, "terminal status preceded stdout FIN");
                    assert!(
                        !frame.data.is_empty(),
                        "terminal status payload is required"
                    );
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&frame.data).unwrap()["status"],
                        "Success"
                    );
                    assert!(frame.fin, "terminal status must carry FIN");
                    saw_status_fin = true;
                }
                other => panic!("unexpected peer stream {other}"),
            }
        }

        assert_eq!(received, payload);
        assert!(saw_stdout_fin);
        assert!(saw_status_fin);
        assert!(relay.await.unwrap().is_ok());
        assert!(cancelled.load(Ordering::Acquire));
        assert!(shutdown_called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn terminal_failure_synthesizes_requested_output_fins_before_status_and_shutdown() {
        let (server_io, mut peer_io) = tokio::io::duplex(4096);
        let shutdown_called = std::sync::Arc::new(AtomicBool::new(false));
        let server_io = ShutdownTrackedIo::new(server_io, shutdown_called.clone());
        let (input_tx, _input_rx) = mpsc::channel(1);
        let (output_tx, output_rx) = mpsc::channel(1);
        output_tx
            .send(NodeExecFrame::new(
                ExecStreamChannel::Error,
                br#"{"status":"Failure","message":"runtime failed","metadata":{}}"#.to_vec(),
                false,
            ))
            .await
            .unwrap();
        drop(output_tx);
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let session = Box::new(FakeSession {
            input_tx,
            output_rx: tokio::sync::Mutex::new(output_rx),
            cancelled: cancelled.clone(),
        });
        let streams = SpdyClientStreams {
            stdin: None,
            stdout: Some(3),
            stderr: Some(5),
            error: Some(7),
            resize: None,
        };
        let relay = tokio::spawn(bridge_remote_exec_full_duplex(
            SpdyConnection::new(),
            server_io,
            streams,
            session,
        ));

        let stdout_fin = read_peer_data_frame(&mut peer_io).await.unwrap().unwrap();
        assert_eq!(stdout_fin.stream_id, 3);
        assert!(stdout_fin.data.is_empty());
        assert!(stdout_fin.fin);
        let stderr_fin = read_peer_data_frame(&mut peer_io).await.unwrap().unwrap();
        assert_eq!(stderr_fin.stream_id, 5);
        assert!(stderr_fin.data.is_empty());
        assert!(stderr_fin.fin);
        let status = read_peer_data_frame(&mut peer_io).await.unwrap().unwrap();
        assert_eq!(status.stream_id, 7);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&status.data).unwrap()["status"],
            "Failure"
        );
        assert!(status.fin);
        assert!(read_peer_data_frame(&mut peer_io).await.unwrap().is_none());

        assert!(relay.await.unwrap().is_ok());
        assert!(cancelled.load(Ordering::Acquire));
        assert!(shutdown_called.load(Ordering::Acquire));
    }
}
