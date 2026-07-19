use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use klights_node_api::{
    BoundedByteStream, ByteFrame, ByteStreamBounds, ByteStreamError, ByteStreamFuture,
    ExecStreamChannel, NodeExecFrame, NodeLogEvent, NodePortForwardFrame,
};

#[derive(Debug)]
struct ProbeFrame(Vec<u8>);

impl ByteFrame for ProbeFrame {
    fn payload(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Default)]
struct StreamState {
    frames: VecDeque<ProbeFrame>,
    send_waker: Option<Waker>,
    receive_waker: Option<Waker>,
    cancelled: bool,
    closed: bool,
    cleaned: bool,
}

struct ContractStream {
    bounds: ByteStreamBounds,
    state: Arc<Mutex<StreamState>>,
    cleanups: Arc<AtomicUsize>,
}

impl ContractStream {
    fn new(bounds: ByteStreamBounds) -> (Self, StreamControl) {
        let state = Arc::new(Mutex::new(StreamState::default()));
        let cleanups = Arc::new(AtomicUsize::new(0));
        (
            Self {
                bounds,
                state: state.clone(),
                cleanups: cleanups.clone(),
            },
            StreamControl { state, cleanups },
        )
    }

    fn cleanup_once(&self, state: &mut StreamState) {
        if !state.cleaned {
            state.cleaned = true;
            self.cleanups.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl BoundedByteStream for ContractStream {
    type Frame = ProbeFrame;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().cancelled
    }

    fn send_frame(&self, frame: ProbeFrame) -> ByteStreamFuture<'_, ()> {
        let mut frame = Some(frame);
        Box::pin(poll_fn(move |context| {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return Poll::Ready(Err(ByteStreamError::cancelled()));
            }
            if state.closed {
                return Poll::Ready(Err(ByteStreamError::closed("stream is closed")));
            }
            if state.frames.len() >= self.bounds.send_frames().get() {
                state.send_waker = Some(context.waker().clone());
                return Poll::Pending;
            }
            state
                .frames
                .push_back(frame.take().expect("send frame is consumed exactly once"));
            if let Some(waker) = state.receive_waker.take() {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        }))
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<ProbeFrame>> {
        Box::pin(poll_fn(move |context| {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return Poll::Ready(Err(ByteStreamError::cancelled()));
            }
            if let Some(frame) = state.frames.pop_front() {
                if let Some(waker) = state.send_waker.take() {
                    waker.wake();
                }
                return Poll::Ready(Ok(Some(frame)));
            }
            if state.closed {
                return Poll::Ready(Ok(None));
            }
            state.receive_waker = Some(context.waker().clone());
            Poll::Pending
        }))
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
            state.closed = true;
            self.cleanup_once(&mut state);
            if let Some(waker) = state.send_waker.take() {
                waker.wake();
            }
            if let Some(waker) = state.receive_waker.take() {
                waker.wake();
            }
            Ok(())
        })
    }
}

impl Drop for ContractStream {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.cleanup_once(&mut state);
        if let Some(waker) = state.send_waker.take() {
            waker.wake();
        }
        if let Some(waker) = state.receive_waker.take() {
            waker.wake();
        }
    }
}

struct StreamControl {
    state: Arc<Mutex<StreamState>>,
    cleanups: Arc<AtomicUsize>,
}

impl StreamControl {
    fn drain_one(&self) -> Option<ProbeFrame> {
        let mut state = self.state.lock().unwrap();
        let frame = state.frames.pop_front();
        if frame.is_some()
            && let Some(waker) = state.send_waker.take()
        {
            waker.wake();
        }
        frame
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        if let Some(waker) = state.receive_waker.take() {
            waker.wake();
        }
    }

    fn cleanup_count(&self) -> usize {
        self.cleanups.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_with_waker<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = Box::pin(future);
    poll_with_waker(future.as_mut(), Waker::noop())
}

#[test]
fn finite_bounds_reject_every_zero_dimension() {
    for (send, receive, field) in [
        (0, 1, "byte_stream.send_frames"),
        (1, 0, "byte_stream.receive_frames"),
        (0, 0, "byte_stream.send_frames"),
    ] {
        let error = ByteStreamBounds::try_new(send, receive).unwrap_err();
        assert_eq!(error.field(), field);
    }
}

#[test]
fn full_send_applies_async_backpressure_then_wakes_and_resumes() {
    let (stream, control) = ContractStream::new(ByteStreamBounds::try_new(1, 1).unwrap());
    assert!(matches!(
        poll_once(stream.send_frame(ProbeFrame(b"first".to_vec()))),
        Poll::Ready(Ok(()))
    ));

    let mut blocked = Box::pin(stream.send_frame(ProbeFrame(b"second".to_vec())));
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(wake_counter.clone());
    assert!(matches!(
        poll_with_waker(blocked.as_mut(), &waker),
        Poll::Pending
    ));
    assert_eq!(control.drain_one().unwrap().payload(), b"first");
    assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_with_waker(blocked.as_mut(), &waker),
        Poll::Ready(Ok(()))
    ));
    drop(blocked);
    assert_eq!(control.drain_one().unwrap().payload(), b"second");
}

#[test]
fn cancellation_and_drop_cleanup_are_idempotent() {
    for cancel_count in [0, 1, 2] {
        let (mut stream, control) = ContractStream::new(ByteStreamBounds::try_new(1, 1).unwrap());
        for _ in 0..cancel_count {
            assert!(matches!(poll_once(stream.cancel()), Poll::Ready(Ok(()))));
        }
        if cancel_count > 0 {
            assert!(stream.is_cancelled());
            assert!(matches!(
                poll_once(stream.recv_frame()),
                Poll::Ready(Err(ByteStreamError::Cancelled))
            ));
        }
        drop(stream);
        assert_eq!(control.cleanup_count(), 1, "cancel_count={cancel_count}");
    }
}

#[test]
fn terminal_frames_and_clean_close_remain_distinct() {
    for (name, terminal) in [
        (
            "exec-status",
            NodeExecFrame::new(
                ExecStreamChannel::Error,
                br#"{"status":"Success"}"#.to_vec(),
                false,
            )
            .is_terminal(),
        ),
        (
            "exec-data",
            NodeExecFrame::new(ExecStreamChannel::Stdout, Vec::new(), true).is_terminal(),
        ),
        ("log-terminal", NodeLogEvent::terminal().is_terminal()),
        ("log-data", NodeLogEvent::data(Vec::new()).is_terminal()),
        ("port-error", false),
    ] {
        let expected = matches!(name, "exec-status" | "log-terminal");
        assert_eq!(terminal, expected, "case={name}");
    }
    assert!(NodePortForwardFrame::error(0, Vec::new()).is_error());

    let (stream, control) = ContractStream::new(ByteStreamBounds::try_new(1, 1).unwrap());
    control.close();
    assert!(matches!(
        poll_once(stream.recv_frame()),
        Poll::Ready(Ok(None))
    ));
    assert!(!stream.is_cancelled());
}
