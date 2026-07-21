//! Dependency-free reference conformance checks for networking port adapters.

use std::collections::VecDeque;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use klights_types::PodIdentity;

use crate::*;

struct PendingOnce<T> {
    value: Option<T>,
    yielded: bool,
    cancelled: Arc<AtomicBool>,
}

impl<T: Unpin> Future for PendingOnce<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        if !self.yielded {
            self.yielded = true;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(
            self.value
                .take()
                .expect("reference future polled after completion"),
        )
    }
}

impl<T> Drop for PendingOnce<T> {
    fn drop(&mut self) {
        if self.value.is_some() {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

fn pending_once<T: Send + Unpin + 'static>(
    value: T,
    cancelled: Arc<AtomicBool>,
) -> Pin<Box<dyn Future<Output = T> + Send>> {
    Box::pin(PendingOnce {
        value: Some(value),
        yielded: false,
        cancelled,
    })
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn pending_then_ready<'a, T>(mut future: Pin<Box<dyn Future<Output = T> + Send + 'a>>) -> T {
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("reference future stayed pending after its wake"),
    }
}

fn assert_cancelled<'a, T>(
    mut future: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
    flag: &AtomicBool,
) {
    let waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    drop(future);
    assert!(flag.load(Ordering::Acquire));
}

struct ReferenceDatapath {
    fail: bool,
    cancelled: Arc<AtomicBool>,
}

impl Datapath for ReferenceDatapath {
    fn cni_add(&self, _request: CniAddRequest) -> DatapathFuture<'_, PodNetwork> {
        let result = if self.fail {
            Err(DatapathError::setup("setup failed"))
        } else {
            Ok(PodNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 2))))
        };
        pending_once(result, self.cancelled.clone())
    }

    fn cni_del<'a>(&'a self, _sandbox_id: &'a SandboxId) -> DatapathFuture<'a, ()> {
        pending_once(
            if self.fail {
                Err(DatapathError::teardown("teardown failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }

    fn host_ip(&self) -> DatapathFuture<'_, IpAddr> {
        pending_once(
            if self.fail {
                Err(DatapathError::address("address failed"))
            } else {
                Ok(IpAddr::V4(Ipv4Addr::LOCALHOST))
            },
            self.cancelled.clone(),
        )
    }

    fn pod_gateway_ip(&self) -> DatapathFuture<'_, IpAddr> {
        self.host_ip()
    }

    fn shutdown(&self) -> DatapathFuture<'_, ()> {
        pending_once(
            if self.fail {
                Err(DatapathError::shutdown("shutdown failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }
}

struct ReferencePeerRouter {
    fail: bool,
    cancelled: Arc<AtomicBool>,
}

impl PeerRouter for ReferencePeerRouter {
    fn apply_peer_route<'a>(&'a self, _route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        pending_once(
            if self.fail {
                Err(PeerRouterError::apply("apply failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }

    fn remove_peer_route<'a>(&'a self, _route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        pending_once(
            if self.fail {
                Err(PeerRouterError::remove("remove failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }
}

struct ReferenceServiceRouter {
    fail: bool,
    cancelled: Arc<AtomicBool>,
}

impl ServiceRouter for ReferenceServiceRouter {
    fn request_services_sync(&self) -> Result<(), ServiceRouterError> {
        if self.fail {
            Err(ServiceRouterError::sync("request failed"))
        } else {
            Ok(())
        }
    }

    fn sync_services_now(&self) -> ServiceRouterFuture<'_> {
        pending_once(
            if self.fail {
                Err(ServiceRouterError::sync("sync failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }

    fn add_hostport_rules(&self, _request: HostPortRules) -> ServiceRouterFuture<'_> {
        pending_once(
            if self.fail {
                Err(ServiceRouterError::hostport("add failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }

    fn remove_hostport_rules(&self, _request: HostPortRemoval) -> ServiceRouterFuture<'_> {
        pending_once(
            if self.fail {
                Err(ServiceRouterError::hostport("remove failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }

    fn cleanup(&self) -> ServiceRouterFuture<'_> {
        pending_once(
            if self.fail {
                Err(ServiceRouterError::cleanup("cleanup failed"))
            } else {
                Ok(())
            },
            self.cancelled.clone(),
        )
    }
}

struct ReferenceResolver {
    fail: bool,
    cancelled: Arc<AtomicBool>,
}

impl PodEndpointResolver for ReferenceResolver {
    fn resolve(&self, pod_ip: Ipv4Addr) -> PodEndpointFuture<'_, Option<PodEndpoint>> {
        let result = if self.fail {
            Err(PodEndpointError::resolve("resolve failed"))
        } else {
            DirectPodEndpoint::try_new(pod_ip, "node-a")
                .map(|endpoint| Some(PodEndpoint::EncryptedDirect(endpoint)))
        };
        pending_once(result, self.cancelled.clone())
    }
}

struct BoundedEventStream {
    queue: VecDeque<Result<PodEndpointEvent, PodEndpointError>>,
    capacity: usize,
    waiter: Option<Waker>,
    cancelled: Arc<AtomicBool>,
}

impl BoundedEventStream {
    fn empty(capacity: usize, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity,
            waiter: None,
            cancelled,
        }
    }

    fn push(&mut self, event: PodEndpointEvent) -> Result<(), PodEndpointError> {
        if self.queue.len() == self.capacity {
            return Err(PodEndpointError::event_source(
                "bounded event source is full",
            ));
        }
        self.queue.push_back(Ok(event));
        if let Some(waker) = self.waiter.take() {
            waker.wake();
        }
        Ok(())
    }
}

impl PodEndpointEventSubscription for BoundedEventStream {
    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<PodEndpointEvent, PodEndpointError>>> {
        if let Some(event) = self.queue.pop_front() {
            Poll::Ready(Some(event))
        } else {
            self.waiter = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for BoundedEventStream {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

struct ReferenceEventSource {
    fail: bool,
    cancelled: Arc<AtomicBool>,
}

impl PodEndpointEventSource for ReferenceEventSource {
    fn subscribe(&self) -> PodEndpointFuture<'_, PodEndpointEventStream> {
        let result = if self.fail {
            Err(PodEndpointError::event_source("subscribe failed"))
        } else {
            let mut stream = BoundedEventStream::empty(1, self.cancelled.clone());
            stream.push(PodEndpointEvent::Resync(Vec::new())).unwrap();
            Ok(Box::pin(stream) as PodEndpointEventStream)
        };
        pending_once(result, self.cancelled.clone())
    }
}

/// Run the dependency-free reference suite used by contract and adapter tests.
#[allow(clippy::too_many_lines)]
pub fn run_reference_suite() {
    let sandbox = SandboxId::try_new("sandbox-a").unwrap();
    let add = CniAddRequest::try_new(
        "sandbox-a",
        PodIdentity::new("default", "pod-a", "uid-a"),
        "/proc/1/ns/net",
        "/run/netns/pod-a",
        false,
    )
    .unwrap();
    let flag = Arc::new(AtomicBool::new(false));
    let datapath = ReferenceDatapath {
        fail: false,
        cancelled: flag.clone(),
    };
    assert!(pending_then_ready(datapath.cni_add(add)).is_ok());
    assert!(pending_then_ready(datapath.cni_del(&sandbox)).is_ok());
    assert!(pending_then_ready(datapath.host_ip()).is_ok());
    assert!(pending_then_ready(datapath.shutdown()).is_ok());
    assert_cancelled(datapath.host_ip(), flag.as_ref());
    let failed = ReferenceDatapath {
        fail: true,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        pending_then_ready(failed.cni_del(&sandbox)),
        Err(DatapathError::Teardown { .. })
    ));

    let route = PeerRoute::Direct(
        DirectPeerRoute::try_new(
            "node-b",
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
            "10.42.2.0/24",
        )
        .unwrap(),
    );
    let peer_flag = Arc::new(AtomicBool::new(false));
    let peer = ReferencePeerRouter {
        fail: false,
        cancelled: peer_flag.clone(),
    };
    assert!(pending_then_ready(peer.apply_peer_route(&route)).is_ok());
    assert_cancelled(peer.remove_peer_route(&route), peer_flag.as_ref());
    let failed_peer = ReferencePeerRouter {
        fail: true,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        pending_then_ready(failed_peer.apply_peer_route(&route)),
        Err(PeerRouterError::Apply { .. })
    ));

    let binding = HostPortBinding::try_new(None, 30_080, 80, HostPortProtocol::Tcp).unwrap();
    let service_flag = Arc::new(AtomicBool::new(false));
    let services = ReferenceServiceRouter {
        fail: false,
        cancelled: service_flag.clone(),
    };
    services.request_services_sync().unwrap();
    assert!(pending_then_ready(services.sync_services_now()).is_ok());
    assert!(
        pending_then_ready(services.add_hostport_rules(
            HostPortRules::try_new(Ipv4Addr::new(10, 42, 0, 2), vec![binding]).unwrap()
        ))
        .is_ok()
    );
    assert_cancelled(services.cleanup(), service_flag.as_ref());
    let failed_services = ReferenceServiceRouter {
        fail: true,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        failed_services.request_services_sync(),
        Err(ServiceRouterError::Sync { .. })
    ));
    assert!(matches!(
        pending_then_ready(failed_services.cleanup()),
        Err(ServiceRouterError::Cleanup { .. })
    ));

    let resolver_flag = Arc::new(AtomicBool::new(false));
    let resolver = ReferenceResolver {
        fail: false,
        cancelled: resolver_flag.clone(),
    };
    assert!(
        pending_then_ready(resolver.resolve(Ipv4Addr::new(10, 42, 0, 2)))
            .unwrap()
            .is_some()
    );
    assert_cancelled(
        resolver.resolve(Ipv4Addr::new(10, 42, 0, 3)),
        resolver_flag.as_ref(),
    );
    let failed_resolver = ReferenceResolver {
        fail: true,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        pending_then_ready(failed_resolver.resolve(Ipv4Addr::new(10, 42, 0, 4))),
        Err(PodEndpointError::Resolve { .. })
    ));

    let source_flag = Arc::new(AtomicBool::new(false));
    let source = ReferenceEventSource {
        fail: false,
        cancelled: source_flag.clone(),
    };
    let mut subscription = pending_then_ready(source.subscribe()).unwrap();
    let waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(subscription.as_mut().poll_next(&mut context), Poll::Ready(Some(Ok(PodEndpointEvent::Resync(events)))) if events.is_empty())
    );
    drop(subscription);
    assert!(source_flag.load(Ordering::Acquire));

    let stream_flag = Arc::new(AtomicBool::new(false));
    let mut stream = Box::pin(BoundedEventStream::empty(1, stream_flag));
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(wake_counter.clone());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        stream.as_mut().poll_next(&mut context),
        Poll::Pending
    ));
    stream
        .as_mut()
        .get_mut()
        .push(PodEndpointEvent::Resync(Vec::new()))
        .unwrap();
    assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
    assert!(matches!(
        stream
            .as_mut()
            .get_mut()
            .push(PodEndpointEvent::Delete(Ipv4Addr::LOCALHOST)),
        Err(PodEndpointError::EventSource { .. })
    ));
    assert!(matches!(
        stream.as_mut().poll_next(&mut context),
        Poll::Ready(Some(Ok(PodEndpointEvent::Resync(_))))
    ));

    let failed_source = ReferenceEventSource {
        fail: true,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        pending_then_ready(failed_source.subscribe()),
        Err(PodEndpointError::EventSource { .. })
    ));
}
