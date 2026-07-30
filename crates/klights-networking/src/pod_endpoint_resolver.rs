//! `PodEndpointResolver` — cross-mode pod reachability lookup.
//!
//! Cross-mode pod reachability is mediated by a `PodEndpointResolver`. The
//! store-backed implementation is shared by dataplane consumers through
//! focused node-store and leader-query ports.

use futures::stream::{BoxStream, StreamExt};
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use klights_leader_api::LeaderNetworkTopologyQuery;
use klights_network_api::{
    DirectPodEndpoint, HostPortPodEndpoint, PodEndpoint, PodEndpointError, PodEndpointEventSource,
    PodEndpointEventStream, PodEndpointEventSubscription, PodEndpointFuture, PodEndpointResolver,
    PodEndpointTopology,
};
use klights_node_store::{
    PodEndpointMode, PodEndpointRecord, PodEndpointStore, PodEndpointStoreEvent,
    PodEndpointStoreEventSource,
};

/// Focused store-backed resolver for persisted pod endpoint topology.
///
/// The adapter has no datastore implementation or backend-selection knowledge:
/// root composition injects the endpoint store, its event source, and the
/// leader topology query.
pub struct StorePodEndpointResolver {
    endpoints: Arc<dyn PodEndpointStore>,
    endpoint_events: Arc<dyn PodEndpointStoreEventSource>,
    topology: Arc<dyn LeaderNetworkTopologyQuery>,
}

impl StorePodEndpointResolver {
    pub fn new(
        endpoints: Arc<dyn PodEndpointStore>,
        endpoint_events: Arc<dyn PodEndpointStoreEventSource>,
        topology: Arc<dyn LeaderNetworkTopologyQuery>,
    ) -> Self {
        Self {
            endpoints,
            endpoint_events,
            topology,
        }
    }
}

impl PodEndpointResolver for StorePodEndpointResolver {
    fn resolve(&self, pod_ip: Ipv4Addr) -> PodEndpointFuture<'_, Option<PodEndpoint>> {
        Box::pin(async move {
            let Some(row) = self
                .endpoints
                .get_endpoint_by_pod_ip(pod_ip)
                .await
                .map_err(|error| PodEndpointError::resolve(error.to_string()))?
            else {
                return Ok(None);
            };
            match row.mode() {
                PodEndpointMode::EncryptedDirect => {
                    let query = klights_leader_api::NodeDataplaneQuery::try_new(row.node_name())
                        .map_err(|error| PodEndpointError::resolve(error.to_string()))?;
                    let Some(metadata) = self
                        .topology
                        .get_node_dataplane(query)
                        .await
                        .map_err(|error| PodEndpointError::resolve(error.to_string()))?
                        .into_option()
                    else {
                        return Ok(None);
                    };
                    let endpoint =
                        DirectPodEndpoint::try_new(row.pod_ip(), row.node_name().to_string())?;
                    Ok(Some(match metadata.encryption() {
                        klights_leader_api::DataplaneEncryption::WireGuard => {
                            PodEndpoint::EncryptedDirect(endpoint)
                        }
                        klights_leader_api::DataplaneEncryption::Direct => {
                            PodEndpoint::UnencryptedDirect(endpoint)
                        }
                    }))
                }
                PodEndpointMode::Hostport => {
                    if row.host_port_tcp().is_none() && row.host_port_udp().is_none() {
                        return Ok(None);
                    }
                    Ok(Some(PodEndpoint::HostPort(HostPortPodEndpoint::try_new(
                        row.pod_ip(),
                        row.node_name().to_string(),
                        row.node_ip(),
                        row.host_port_tcp(),
                        row.host_port_udp(),
                    )?)))
                }
            }
        })
    }
}

impl PodEndpointEventSource for StorePodEndpointResolver {
    fn subscribe(&self) -> PodEndpointFuture<'_, PodEndpointEventStream> {
        Box::pin(async move {
            let mut events = self
                .endpoint_events
                .subscribe_endpoint_events()
                .await
                .map_err(|error| PodEndpointError::event_source(error.to_string()))?;
            let inner = futures::stream::poll_fn(move |context| {
                let event = events.as_mut().poll_next(context);
                event.map(|item| {
                    item.map(|result| {
                        result
                            .map_err(|error| PodEndpointError::event_source(error.to_string()))
                            .and_then(translate_endpoint_event)
                    })
                })
            })
            .boxed();
            Ok(Box::pin(ResolverEndpointSubscription { inner }) as PodEndpointEventStream)
        })
    }
}

struct ResolverEndpointSubscription {
    inner: BoxStream<
        'static,
        Result<klights_network_api::PodEndpointEvent, klights_network_api::PodEndpointError>,
    >,
}

impl PodEndpointEventSubscription for ResolverEndpointSubscription {
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<
        Option<
            Result<klights_network_api::PodEndpointEvent, klights_network_api::PodEndpointError>,
        >,
    > {
        self.get_mut().inner.as_mut().poll_next(context)
    }
}

fn translate_endpoint_snapshot(
    rows: Vec<PodEndpointRecord>,
) -> Result<Vec<PodEndpointTopology>, PodEndpointError> {
    rows.into_iter().map(translate_endpoint_row).collect()
}

fn translate_endpoint_row(row: PodEndpointRecord) -> Result<PodEndpointTopology, PodEndpointError> {
    match row.mode() {
        PodEndpointMode::EncryptedDirect => Ok(PodEndpointTopology::Direct(
            DirectPodEndpoint::try_new(row.pod_ip(), row.node_name().to_string())?,
        )),
        PodEndpointMode::Hostport => {
            Ok(PodEndpointTopology::HostPort(HostPortPodEndpoint::try_new(
                row.pod_ip(),
                row.node_name().to_string(),
                row.node_ip(),
                row.host_port_tcp(),
                row.host_port_udp(),
            )?))
        }
    }
}

fn translate_endpoint_event(
    event: PodEndpointStoreEvent,
) -> Result<klights_network_api::PodEndpointEvent, PodEndpointError> {
    match event {
        PodEndpointStoreEvent::Resync(rows) => Ok(klights_network_api::PodEndpointEvent::Resync(
            translate_endpoint_snapshot(rows)?,
        )),
        PodEndpointStoreEvent::Upsert(row) => Ok(klights_network_api::PodEndpointEvent::Upsert(
            translate_endpoint_row(row)?,
        )),
        PodEndpointStoreEvent::Delete { pod_ip } => {
            Ok(klights_network_api::PodEndpointEvent::Delete(pod_ip))
        }
    }
}
