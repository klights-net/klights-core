use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::future::BoxFuture;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_cluster_store::{
    ClusterResourceRead, DurableAllocatorRead, DurableWatchHistoryRead, DurableWatchScope,
    DurableWatchTarget, MAX_WATCH_HISTORY_PAGE, ResourceCollectionScope, ResourceListQuery,
    ResourceListRead, ResourceListRequest, ResourceReadError, ResourceVersionMatch,
    WatchHistoryError, WatchHistoryRead, WatchHistoryRequest,
};
use klights_leader_api::{
    LeaderWatch, LeaderWatchError, LeaderWatchFuture, ResourceEvent, WatchEventType, WatchRequest,
    WatchStream,
};

use crate::{
    WatchSignalReceiveError, WatchSignalReceiver, WatchSignalSubscribe, WatchTopic,
    filter::ResourceFilter,
};

/// Kubernetes collection scope required to distinguish an all-namespaces watch
/// from a cluster-scoped resource when `metadata.namespace` is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchResourceScope {
    Cluster,
    Namespaced,
}

/// Focused metadata fact injected by the composition root. The watch kernel
/// does not import datastore schemas, discovery registries, or API handlers.
pub trait WatchScopeResolver: Send + Sync {
    fn resource_scope<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<WatchResourceScope, LeaderWatchError>>;
}

#[derive(Clone)]
pub struct ProjectedWatchBaselineRequest {
    targets: Vec<DurableWatchTarget>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    position: WatchReplayPosition,
}

impl ProjectedWatchBaselineRequest {
    pub fn new(
        targets: Vec<DurableWatchTarget>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        position: WatchReplayPosition,
    ) -> Self {
        Self {
            targets,
            label_selector,
            field_selector,
            position,
        }
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }

    pub const fn position(&self) -> WatchReplayPosition {
        self.position
    }
}

pub trait ProjectedWatchBaselineRead: Send + Sync {
    fn read_baseline(
        &self,
        request: ProjectedWatchBaselineRequest,
    ) -> BoxFuture<'_, Result<ResourceListRead, LeaderWatchError>>;
}

pub trait WatchResourceProjection: Send + Sync {
    fn project_resources(
        &self,
        resources: Vec<Resource>,
    ) -> BoxFuture<'_, Result<Vec<Resource>, LeaderWatchError>>;
}

pub struct ProjectedWatchPlan {
    request: WatchRequest,
    targets: Vec<DurableWatchTarget>,
    topics: Vec<WatchTopic>,
    resource_scope: WatchResourceScope,
    baseline: Arc<dyn ProjectedWatchBaselineRead>,
    projection: Arc<dyn WatchResourceProjection>,
}

impl ProjectedWatchPlan {
    pub fn try_new(
        request: WatchRequest,
        targets: Vec<DurableWatchTarget>,
        topics: Vec<WatchTopic>,
        resource_scope: WatchResourceScope,
        baseline: Arc<dyn ProjectedWatchBaselineRead>,
        projection: Arc<dyn WatchResourceProjection>,
    ) -> Result<Self, LeaderWatchError> {
        if targets.is_empty() || targets.len() != topics.len() {
            return Err(LeaderWatchError::invalid_request(
                "watch.targets",
                "projected watch requires one topic per non-empty durable target",
            ));
        }
        Ok(Self {
            request,
            targets,
            topics,
            resource_scope,
            baseline,
            projection,
        })
    }
}

/// Local positioned-watch capability composed only from focused read and
/// subscription ports.
#[derive(Clone)]
pub struct PositionedWatchService {
    resources: Arc<dyn ClusterResourceRead>,
    history: Arc<dyn DurableWatchHistoryRead>,
    allocator: Arc<dyn DurableAllocatorRead>,
    signals: Arc<dyn WatchSignalSubscribe>,
    scopes: Arc<dyn WatchScopeResolver>,
}

impl PositionedWatchService {
    pub fn new(
        resources: Arc<dyn ClusterResourceRead>,
        history: Arc<dyn DurableWatchHistoryRead>,
        allocator: Arc<dyn DurableAllocatorRead>,
        signals: Arc<dyn WatchSignalSubscribe>,
        scopes: Arc<dyn WatchScopeResolver>,
    ) -> Self {
        Self {
            resources,
            history,
            allocator,
            signals,
            scopes,
        }
    }

    async fn open(&self, request: WatchRequest) -> Result<WatchStream, LeaderWatchError> {
        let topic = WatchTopic::new(request.api_version(), request.kind());
        // This synchronous subscription is intentionally first. Every await
        // after it is covered by durable replay from the chosen cursor.
        let signal_rx = self.signals.subscribe(topic.clone());
        let resource_scope = self
            .scopes
            .resource_scope(request.api_version(), request.kind())
            .await?;
        let target = durable_target(&request, resource_scope);
        let replay_position = match request.start_watch_replay_position() {
            Some(position) => position,
            None => {
                let anchor = self
                    .allocator
                    .read_allocator_state()
                    .await
                    .map_err(map_allocator_error)?
                    .position();
                match request.start_resource_version() {
                    Some(resource_version) if resource_version > 0 => {
                        WatchReplayPosition::from_resource_version_through_event_id(
                            resource_version,
                            anchor.event_id,
                        )
                    }
                    _ => anchor,
                }
            }
        };
        let filter = ResourceFilter::for_watch(&request)?;
        let mut membership = SelectorMembership::default();
        if filter.has_selector() {
            let query = ResourceListQuery::try_new(
                request.label_selector().map(str::to_owned),
                request.field_selector().map(str::to_owned),
                None,
                None,
                ResourceVersionMatch::AtPosition(replay_position),
            )
            .map_err(map_resource_read_error)?;
            let scope = match (resource_scope, request.namespace()) {
                (WatchResourceScope::Cluster, _) => ResourceCollectionScope::Cluster,
                (WatchResourceScope::Namespaced, Some(namespace)) => {
                    ResourceCollectionScope::Namespace(namespace.to_string())
                }
                (WatchResourceScope::Namespaced, None) => ResourceCollectionScope::AllNamespaces,
            };
            let baseline = self
                .resources
                .list_resources(ResourceListRequest::new(
                    request.api_version(),
                    request.kind(),
                    scope,
                    query,
                ))
                .await
                .map_err(map_resource_read_error)?;
            match baseline {
                ResourceListRead::Historical(page) => {
                    validate_selector_baseline(&page, replay_position, &target, &filter)?;
                    membership.replace(page.items());
                }
                ResourceListRead::Current(_) => {
                    return Err(LeaderWatchError::malformed_event(
                        "positioned selector baseline returned an unpinned Current result",
                    ));
                }
                ResourceListRead::Expired { requested, .. } => {
                    return Err(LeaderWatchError::ReplayExpired {
                        accepted_resource_version: requested,
                    });
                }
            }
        }
        let cursor = PositionedCursor {
            request,
            topics: vec![topic],
            targets: vec![target.clone()],
            delivery_scope: target.scope().clone(),
            signal_rx,
            history: self.history.clone(),
            replay_position,
            pending: VecDeque::new(),
            replay_needed: true,
            filter,
            membership,
            projection: None,
        };
        let stream = async_stream::stream! {
            let mut cursor = cursor;
            loop {
                match cursor.next_event().await {
                    Ok(event) => yield Ok(event),
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    pub fn watch_projected_resources(&self, plan: ProjectedWatchPlan) -> LeaderWatchFuture<'_> {
        Box::pin(self.open_projected(plan))
    }

    async fn open_projected(
        &self,
        plan: ProjectedWatchPlan,
    ) -> Result<WatchStream, LeaderWatchError> {
        let ProjectedWatchPlan {
            request,
            targets,
            topics,
            resource_scope,
            baseline,
            projection,
        } = plan;
        let signal_rx = WatchSignalReceiver::new(
            topics
                .iter()
                .cloned()
                .map(|topic| self.signals.subscribe(topic))
                .collect(),
        );
        let replay_position = match request.start_watch_replay_position() {
            Some(position) => position,
            None => {
                let anchor = self
                    .allocator
                    .read_allocator_state()
                    .await
                    .map_err(map_allocator_error)?
                    .position();
                match request.start_resource_version() {
                    Some(resource_version) if resource_version > 0 => {
                        WatchReplayPosition::from_resource_version_through_event_id(
                            resource_version,
                            anchor.event_id,
                        )
                    }
                    _ => anchor,
                }
            }
        };
        let filter = ResourceFilter::for_watch(&request)?;
        let mut membership = SelectorMembership::default();
        let delivery_scope = durable_target(&request, resource_scope).scope().clone();
        if filter.has_selector() {
            let read = baseline
                .read_baseline(ProjectedWatchBaselineRequest::new(
                    targets.clone(),
                    request.label_selector().map(str::to_owned),
                    request.field_selector().map(str::to_owned),
                    replay_position,
                ))
                .await?;
            match read {
                ResourceListRead::Historical(page) => {
                    if page.continuation().is_some()
                        || page.remaining_item_count().is_some_and(|count| count > 0)
                    {
                        return Err(LeaderWatchError::malformed_event(
                            "projected watch baseline must be complete and non-paginated",
                        ));
                    }
                    let snapshot = page.snapshot();
                    let mut projected = projection.project_resources(page.into_items()).await?;
                    if projected.iter().any(|resource| {
                        !filter.matches_identity(resource)
                            || !resource_matches_scope(resource, &delivery_scope)
                    }) {
                        return Err(LeaderWatchError::mismatched_event(
                            "projected watch baseline contains a resource outside its delivery scope",
                        ));
                    }
                    projected.retain(|resource| filter.matches(resource));
                    let projected_page = klights_cluster_store::ResourceListPage::try_new(
                        projected, snapshot, None, None,
                    )
                    .map_err(map_resource_read_error)?;
                    validate_selector_baseline(
                        &projected_page,
                        replay_position,
                        &durable_target(&request, resource_scope),
                        &filter,
                    )?;
                    membership.replace(projected_page.items());
                }
                ResourceListRead::Current(_) => {
                    return Err(LeaderWatchError::malformed_event(
                        "projected positioned baseline returned an unpinned Current result",
                    ));
                }
                ResourceListRead::Expired { requested, .. } => {
                    return Err(LeaderWatchError::ReplayExpired {
                        accepted_resource_version: requested,
                    });
                }
            }
        }
        let cursor = PositionedCursor {
            request,
            topics,
            targets,
            delivery_scope,
            signal_rx,
            history: self.history.clone(),
            replay_position,
            pending: VecDeque::new(),
            replay_needed: true,
            filter,
            membership,
            projection: Some(projection),
        };
        let stream = async_stream::stream! {
            let mut cursor = cursor;
            loop {
                match cursor.next_event().await {
                    Ok(event) => yield Ok(event),
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

fn validate_selector_baseline(
    page: &klights_cluster_store::ResourceListPage,
    requested: WatchReplayPosition,
    target: &DurableWatchTarget,
    filter: &ResourceFilter,
) -> Result<(), LeaderWatchError> {
    if page.snapshot().position() != requested {
        return Err(LeaderWatchError::malformed_event(format!(
            "selector baseline snapshot {:?} does not equal requested position {requested:?}",
            page.snapshot().position(),
        )));
    }
    if page.continuation().is_some() || page.remaining_item_count().is_some_and(|count| count > 0) {
        return Err(LeaderWatchError::malformed_event(
            "positioned selector baseline must be complete and non-paginated",
        ));
    }
    let mut identities = HashSet::with_capacity(page.items().len());
    for resource in page.items() {
        let identity = (
            resource.api_version.as_str(),
            resource.kind.as_str(),
            resource.namespace.as_deref(),
            resource.name.as_str(),
        );
        if !identities.insert(identity) {
            return Err(LeaderWatchError::malformed_event(
                "positioned selector baseline contains duplicate resource identities",
            ));
        }
        if resource.resource_version > requested.resource_version {
            return Err(LeaderWatchError::malformed_event(
                "positioned selector baseline contains a body newer than its snapshot",
            ));
        }
        if !filter.matches(resource) || !resource_matches_scope(resource, target.scope()) {
            return Err(LeaderWatchError::mismatched_event(
                "positioned selector baseline contains a resource outside its scope or selector",
            ));
        }
    }
    Ok(())
}

fn resource_matches_scope(resource: &Resource, scope: &DurableWatchScope) -> bool {
    match scope {
        DurableWatchScope::Cluster => resource.namespace.is_none(),
        DurableWatchScope::Namespaced(None) => resource.namespace.is_some(),
        DurableWatchScope::Namespaced(Some(expected)) => {
            resource.namespace.as_deref() == Some(expected.as_str())
        }
    }
}

impl LeaderWatch for PositionedWatchService {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        Box::pin(self.open(request))
    }
}

fn durable_target(request: &WatchRequest, scope: WatchResourceScope) -> DurableWatchTarget {
    match (scope, request.namespace()) {
        (WatchResourceScope::Cluster, _) => {
            DurableWatchTarget::cluster(request.api_version(), request.kind())
        }
        (WatchResourceScope::Namespaced, Some(namespace)) => {
            DurableWatchTarget::namespaced_in_namespace(
                request.api_version(),
                request.kind(),
                namespace,
            )
        }
        (WatchResourceScope::Namespaced, None) => {
            DurableWatchTarget::namespaced(request.api_version(), request.kind())
        }
    }
}

struct PositionedCursor {
    request: WatchRequest,
    topics: Vec<WatchTopic>,
    targets: Vec<DurableWatchTarget>,
    delivery_scope: DurableWatchScope,
    signal_rx: WatchSignalReceiver,
    history: Arc<dyn DurableWatchHistoryRead>,
    replay_position: WatchReplayPosition,
    pending: VecDeque<ResourceEvent>,
    replay_needed: bool,
    filter: ResourceFilter,
    membership: SelectorMembership,
    projection: Option<Arc<dyn WatchResourceProjection>>,
}

impl PositionedCursor {
    async fn next_event(&mut self) -> Result<ResourceEvent, LeaderWatchError> {
        loop {
            while let Some(event) = self.pending.pop_front() {
                if !self.filter.has_selector() {
                    return Ok(event);
                }
                let matches = self.filter.matches(event.resource());
                if let Some(event) = self.membership.transition(event, matches)? {
                    return Ok(event);
                }
            }
            if self.replay_needed {
                self.replay_once().await?;
                continue;
            }
            match self.signal_rx.recv().await {
                Ok(signal) if self.signal_matches(&signal) => {
                    // Set before awaiting durable I/O so cancellation of this
                    // pull cannot consume the only recovery obligation.
                    self.replay_needed = true;
                }
                Ok(_) => {}
                Err(WatchSignalReceiveError::Lagged(_)) => {
                    self.replay_needed = true;
                }
                Err(WatchSignalReceiveError::Closed) => {
                    return Err(LeaderWatchError::unavailable(
                        "local watch signal channel closed",
                    ));
                }
            }
        }
    }

    async fn replay_once(&mut self) -> Result<(), LeaderWatchError> {
        let limit = NonZeroUsize::new(MAX_WATCH_HISTORY_PAGE)
            .expect("cluster-store watch page maximum is non-zero");
        let requested_position = self.replay_position;
        let read = self
            .history
            .replay_watch_history(
                WatchHistoryRequest::new(self.targets.clone(), requested_position, limit.get())
                    .map_err(map_history_error)?,
            )
            .await
            .map_err(map_history_error)?;
        match read {
            WatchHistoryRead::Expired => Err(LeaderWatchError::ReplayExpired {
                accepted_resource_version: requested_position.resource_version,
            }),
            WatchHistoryRead::Events(page) => {
                page.validate_after(requested_position)
                    .map_err(map_history_error)?;
                let next_position = page.next_position();
                let mut decoded = VecDeque::with_capacity(page.events().len());
                let mut delivered_position = requested_position;
                for positioned in page.into_events() {
                    delivered_position = delivered_position
                        .advance_through_event(positioned.position)
                        .map_err(LeaderWatchError::malformed_event)?;
                    let event_type = positioned.event.event_type().to_string();
                    let mut projected = if let Some(projection) = &self.projection {
                        projection
                            .project_resources(vec![positioned.event.into_resource()])
                            .await?
                    } else {
                        vec![positioned.event.into_resource()]
                    };
                    if projected.len() != 1 {
                        return Err(LeaderWatchError::malformed_event(
                            "watch event projection must return exactly one resource",
                        ));
                    }
                    let event = ResourceEvent::try_from_wire_type(
                        &event_type,
                        projected.pop().expect("projection length checked"),
                        Some(delivered_position),
                    )?;
                    event.validate_for(&self.request)?;
                    validate_event_scope(&event, &self.delivery_scope)?;
                    decoded.push_back(event);
                }
                let delivered = !decoded.is_empty();
                self.replay_position = next_position;
                self.pending = decoded;
                // A non-empty page requires another bounded read. Only an
                // empty page is an authoritative transition back to signals.
                self.replay_needed = delivered;
                Ok(())
            }
        }
    }

    fn signal_matches(&self, signal: &crate::WatchSignal) -> bool {
        self.topics
            .iter()
            .position(|topic| *topic == signal.topic)
            .is_some_and(|index| {
                signal.advances.iter().any(|advance| {
                    namespace_matches(&self.targets[index], advance.namespace.as_deref())
                        && advance.high_rv > 0
                })
            })
    }
}

fn validate_event_scope(
    event: &ResourceEvent,
    scope: &DurableWatchScope,
) -> Result<(), LeaderWatchError> {
    if matches!(
        event.event_type(),
        WatchEventType::Bookmark | WatchEventType::Error
    ) {
        return Ok(());
    }
    let namespace = event.resource().namespace.as_deref();
    let matches = resource_matches_scope(event.resource(), scope);
    if !matches {
        return Err(LeaderWatchError::mismatched_event(format!(
            "watch event namespace {namespace:?} does not match target scope {scope:?}"
        )));
    }
    Ok(())
}

#[derive(Default)]
struct SelectorMembership {
    matched: HashMap<SelectorKey, Resource>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SelectorKey {
    namespace: Option<String>,
    name: String,
}

impl SelectorKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

impl SelectorMembership {
    fn replace(&mut self, resources: &[Resource]) {
        self.matched.clear();
        self.matched.extend(
            resources
                .iter()
                .cloned()
                .map(|resource| (SelectorKey::from_resource(&resource), resource)),
        );
    }

    fn transition(
        &mut self,
        event: ResourceEvent,
        matches: bool,
    ) -> Result<Option<ResourceEvent>, LeaderWatchError> {
        if ResourceFilter::event_always_deliver(event.event_type()) {
            return Ok(Some(event));
        }
        let key = SelectorKey::from_resource(event.resource());
        let prior = self.matched.get(&key).cloned();
        let was_member = prior.is_some();
        let position = event.resume_position();
        let event_type = event.event_type();
        let current = event.resource().clone();
        match event_type {
            WatchEventType::Deleted => {
                if was_member {
                    self.matched.remove(&key);
                }
                Ok((was_member || matches).then_some(event))
            }
            WatchEventType::Added | WatchEventType::Modified if matches => {
                self.matched.insert(key, current.clone());
                if was_member || event_type == WatchEventType::Added {
                    Ok(Some(event))
                } else {
                    ResourceEvent::try_new(WatchEventType::Added, current, position).map(Some)
                }
            }
            WatchEventType::Added | WatchEventType::Modified if was_member => {
                self.matched.remove(&key);
                ResourceEvent::try_new(
                    WatchEventType::Deleted,
                    prior.expect("membership was checked"),
                    position,
                )
                .map(Some)
            }
            WatchEventType::Added | WatchEventType::Modified => Ok(None),
            WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
        }
    }
}

fn namespace_matches(target: &DurableWatchTarget, namespace: Option<&str>) -> bool {
    match target.scope() {
        DurableWatchScope::Cluster => namespace.is_none(),
        DurableWatchScope::Namespaced(None) => namespace.is_some(),
        DurableWatchScope::Namespaced(Some(expected)) => namespace == Some(expected.as_str()),
    }
}

fn map_allocator_error(error: klights_cluster_store::AllocatorStateError) -> LeaderWatchError {
    match error {
        klights_cluster_store::AllocatorStateError::Timeout => LeaderWatchError::Timeout,
        klights_cluster_store::AllocatorStateError::Cancelled => LeaderWatchError::Cancelled,
        other => LeaderWatchError::unavailable(other.to_string()),
    }
}

fn map_history_error(error: WatchHistoryError) -> LeaderWatchError {
    match error {
        WatchHistoryError::Expired { requested } => LeaderWatchError::ReplayExpired {
            accepted_resource_version: requested.resource_version,
        },
        WatchHistoryError::Timeout => LeaderWatchError::Timeout,
        WatchHistoryError::Cancelled => LeaderWatchError::Cancelled,
        WatchHistoryError::CorruptData { message } => LeaderWatchError::malformed_event(message),
        other => LeaderWatchError::unavailable(other.to_string()),
    }
}

fn map_resource_read_error(error: ResourceReadError) -> LeaderWatchError {
    match error {
        ResourceReadError::Expired { requested, .. } => LeaderWatchError::ReplayExpired {
            accepted_resource_version: requested,
        },
        ResourceReadError::InvalidSelector { message } => {
            LeaderWatchError::invalid_request("watch.selector", message)
        }
        ResourceReadError::Timeout => LeaderWatchError::Timeout,
        ResourceReadError::Cancelled => LeaderWatchError::Cancelled,
        ResourceReadError::CorruptData { message } => LeaderWatchError::malformed_event(message),
        other => LeaderWatchError::unavailable(other.to_string()),
    }
}
