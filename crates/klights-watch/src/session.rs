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
    LeaderWatch, LeaderWatchError, LeaderWatchFuture, ResourceEvent, ResourceListScope,
    WatchEventType, WatchRequest, WatchResumeCursor, WatchStream,
};

use crate::{
    WatchSignalReceiveError, WatchSignalReceiver, WatchSignalSubscribe, WatchTopic,
    filter::ResourceFilter,
};

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

/// Reads the exact durable snapshot used by a converted/served-resource watch.
/// The conversion boundary supplies targets; snapshot and expiry semantics stay
/// with the positioned-watch owner.
pub struct SnapshotProjectedWatchBaseline {
    resources: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
}

impl SnapshotProjectedWatchBaseline {
    pub fn new(resources: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>) -> Self {
        Self { resources }
    }
}

impl ProjectedWatchBaselineRead for SnapshotProjectedWatchBaseline {
    fn read_baseline(
        &self,
        request: ProjectedWatchBaselineRequest,
    ) -> BoxFuture<'_, Result<ResourceListRead, LeaderWatchError>> {
        Box::pin(async move {
            match self
                .resources
                .snapshot_resources_at_position(
                    klights_cluster_store::ResourceSnapshotAtPositionRequest::try_new(
                        request.targets().to_vec(),
                        request.label_selector().map(str::to_owned),
                        None,
                        request.position(),
                    )
                    .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?,
                )
                .await
                .map_err(|error| LeaderWatchError::unavailable(format!("{error:?}")))?
            {
                klights_cluster_store::ResourceSnapshotRead::Historical(list) => {
                    let snapshot = list.snapshot();
                    let page = klights_cluster_store::ResourceListPage::try_new(
                        list.into_items(),
                        snapshot,
                        None,
                        None,
                    )
                    .map_err(|error| LeaderWatchError::malformed_event(error.to_string()))?;
                    Ok(ResourceListRead::Historical(page))
                }
                klights_cluster_store::ResourceSnapshotRead::Expired => {
                    Ok(ResourceListRead::Expired {
                        requested: request.position().resource_version,
                        oldest_available: request.position().resource_version.saturating_add(1),
                        replacement: None,
                    })
                }
                klights_cluster_store::ResourceSnapshotRead::Current => {
                    Err(LeaderWatchError::malformed_event(
                        "projected positioned baseline returned an unpinned Current sentinel",
                    ))
                }
            }
        })
    }
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
    baseline: Arc<dyn ProjectedWatchBaselineRead>,
    projection: Arc<dyn WatchResourceProjection>,
}

impl ProjectedWatchPlan {
    pub fn try_new(
        request: WatchRequest,
        targets: Vec<DurableWatchTarget>,
        topics: Vec<WatchTopic>,
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
}

impl PositionedWatchService {
    pub fn new(
        resources: Arc<dyn ClusterResourceRead>,
        history: Arc<dyn DurableWatchHistoryRead>,
        allocator: Arc<dyn DurableAllocatorRead>,
        signals: Arc<dyn WatchSignalSubscribe>,
    ) -> Self {
        Self {
            resources,
            history,
            allocator,
            signals,
        }
    }

    async fn open(&self, request: WatchRequest) -> Result<WatchStream, LeaderWatchError> {
        let topic = WatchTopic::new(request.api_version(), request.kind());
        // This synchronous subscription is intentionally first. Every await
        // after it is covered by durable replay from the chosen cursor.
        let signal_rx = self.signals.subscribe(topic.clone());
        let target = durable_target(&request);
        let replay_position = match request.start_watch_replay_position() {
            Some(position) => position,
            None => {
                let anchor = self
                    .allocator
                    .read_allocator_state()
                    .await
                    .map_err(map_allocator_error)?
                    .position();
                replay_position_at_anchor(&request, anchor)
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
            let scope = match request.scope() {
                ResourceListScope::Cluster => ResourceCollectionScope::Cluster,
                ResourceListScope::AllNamespaces => ResourceCollectionScope::AllNamespaces,
                ResourceListScope::Namespace(namespace) => {
                    ResourceCollectionScope::Namespace(namespace.clone())
                }
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
        let accepted_cursor = WatchResumeCursor::try_new(
            Some(replay_position.resource_version),
            Some(replay_position),
        )?;
        Ok(WatchStream::positioned(Box::pin(stream), accepted_cursor))
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
                replay_position_at_anchor(&request, anchor)
            }
        };
        let filter = ResourceFilter::for_watch(&request)?;
        let mut membership = SelectorMembership::default();
        let delivery_scope = durable_target(&request).scope().clone();
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
                        &durable_target(&request),
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
        let accepted_cursor = WatchResumeCursor::try_new(
            Some(replay_position.resource_version),
            Some(replay_position),
        )?;
        Ok(WatchStream::positioned(Box::pin(stream), accepted_cursor))
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
        if requested.resource_version_filter_through_event_id == 0
            && resource.resource_version > requested.resource_version
        {
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

fn replay_position_at_anchor(
    request: &WatchRequest,
    anchor: WatchReplayPosition,
) -> WatchReplayPosition {
    request
        .start_resource_version()
        .map_or(anchor, |resource_version| {
            WatchReplayPosition::from_resource_version_through_event_id(
                resource_version,
                anchor.event_id,
            )
        })
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

fn durable_target(request: &WatchRequest) -> DurableWatchTarget {
    match request.scope() {
        ResourceListScope::Cluster => {
            DurableWatchTarget::cluster(request.api_version(), request.kind())
        }
        ResourceListScope::Namespace(namespace) => DurableWatchTarget::namespaced_in_namespace(
            request.api_version(),
            request.kind(),
            namespace,
        ),
        ResourceListScope::AllNamespaces => {
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

/// Selector membership state for a remote LIST-to-WATCH consumer.
///
/// The two-phase transition keeps membership unchanged until the consumer has
/// durably applied the synthesized event.
pub struct WatchSelectorMembership {
    filter: ResourceFilter,
    membership: SelectorMembership,
}

impl WatchSelectorMembership {
    pub fn try_new(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        Ok(Self {
            filter: ResourceFilter::for_watch(request)?,
            membership: SelectorMembership::default(),
        })
    }

    pub fn replace(&mut self, resources: &[Resource]) {
        self.membership.replace(resources);
    }

    pub fn len(&self) -> usize {
        self.membership.matched.len()
    }

    pub fn is_empty(&self) -> bool {
        self.membership.matched.is_empty()
    }

    pub fn prepare(
        &self,
        event: ResourceEvent,
    ) -> Result<PendingWatchSelectorTransition, LeaderWatchError> {
        let matches = self.filter.matches(event.resource());
        self.membership.prepare(event, matches)
    }

    pub fn commit(&mut self, pending: PendingWatchSelectorTransition) {
        self.membership.commit(pending.mutation);
    }
}

pub struct PendingWatchSelectorTransition {
    event: Option<ResourceEvent>,
    mutation: SelectorMembershipMutation,
}

impl PendingWatchSelectorTransition {
    pub fn event(&self) -> Option<&ResourceEvent> {
        self.event.as_ref()
    }
}

#[derive(Default)]
struct SelectorMembership {
    matched: HashMap<SelectorKey, Resource>,
}

enum SelectorMembershipMutation {
    None,
    Upsert(SelectorKey, Resource),
    Remove(SelectorKey),
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
        let pending = self.prepare(event, matches)?;
        let event = pending.event.clone();
        self.commit(pending.mutation);
        Ok(event)
    }

    fn prepare(
        &self,
        event: ResourceEvent,
        matches: bool,
    ) -> Result<PendingWatchSelectorTransition, LeaderWatchError> {
        if ResourceFilter::event_always_deliver(event.event_type()) {
            return Ok(PendingWatchSelectorTransition {
                event: Some(event),
                mutation: SelectorMembershipMutation::None,
            });
        }
        let key = SelectorKey::from_resource(event.resource());
        let prior = self.matched.get(&key).cloned();
        let was_member = prior.is_some();
        let position = event.resume_position();
        let event_type = event.event_type();
        let current = event.resource().clone();
        let (event, mutation) = match event_type {
            WatchEventType::Deleted => {
                let mutation = if was_member {
                    SelectorMembershipMutation::Remove(key)
                } else {
                    SelectorMembershipMutation::None
                };
                ((was_member || matches).then_some(event), mutation)
            }
            WatchEventType::Added | WatchEventType::Modified if matches => {
                let event = if was_member || event_type == WatchEventType::Added {
                    Some(event)
                } else {
                    Some(ResourceEvent::try_new(
                        WatchEventType::Added,
                        current.clone(),
                        position,
                    )?)
                };
                (event, SelectorMembershipMutation::Upsert(key, current))
            }
            WatchEventType::Added | WatchEventType::Modified if was_member => {
                let event = ResourceEvent::try_new(
                    WatchEventType::Deleted,
                    prior.expect("membership was checked"),
                    position,
                )?;
                (Some(event), SelectorMembershipMutation::Remove(key))
            }
            WatchEventType::Added | WatchEventType::Modified => {
                (None, SelectorMembershipMutation::None)
            }
            WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
        };
        Ok(PendingWatchSelectorTransition { event, mutation })
    }

    fn commit(&mut self, mutation: SelectorMembershipMutation) {
        match mutation {
            SelectorMembershipMutation::None => {}
            SelectorMembershipMutation::Upsert(key, resource) => {
                self.matched.insert(key, resource);
            }
            SelectorMembershipMutation::Remove(key) => {
                self.matched.remove(&key);
            }
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
