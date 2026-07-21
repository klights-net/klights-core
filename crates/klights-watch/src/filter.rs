use klights_cluster_core::Resource;
use klights_leader_api::{
    CacheReadinessRequest, LeaderWatchError, ResourceListRequest, WatchEventType, WatchRequest,
};
use klights_types::{FieldSelector, LabelSelector};

use crate::WatchCacheError;

pub(crate) struct ResourceFilter {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<LabelSelector>,
    field_selector: Option<FieldSelector>,
}

impl ResourceFilter {
    pub(crate) fn for_watch(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        let label_selector = parse_label(request.label_selector()).map_err(|message| {
            LeaderWatchError::invalid_request("watch.label_selector", message)
        })?;
        Ok(Self {
            api_version: request.api_version().to_string(),
            kind: request.kind().to_string(),
            namespace: request.namespace().map(str::to_owned),
            label_selector,
            field_selector: parse_field(request.field_selector()).map_err(|message| {
                LeaderWatchError::invalid_request("watch.field_selector", message)
            })?,
        })
    }

    pub(crate) fn for_list(request: &ResourceListRequest) -> Result<Self, WatchCacheError> {
        let label_selector =
            parse_label(request.label_selector()).map_err(WatchCacheError::invalid_selector)?;
        Ok(Self {
            api_version: request.api_version().to_string(),
            kind: request.kind().to_string(),
            namespace: request.namespace().map(str::to_owned),
            label_selector,
            field_selector: parse_field(request.field_selector())
                .map_err(WatchCacheError::invalid_selector)?,
        })
    }

    pub(crate) fn for_cache_scope(
        request: &CacheReadinessRequest,
    ) -> Result<Self, WatchCacheError> {
        let label_selector =
            parse_label(request.label_selector()).map_err(WatchCacheError::invalid_selector)?;
        Ok(Self {
            api_version: request.api_version().to_string(),
            kind: request.kind().to_string(),
            namespace: request.namespace().map(str::to_owned),
            label_selector,
            field_selector: parse_field(request.field_selector())
                .map_err(WatchCacheError::invalid_selector)?,
        })
    }

    pub(crate) fn has_selector(&self) -> bool {
        self.label_selector.is_some() || self.field_selector.is_some()
    }

    pub(crate) fn matches(&self, resource: &Resource) -> bool {
        self.matches_identity(resource)
            && self
                .label_selector
                .as_ref()
                .is_none_or(|selector| selector.matches_resource(&resource.data))
            && self.field_selector.as_ref().is_none_or(|selector| {
                selector.matches_resource_with_identity(
                    &resource.api_version,
                    &resource.kind,
                    &resource.data,
                )
            })
    }

    pub(crate) fn matches_identity(&self, resource: &Resource) -> bool {
        resource.api_version == self.api_version
            && resource.kind == self.kind
            && self
                .namespace
                .as_deref()
                .is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
    }

    pub(crate) fn event_always_deliver(event_type: WatchEventType) -> bool {
        matches!(event_type, WatchEventType::Bookmark | WatchEventType::Error)
    }
}

fn parse_label(selector: Option<&str>) -> Result<Option<LabelSelector>, String> {
    let Some(selector) = normalized(selector) else {
        return Ok(None);
    };
    LabelSelector::parse(&selector)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn parse_field(selector: Option<&str>) -> Result<Option<FieldSelector>, String> {
    let Some(selector) = normalized(selector) else {
        return Ok(None);
    };
    FieldSelector::parse(&selector)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn normalized(selector: Option<&str>) -> Option<String> {
    selector
        .filter(|selector| !selector.trim().is_empty())
        .map(str::to_owned)
}
