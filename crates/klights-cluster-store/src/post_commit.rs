//! Neutral values staged by persistence for root-owned post-commit delivery.

use std::any::Any;

#[cfg(feature = "test-support")]
use bytes::Bytes;
#[cfg(feature = "test-support")]
use klights_cluster_core::Resource;

/// Durable resource mutation metadata emitted only after its transaction
/// commits. Root composition projects this value into active watch delivery.
#[derive(Clone, Debug)]
pub struct StagedPostCommit {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    resource_version: i64,
    #[cfg(feature = "test-support")]
    test_event: Option<StagedResourceEvent>,
}

/// Synchronous, nonblocking post-commit observation port.
///
/// Persistence stages neutral facts and invokes this injected sink only after
/// commit. Root composition owns active watch and reconciliation delivery.
pub trait CommitObservationSink: Send + Sync {
    fn observe(&self, observations: &[StagedPostCommit]);
    fn as_any(&self) -> &dyn Any;
}

impl StagedPostCommit {
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        resource_version: i64,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(str::to_string),
            resource_version,
            #[cfg(feature = "test-support")]
            test_event: None,
        }
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    #[cfg(feature = "test-support")]
    pub fn with_test_event(
        mut self,
        event_type: impl Into<String>,
        resource: Resource,
        encoded_json: Option<Bytes>,
    ) -> Self {
        self.test_event = Some(StagedResourceEvent {
            event_type: event_type.into(),
            resource,
            encoded_json,
        });
        self
    }

    #[cfg(feature = "test-support")]
    pub const fn test_event(&self) -> Option<&StagedResourceEvent> {
        self.test_event.as_ref()
    }
}

/// Test-only neutral resource payload used to preserve full watch-event
/// assertions without coupling persistence to the active watch package.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug)]
pub struct StagedResourceEvent {
    event_type: String,
    resource: Resource,
    encoded_json: Option<Bytes>,
}

#[cfg(feature = "test-support")]
impl StagedResourceEvent {
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub const fn resource(&self) -> &Resource {
        &self.resource
    }

    pub const fn encoded_json(&self) -> Option<&Bytes> {
        self.encoded_json.as_ref()
    }
}
