//! Datastore types shared across the trait surface and every backend.
//!
//! Anything used in a `DatastoreBackend` method signature lives here so the
//! trait module stays SQL-free and a future backend implementor can build
//! against `crate::datastore::*` without pulling in SQLite-specific code.

#[cfg(any(test, feature = "pod-repository-test-support"))]
use bytes::Bytes;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use serde::Serialize;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use serde_json::Value;

#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_cluster_core::Resource;

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Serialize)]
struct TestWatchEnvelope<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    object: &'a Value,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) fn with_staged_test_resource_event(
    staged: klights_cluster_store::StagedPostCommit,
    event_type: &str,
    name: &str,
    data: std::sync::Arc<Value>,
) -> klights_cluster_store::StagedPostCommit {
    let data = hydrate_staged_test_resource(
        std::sync::Arc::unwrap_or_clone(data),
        staged.api_version(),
        staged.kind(),
        staged.namespace(),
        name,
        staged.resource_version(),
    );
    let is_envelope = data
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
        && data.get("object").is_some();
    let resource_data = if is_envelope {
        data.get("object")
            .expect("checked staged watch envelope object")
            .clone()
    } else {
        data.clone()
    };
    let resource = Resource::try_from_data(std::sync::Arc::new(resource_data))
        .expect("staged test resource has canonical identity");
    let encoded_json = if is_envelope {
        serde_json::to_vec(&data)
    } else {
        serde_json::to_vec(&TestWatchEnvelope {
            event_type,
            object: resource.data.as_ref(),
        })
    }
    .ok()
    .map(Bytes::from);
    staged.with_test_event(event_type, resource, encoded_json)
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
fn hydrate_staged_test_resource(
    mut data: Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> Value {
    if data
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|event_type| matches!(event_type, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
        && let Some(object) = data.get_mut("object")
    {
        *object = hydrate_staged_test_resource(
            std::mem::take(object),
            api_version,
            kind,
            namespace,
            name,
            resource_version,
        );
        return data;
    }
    if let Some(object) = data.as_object_mut() {
        object.insert("apiVersion".to_string(), Value::from(api_version));
        object.insert("kind".to_string(), Value::from(kind));
        let metadata = object
            .entry("metadata")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Some(metadata) = metadata.as_object_mut() {
            metadata.insert("name".to_string(), Value::from(name));
            if let Some(namespace) = namespace {
                metadata.insert("namespace".to_string(), Value::from(namespace));
            }
            metadata.insert(
                "resourceVersion".to_string(),
                Value::from(resource_version.to_string()),
            );
        }
    }
    data
}

pub const POD_CLEANUP_REASON_NODE_LOST: &str = "NodeLost";

pub use klights_cluster_store::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    PositionedWatchReplay, PositionedWatchReplayRead, ReplicatedMembershipState,
    ReplicatedSnapshotMetadata, ResourceList, ResourceListOptions as ResourceListQuery,
    SnapshotAtRv, WatchReplayFloor, WatchReplayRead, WatchTarget, WatchTargetScope,
};
