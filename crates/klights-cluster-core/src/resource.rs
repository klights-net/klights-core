//! Backend- and transport-neutral Kubernetes resource values.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use klights_types::ResourceKey;

/// Missing identity fields rejected at resource normalization boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceIdentityError {
    ResourceMissingApiVersion,
    ResourceMissingKind,
    ResourceMissingMetadataName,
    WatchEventMissingApiVersion,
    WatchEventMissingKind,
    WatchEventMissingMetadataName,
    MetadataUidRequired,
}

impl fmt::Display for ResourceIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResourceMissingApiVersion => "resource missing apiVersion",
            Self::ResourceMissingKind => "resource missing kind",
            Self::ResourceMissingMetadataName => "resource missing metadata.name",
            Self::WatchEventMissingApiVersion => "watch event missing apiVersion",
            Self::WatchEventMissingKind => "watch event missing kind",
            Self::WatchEventMissingMetadataName => "watch event missing metadata.name",
            Self::MetadataUidRequired => "metadata.uid is required for UID-qualified write",
        })
    }
}

impl std::error::Error for ResourceIdentityError {}

/// Canonical stored Kubernetes resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub id: i64,
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    /// Identity-stable guard for delete/recreate name slots.
    pub uid: String,
    pub resource_version: i64,
    /// Shared JSON body; mutation uses `Arc::make_mut` for copy-on-write.
    pub data: Arc<Value>,
}

/// Neutral view implemented by adapters that carry a watch object.
pub trait ResourceEventObject {
    fn resource_object(&self) -> &Arc<Value>;
}

impl Resource {
    /// Normalize a complete Kubernetes JSON object, rejecting missing identity.
    pub fn try_from_data(data: Arc<Value>) -> Result<Self, ResourceIdentityError> {
        let api_version = data
            .get("apiVersion")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::ResourceMissingApiVersion)?
            .to_string();
        let kind = data
            .get("kind")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::ResourceMissingKind)?
            .to_string();
        let name = data
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::ResourceMissingMetadataName)?
            .to_string();
        Ok(Self::from_normalized_parts(data, api_version, kind, name))
    }

    pub fn from_watch_event(event: impl ResourceEventObject) -> Self {
        Self::from_data_lossy(event.resource_object().clone())
    }

    pub fn from_watch_event_ref(event: &(impl ResourceEventObject + ?Sized)) -> Self {
        Self::from_data_lossy(event.resource_object().clone())
    }

    pub fn try_from_watch_event(
        event: &(impl ResourceEventObject + ?Sized),
    ) -> Result<Self, ResourceIdentityError> {
        let data = event.resource_object();
        data.get("apiVersion")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::WatchEventMissingApiVersion)?;
        data.get("kind")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::WatchEventMissingKind)?;
        data.pointer("/metadata/name")
            .and_then(Value::as_str)
            .ok_or(ResourceIdentityError::WatchEventMissingMetadataName)?;
        Self::try_from_data(data.clone())
    }

    /// Normalize trusted historical data while preserving the prior empty-field
    /// fallback used by watch replay.
    pub fn from_data_lossy(data: Arc<Value>) -> Self {
        let api_version = data
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let kind = data
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = data
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Self::from_normalized_parts(data, api_version, kind, name)
    }

    fn from_normalized_parts(
        data: Arc<Value>,
        api_version: String,
        kind: String,
        name: String,
    ) -> Self {
        Self {
            id: 0,
            api_version,
            kind,
            namespace: data
                .pointer("/metadata/namespace")
                .and_then(Value::as_str)
                .filter(|namespace| !namespace.is_empty())
                .map(str::to_string),
            name,
            uid: Self::uid_from_data(&data),
            resource_version: resource_version_from_data(&data),
            data,
        }
    }

    pub fn uid_from_data(data: &Value) -> String {
        data.pointer("/metadata/uid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }

    pub fn key(&self) -> ResourceKey {
        ResourceKey::new(
            self.api_version.clone(),
            self.kind.clone(),
            self.namespace.clone(),
            self.name.clone(),
        )
    }
}

fn resource_version_from_data(data: &Value) -> i64 {
    data.pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

/// Optimistic write guards for one Kubernetes object identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePreconditions {
    pub uid: Option<String>,
    pub resource_version: Option<i64>,
}

impl ResourcePreconditions {
    pub fn resource_version(resource_version: i64) -> Self {
        Self {
            uid: None,
            resource_version: Some(resource_version),
        }
    }

    pub fn uid(uid: impl Into<String>) -> Self {
        Self {
            uid: Some(uid.into()),
            resource_version: None,
        }
    }

    pub fn uid_and_resource_version(uid: impl Into<String>, resource_version: i64) -> Self {
        Self {
            uid: Some(uid.into()),
            resource_version: Some(resource_version),
        }
    }

    pub fn from_resource(resource: &Resource) -> Self {
        Self::uid_and_resource_version(resource.uid.clone(), resource.resource_version)
    }

    pub fn from_metadata(
        metadata: &Value,
        resource_version: i64,
    ) -> Result<Self, ResourceIdentityError> {
        let uid = metadata
            .get("uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.trim().is_empty())
            .ok_or(ResourceIdentityError::MetadataUidRequired)?;
        Ok(Self::uid_and_resource_version(uid, resource_version))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceBatchPutMode {
    Create,
    Update,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResourceBatchOperation {
    Put {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        data: Value,
        mode: ResourceBatchPutMode,
        preconditions: ResourcePreconditions,
    },
    Delete {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        preconditions: ResourcePreconditions,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PatchKind {
    Merge,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourcePatchRequest {
    pub patch_kind: PatchKind,
    pub patch: Value,
    pub preconditions: ResourcePreconditions,
    #[serde(default)]
    pub strict_resource_version: bool,
}

impl ResourcePatchRequest {
    pub fn new(patch_kind: PatchKind, patch: Value, preconditions: ResourcePreconditions) -> Self {
        Self {
            patch_kind,
            patch,
            preconditions,
            strict_resource_version: false,
        }
    }

    pub fn without_preconditions(patch_kind: PatchKind, patch: Value) -> Self {
        Self::new(patch_kind, patch, ResourcePreconditions::default())
    }

    pub fn with_strict_resource_version(mut self) -> Self {
        self.strict_resource_version = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "p".to_string(),
            uid: "uid-p".to_string(),
            resource_version: 1,
            data: Arc::new(json!({"spec": {"x": 1}, "status": {"y": 2}})),
        }
    }

    #[test]
    fn resource_normalization_is_strict_and_preserves_shared_data() {
        let data = Arc::new(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"namespace": "default", "name": "web", "uid": "u1", "resourceVersion": "42"}
        }));
        let resource = Resource::try_from_data(data.clone()).unwrap();
        assert_eq!(
            (resource.api_version.as_str(), resource.kind.as_str()),
            ("v1", "Pod")
        );
        assert_eq!(resource.namespace.as_deref(), Some("default"));
        assert_eq!(
            (resource.name.as_str(), resource.uid.as_str()),
            ("web", "u1")
        );
        assert_eq!(resource.resource_version, 42);
        assert!(Arc::ptr_eq(&resource.data, &data));
        assert_eq!(
            resource.key(),
            ResourceKey::new("v1", "Pod", Some("default".to_string()), "web")
        );
        assert_eq!(
            Resource::try_from_data(Arc::new(
                json!({"kind": "Pod", "metadata": {"name": "web"}})
            ))
            .unwrap_err()
            .to_string(),
            "resource missing apiVersion"
        );
    }

    #[test]
    fn empty_metadata_namespace_is_absent_from_identity_but_preserved_in_data() {
        let data = Arc::new(json!({
            "apiVersion": "example.com/v1",
            "kind": "ClusterThing",
            "metadata": {
                "name": "cluster-thing",
                "namespace": "",
                "resourceVersion": "42"
            }
        }));
        let resource = Resource::try_from_data(data.clone()).unwrap();

        assert_eq!(resource.namespace, None);
        assert_eq!(resource.data["metadata"]["namespace"], "");
        assert!(Arc::ptr_eq(&resource.data, &data));
    }

    #[test]
    fn resource_identity_errors_are_typed_and_preserve_display_contracts() {
        let cases = [
            (
                json!({"kind": "Pod", "metadata": {"name": "web"}}),
                ResourceIdentityError::ResourceMissingApiVersion,
                "resource missing apiVersion",
            ),
            (
                json!({"apiVersion": "v1", "metadata": {"name": "web"}}),
                ResourceIdentityError::ResourceMissingKind,
                "resource missing kind",
            ),
            (
                json!({"apiVersion": "v1", "kind": "Pod", "metadata": {}}),
                ResourceIdentityError::ResourceMissingMetadataName,
                "resource missing metadata.name",
            ),
        ];
        for (data, expected, display) in cases {
            let error = Resource::try_from_data(Arc::new(data)).unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(error.to_string(), display);
        }

        struct Event(Arc<Value>);
        impl ResourceEventObject for Event {
            fn resource_object(&self) -> &Arc<Value> {
                &self.0
            }
        }
        let watch_cases = [
            (
                json!({"kind": "Pod", "metadata": {"name": "web"}}),
                ResourceIdentityError::WatchEventMissingApiVersion,
                "watch event missing apiVersion",
            ),
            (
                json!({"apiVersion": "v1", "metadata": {"name": "web"}}),
                ResourceIdentityError::WatchEventMissingKind,
                "watch event missing kind",
            ),
            (
                json!({"apiVersion": "v1", "kind": "Pod", "metadata": {}}),
                ResourceIdentityError::WatchEventMissingMetadataName,
                "watch event missing metadata.name",
            ),
        ];
        for (data, expected, display) in watch_cases {
            let error = Resource::try_from_watch_event(&Event(Arc::new(data))).unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(error.to_string(), display);
        }

        let error = ResourcePreconditions::from_metadata(&json!({"uid": "  "}), 9).unwrap_err();
        assert_eq!(error, ResourceIdentityError::MetadataUidRequired);
        assert_eq!(
            error.to_string(),
            "metadata.uid is required for UID-qualified write"
        );
    }

    #[test]
    fn preconditions_and_patch_request_preserve_existing_defaults() {
        let preconditions = ResourcePreconditions::from_metadata(&json!({"uid": "u1"}), 9).unwrap();
        assert_eq!(
            preconditions,
            ResourcePreconditions::uid_and_resource_version("u1", 9)
        );
        assert!(
            !ResourcePatchRequest::without_preconditions(PatchKind::Merge, json!({}))
                .strict_resource_version
        );
        assert_eq!(
            ResourcePreconditions::from_metadata(&json!({"uid": "  "}), 9)
                .unwrap_err()
                .to_string(),
            "metadata.uid is required for UID-qualified write"
        );

        let operation = ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: json!({"data": {"key": "value"}}),
            mode: ResourceBatchPutMode::Update,
            preconditions,
        };
        assert_eq!(
            serde_json::from_str::<ResourceBatchOperation>(
                &serde_json::to_string(&operation).unwrap()
            )
            .unwrap(),
            operation
        );
    }

    #[test]
    fn cloning_resource_is_shallow_and_make_mut_is_copy_on_write() {
        let resource = sample();
        let mut clone = resource.clone();
        assert_eq!(Arc::strong_count(&resource.data), 2);
        assert!(Arc::ptr_eq(&resource.data, &clone.data));

        Arc::make_mut(&mut clone.data)
            .as_object_mut()
            .unwrap()
            .insert("forked".to_string(), json!(true));
        assert_eq!(Arc::strong_count(&resource.data), 1);
        assert_eq!(Arc::strong_count(&clone.data), 1);
        assert!(!Arc::ptr_eq(&resource.data, &clone.data));
        assert!(resource.data.get("forked").is_none());
        assert_eq!(clone.data.get("forked"), Some(&json!(true)));
    }
}
