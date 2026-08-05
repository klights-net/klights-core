use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryFuture,
};
use serde_json::Value;

use crate::volume_sources::VolumeSourceReader;

type ResourceKey = (String, String, Option<String>, String);

#[derive(Clone)]
pub(super) struct TestVolumeSources {
    inner: Arc<TestVolumeSourcesInner>,
}

struct TestVolumeSourcesInner {
    resources: Mutex<BTreeMap<ResourceKey, Resource>>,
    next_resource_version: AtomicI64,
}

impl TestVolumeSources {
    pub(super) async fn new_in_memory() -> Result<Self> {
        Ok(Self {
            inner: Arc::new(TestVolumeSourcesInner {
                resources: Mutex::new(BTreeMap::new()),
                next_resource_version: AtomicI64::new(1),
            }),
        })
    }

    fn allocate_resource_version(&self) -> i64 {
        self.inner
            .next_resource_version
            .fetch_add(1, Ordering::Relaxed)
    }

    fn canonical_namespace<'a>(kind: &str, namespace: Option<&'a str>) -> Option<&'a str> {
        match kind {
            "Namespace" | "Node" | "PersistentVolume" => None,
            _ => namespace,
        }
    }

    fn key(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> ResourceKey {
        (
            api_version.to_string(),
            kind.to_string(),
            namespace.map(str::to_string),
            name.to_string(),
        )
    }

    fn normalize(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
        resource_version: i64,
    ) -> Result<Resource> {
        data["apiVersion"] = Value::String(api_version.to_string());
        data["kind"] = Value::String(kind.to_string());
        data["metadata"]["name"] = Value::String(name.to_string());
        let namespace = Self::canonical_namespace(kind, namespace);
        if let Some(namespace) = namespace {
            data["metadata"]["namespace"] = Value::String(namespace.to_string());
        } else if let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) {
            metadata.remove("namespace");
        }
        let uid = data
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                let scope = namespace.unwrap_or("cluster");
                format!("test-{kind}-{scope}-{name}-uid")
            });
        data["metadata"]["uid"] = Value::String(uid);
        data["metadata"]["resourceVersion"] = Value::String(resource_version.to_string());
        let mut resource = Resource::try_from_data(Arc::new(data))?;
        resource.id = resource_version;
        resource.resource_version = resource_version;
        Ok(resource)
    }

    pub(super) async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        let namespace = Self::canonical_namespace(kind, namespace);
        let key = Self::key(api_version, kind, namespace, name);
        let mut resources = self.inner.resources.lock().unwrap();
        if resources.contains_key(&key) {
            return Err(anyhow!("Resource already exists"));
        }
        let resource = self.normalize(
            api_version,
            kind,
            namespace,
            name,
            data,
            self.allocate_resource_version(),
        )?;
        resources.insert(key, resource.clone());
        Ok(resource)
    }

    pub(super) async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let namespace = Self::canonical_namespace(kind, namespace);
        Ok(self
            .inner
            .resources
            .lock()
            .unwrap()
            .get(&Self::key(api_version, kind, namespace, name))
            .cloned())
    }

    pub(super) async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        let namespace = Self::canonical_namespace(kind, namespace);
        let key = Self::key(api_version, kind, namespace, name);
        let mut resources = self.inner.resources.lock().unwrap();
        let current = resources
            .get(&key)
            .ok_or_else(|| anyhow!("Resource not found"))?;
        if current.resource_version != expected_resource_version {
            return Err(anyhow!("resourceVersion conflict"));
        }
        let mut data = data;
        if data
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .is_none()
        {
            data["metadata"]["uid"] = Value::String(current.uid.clone());
        }
        let resource = self.normalize(
            api_version,
            kind,
            namespace,
            name,
            data,
            self.allocate_resource_version(),
        )?;
        resources.insert(key, resource.clone());
        Ok(resource)
    }

    pub(super) async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        let namespace = Self::canonical_namespace(kind, namespace);
        self.inner
            .resources
            .lock()
            .unwrap()
            .remove(&Self::key(api_version, kind, namespace, name));
        Ok(())
    }

    fn get(&self, kind: &str, namespace: Option<&str>, name: &str) -> Option<Resource> {
        self.inner
            .resources
            .lock()
            .unwrap()
            .get(&Self::key("v1", kind, namespace, name))
            .cloned()
    }
}

#[async_trait]
impl VolumeSourceReader for TestVolumeSources {
    async fn config_map(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("ConfigMap", Some(namespace), name))
    }
    async fn secret(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("Secret", Some(namespace), name))
    }
    async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("ServiceAccount", Some(namespace), name))
    }
    async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("Pod", Some(namespace), name))
    }
    async fn node(&self, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("Node", None, name))
    }
    async fn persistent_volume_claim(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        Ok(self.get("PersistentVolumeClaim", Some(namespace), name))
    }
    async fn persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        Ok(self.get("PersistentVolume", None, name))
    }
}

impl PodQuery for TestVolumeSources {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            Ok(self
                .get("Pod", Some(request.namespace()), request.name())
                .filter(|pod| request.uid().is_none_or(|uid| pod.uid == uid)))
        })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let resources = self.inner.resources.lock().unwrap();
            let items = resources
                .values()
                .filter(|resource| {
                    resource.api_version == "v1"
                        && resource.kind == "Pod"
                        && request.namespace().is_none_or(|namespace| {
                            resource.namespace.as_deref() == Some(namespace)
                        })
                })
                .cloned()
                .collect();
            PodListResult::try_new(
                items,
                self.inner.next_resource_version.load(Ordering::Relaxed) - 1,
                None,
                None,
            )
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            let resources = self.inner.resources.lock().unwrap();
            Ok(resources
                .values()
                .filter(|resource| {
                    resource.kind == "Pod"
                        && resource.namespace.as_deref() == Some(request.namespace())
                        && resource
                            .data
                            .pointer("/metadata/ownerReferences")
                            .and_then(Value::as_array)
                            .is_some_and(|owners| {
                                owners.iter().any(|owner| {
                                    owner.get("uid").and_then(Value::as_str)
                                        == Some(request.owner_uid())
                                })
                            })
                })
                .cloned()
                .collect())
        })
    }
}

pub(super) fn file_process_executor() -> klights_supervisor::FileProcessExecutor {
    crate::phase15d_test_support::file_process_executor()
}

#[tokio::test]
async fn test_volume_sources_resource_and_pod_query_contract() {
    use serde_json::json;

    let sources = TestVolumeSources::new_in_memory().await.unwrap();
    let mut created = Vec::new();
    for namespace in ["team-a", "team-b"] {
        let pod = sources
            .create_resource(
                "v1",
                "Pod",
                Some(namespace),
                "shared-name",
                json!({"metadata": {}, "spec": {"containers": []}}),
            )
            .await
            .unwrap();
        assert_eq!(pod.namespace.as_deref(), Some(namespace));
        assert_eq!(
            pod.data
                .pointer("/metadata/namespace")
                .and_then(Value::as_str),
            Some(namespace)
        );
        assert!(pod.resource_version > 0);
        assert_eq!(
            pod.data
                .pointer("/metadata/resourceVersion")
                .and_then(Value::as_str),
            Some(pod.resource_version.to_string().as_str()),
        );
        created.push(pod);
    }
    assert_ne!(created[0].uid, created[1].uid);
    assert!(created[1].resource_version > created[0].resource_version);

    let node = sources
        .create_resource(
            "v1",
            "Node",
            Some("must-be-cleared"),
            "node-a",
            json!({"metadata": {"namespace": "must-be-cleared"}}),
        )
        .await
        .unwrap();
    assert_eq!(node.namespace, None);
    assert!(node.data.pointer("/metadata/namespace").is_none());
    assert!(sources.node("node-a").await.unwrap().is_some());

    let original = created[0].clone();
    let updated = sources
        .update_resource(
            "v1",
            "Pod",
            Some("team-a"),
            "shared-name",
            json!({"metadata": {}, "spec": {"containers": []}, "status": {"phase": "Running"}}),
            original.resource_version,
        )
        .await
        .unwrap();
    assert!(updated.resource_version > node.resource_version);
    assert_eq!(updated.uid, original.uid);
    assert_eq!(
        updated
            .data
            .pointer("/status/phase")
            .and_then(Value::as_str),
        Some("Running")
    );

    let stale = sources
        .update_resource(
            "v1",
            "Pod",
            Some("team-a"),
            "shared-name",
            json!({"metadata": {}, "status": {"phase": "Failed"}}),
            original.resource_version,
        )
        .await;
    assert!(
        stale
            .unwrap_err()
            .to_string()
            .contains("resourceVersion conflict")
    );
    let after_stale = sources
        .get_resource("v1", "Pod", Some("team-a"), "shared-name")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_stale.resource_version, updated.resource_version);
    assert_eq!(
        after_stale
            .data
            .pointer("/status/phase")
            .and_then(Value::as_str),
        Some("Running")
    );

    let namespaced = sources
        .list_pods(PodListRequest::try_new(Some("team-a".into()), None, None, None, None).unwrap())
        .await
        .unwrap();
    assert_eq!(namespaced.items().len(), 1);
    assert!(namespaced.resource_version() >= updated.resource_version);
    let all = sources
        .list_pods(PodListRequest::try_new(None, None, None, None, None).unwrap())
        .await
        .unwrap();
    assert_eq!(all.items().len(), 2);
    assert!(all.resource_version() >= updated.resource_version);

    sources
        .delete_resource("v1", "Pod", Some("team-b"), "shared-name")
        .await
        .unwrap();
    assert!(
        sources
            .get_resource("v1", "Pod", Some("team-b"), "shared-name")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        sources
            .get_resource("v1", "Pod", Some("team-a"), "shared-name")
            .await
            .unwrap()
            .is_some()
    );
}
