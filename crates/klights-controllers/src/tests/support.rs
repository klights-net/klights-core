use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klights_cluster_core::{
    Resource, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
};
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryFuture,
};
use klights_reconcile_api::{
    ControllerStoreError, ControllerStoreResult, GcNonPodFinalizationFuture,
    GcNonPodFinalizationOutcome, GcNonPodFinalizationPort, GcNonPodFinalizationRequest,
    GcPodDeleteError, GcPodDeleteFuture, GcPodDeleteRequest, GcPodDeleteSink,
};
use serde_json::{Value, json};

type ResourceKey = (String, String, Option<String>, String);

#[derive(Debug, Default)]
struct Inner {
    resources: Mutex<BTreeMap<ResourceKey, Resource>>,
    next_resource_version: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TestStore {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) struct ResourceList {
    pub(crate) items: Vec<Resource>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResourceListQuery<'a> {
    label_selector: Option<&'a str>,
    field_selector: Option<&'a str>,
}

impl ResourceListQuery<'_> {
    pub(crate) const fn all() -> Self {
        Self {
            label_selector: None,
            field_selector: None,
        }
    }

    pub(crate) const fn new<'a>(
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        _limit: Option<i64>,
        _continue_token: Option<&'a str>,
    ) -> ResourceListQuery<'a> {
        ResourceListQuery {
            label_selector,
            field_selector,
        }
    }
}

pub(crate) async fn in_memory() -> TestStore {
    TestStore::default()
}

impl TestStore {
    fn key(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> ResourceKey {
        (
            api_version.to_string(),
            kind.to_string(),
            namespace.map(str::to_string),
            name.to_string(),
        )
    }

    fn next_rv(&self) -> i64 {
        self.inner
            .next_resource_version
            .fetch_add(1, Ordering::Relaxed) as i64
            + 1
    }

    fn normalize(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
        resource_version: i64,
    ) -> Resource {
        let object = data.as_object_mut().expect("test resource object");
        object.insert("apiVersion".into(), json!(api_version));
        object.insert("kind".into(), json!(kind));
        let metadata = object
            .entry("metadata")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("test metadata object");
        metadata.insert("name".into(), json!(name));
        match namespace {
            Some(namespace) => {
                metadata.insert("namespace".into(), json!(namespace));
            }
            None => {
                metadata.remove("namespace");
            }
        }
        let uid = metadata
            .get("uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{resource_version:08x}-0000-4000-8000-000000000000"));
        metadata.insert("uid".into(), json!(&uid));
        metadata.insert(
            "resourceVersion".into(),
            json!(resource_version.to_string()),
        );
        Resource {
            id: resource_version,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            uid,
            resource_version,
            data: Arc::new(data),
        }
    }

    pub(crate) async fn create_namespace(
        &self,
        name: &str,
        data: Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "Namespace", None, name, data)
            .await
    }

    pub(crate) async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> ControllerStoreResult<Resource> {
        let key = Self::key(api_version, kind, namespace, name);
        let mut resources = self.inner.resources.lock().expect("test resource lock");
        if resources.contains_key(&key) {
            return Err(ControllerStoreError::already_exists(format!(
                "{kind} {name} already exists"
            )));
        }
        let resource = self.normalize(api_version, kind, namespace, name, data, self.next_rv());
        resources.insert(key, resource.clone());
        Ok(resource)
    }

    pub(crate) async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get(api_version, kind, namespace, name))
    }

    pub(crate) async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> ControllerStoreResult<ResourceList> {
        let mut items = self
            .resources_of_kind(api_version, kind, namespace)
            .into_iter()
            .filter(|resource| matches_query(resource, query))
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(ResourceList { items })
    }

    pub(crate) async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .resources_of_kind(api_version, kind, namespace)
            .into_iter()
            .filter(|resource| owned_by(resource, owner_uid))
            .collect())
    }

    pub(crate) async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            data,
            ResourcePreconditions::resource_version(expected_resource_version),
        )
    }

    pub(crate) async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        let current = self
            .get(api_version, kind, namespace, name)
            .ok_or_else(|| ControllerStoreError::not_found(format!("{kind} {name}")))?;
        let mut data = (*current.data).clone();
        data["status"] = status;
        self.update_with_preconditions(api_version, kind, namespace, name, data, preconditions)
    }

    pub(crate) async fn replace_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        status: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some(namespace),
            name,
            status,
            ResourcePreconditions::resource_version(expected_resource_version),
        )
        .await
    }

    pub(crate) async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<()> {
        self.inner
            .resources
            .lock()
            .expect("test resource lock")
            .remove(&Self::key(api_version, kind, namespace, name));
        Ok(())
    }

    fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Option<Resource> {
        self.inner
            .resources
            .lock()
            .expect("test resource lock")
            .get(&Self::key(api_version, kind, namespace, name))
            .cloned()
    }

    fn resources_of_kind(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<Resource> {
        self.inner
            .resources
            .lock()
            .expect("test resource lock")
            .values()
            .filter(|resource| {
                resource.api_version == api_version
                    && resource.kind == kind
                    && namespace
                        .is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
            })
            .cloned()
            .collect()
    }

    fn update_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        let key = Self::key(api_version, kind, namespace, name);
        let mut resources = self.inner.resources.lock().expect("test resource lock");
        let current = resources
            .get(&key)
            .ok_or_else(|| ControllerStoreError::not_found(format!("{kind} {name}")))?;
        if preconditions
            .uid
            .as_deref()
            .is_some_and(|uid| uid != current.uid)
            || preconditions
                .resource_version
                .is_some_and(|rv| rv != current.resource_version)
        {
            return Err(ControllerStoreError::conflict(format!(
                "stale {kind} {name}"
            )));
        }
        let updated = self.normalize(api_version, kind, namespace, name, data, self.next_rv());
        resources.insert(key, updated.clone());
        Ok(updated)
    }
}

fn owned_by(resource: &Resource, owner_uid: &str) -> bool {
    resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(Value::as_array)
        .is_some_and(|owners| {
            owners
                .iter()
                .any(|owner| owner.get("uid").and_then(Value::as_str) == Some(owner_uid))
        })
}

fn matches_query(resource: &Resource, query: ResourceListQuery<'_>) -> bool {
    let labels_match = query.label_selector.is_none_or(|selector| {
        selector.split(',').all(|term| {
            let Some((key, value)) = term.split_once('=') else {
                return false;
            };
            resource
                .data
                .pointer(&format!("/metadata/labels/{}", key.trim()))
                .and_then(Value::as_str)
                == Some(value.trim())
        })
    });
    let fields_match = query.field_selector.is_none_or(|selector| {
        selector.split(',').all(|term| {
            let Some((key, value)) = term.split_once('=') else {
                return false;
            };
            let pointer = format!("/{}", key.trim().replace('.', "/"));
            resource.data.pointer(&pointer).and_then(Value::as_str) == Some(value.trim())
        })
    });
    labels_match && fields_match
}

#[async_trait]
impl crate::gc::GcResourceStore for TestStore {
    async fn list_custom_resource_definitions(&self) -> ControllerStoreResult<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get(api_version, kind, namespace, name))
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_with_preconditions(api_version, kind, namespace, name, data, preconditions)
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_with_preconditions(api_version, kind, namespace, name, data, preconditions)
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .inner
            .resources
            .lock()
            .expect("test resource lock")
            .values()
            .filter(|resource| {
                namespace.is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
                    && owned_by(resource, owner_uid)
            })
            .cloned()
            .collect())
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .inner
            .resources
            .lock()
            .expect("test resource lock")
            .values()
            .filter(|resource| {
                namespace.is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
                    && resource
                        .data
                        .pointer("/metadata/ownerReferences")
                        .and_then(Value::as_array)
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| {
                                owner.get("apiVersion").and_then(Value::as_str)
                                    == Some(owner_api_version)
                                    && owner.get("kind").and_then(Value::as_str) == Some(owner_kind)
                                    && owner.get("name").and_then(Value::as_str) == Some(owner_name)
                                    && owner
                                        .get("uid")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .is_empty()
                            })
                        })
            })
            .cloned()
            .collect())
    }
}

impl PodQuery for TestStore {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let pod = self.get("v1", "Pod", Some(request.namespace()), request.name());
            Ok(pod.filter(|pod| request.uid().is_none_or(|uid| pod.uid == uid)))
        })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            PodListResult::try_new(
                self.resources_of_kind("v1", "Pod", request.namespace()),
                self.inner.next_resource_version.load(Ordering::Relaxed) as i64,
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
            Ok(self
                .resources_of_kind("v1", "Pod", Some(request.namespace()))
                .into_iter()
                .filter(|pod| owned_by(pod, request.owner_uid()))
                .collect())
        })
    }
}

fn apply_limit_range_defaults(store: &TestStore, namespace: &str, pod: &mut Value) {
    let Some(default_cpu) = store
        .resources_of_kind("v1", "LimitRange", Some(namespace))
        .iter()
        .find_map(|limit_range| {
            limit_range
                .data
                .pointer("/spec/limits")
                .and_then(Value::as_array)?
                .iter()
                .find(|limit| limit.get("type").and_then(Value::as_str) == Some("Container"))?
                .pointer("/defaultRequest/cpu")
                .cloned()
        })
    else {
        return;
    };
    let Some(containers) = pod
        .pointer_mut("/spec/containers")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for container in containers {
        let resources = container
            .as_object_mut()
            .expect("container object")
            .entry("resources")
            .or_insert_with(|| json!({}));
        let requests = resources
            .as_object_mut()
            .expect("resources object")
            .entry("requests")
            .or_insert_with(|| json!({}));
        requests
            .as_object_mut()
            .expect("requests object")
            .entry("cpu")
            .or_insert_with(|| default_cpu.clone());
    }
}

async fn create_controller_pod(
    store: &TestStore,
    namespace: &str,
    name: &str,
    mut pod: Value,
) -> ControllerStoreResult<Resource> {
    if store
        .resources_of_kind("v1", "ResourceQuota", Some(namespace))
        .iter()
        .any(|quota| {
            quota
                .data
                .pointer("/spec/hard/pods")
                .and_then(Value::as_str)
                == Some("0")
        })
    {
        return Err(ControllerStoreError::unavailable(
            "Pod creation denied by ResourceQuota",
        ));
    }
    apply_limit_range_defaults(store, namespace, &mut pod);
    store
        .create_resource("v1", "Pod", Some(namespace), name, pod)
        .await
}

#[async_trait]
impl crate::job::JobStore for TestStore {
    async fn get_job(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get("batch/v1", "Job", Some(namespace), name))
    }

    async fn update_job_status(
        &self,
        resource: &Resource,
        status: Value,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            "batch/v1",
            "Job",
            resource.namespace.as_deref(),
            &resource.name,
            status,
            ResourcePreconditions::from_resource(resource),
        )
        .await
    }
}

#[async_trait]
impl crate::job::JobPodMutation for TestStore {
    async fn create_job_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> ControllerStoreResult<Resource> {
        create_controller_pod(self, namespace, name, pod).await
    }

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> ControllerStoreResult<Resource> {
        let current = self
            .get("v1", "Pod", Some(namespace), name)
            .ok_or_else(|| ControllerStoreError::not_found("Pod missing"))?;
        let mut pod = (*current.data).clone();
        pod["metadata"]["ownerReferences"] = Value::Array(owner_references);
        self.update_with_preconditions(
            "v1",
            "Pod",
            Some(namespace),
            name,
            pod,
            ResourcePreconditions::from_resource(&current),
        )
    }
}

#[async_trait]
impl crate::statefulset::StatefulSetStore for TestStore {
    async fn get_statefulset(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get("apps/v1", "StatefulSet", Some(namespace), name))
    }

    async fn update_statefulset_status(
        &self,
        resource: &Resource,
        status: Value,
    ) -> ControllerStoreResult<()> {
        self.update_status_only_with_preconditions(
            "apps/v1",
            "StatefulSet",
            resource.namespace.as_deref(),
            &resource.name,
            status,
            ResourcePreconditions::from_resource(resource),
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl crate::statefulset::StatefulSetPodMutation for TestStore {
    async fn create_statefulset_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> ControllerStoreResult<Resource> {
        create_controller_pod(self, namespace, name, pod).await
    }
}

#[async_trait]
impl crate::common::ControllerStatusStore for TestStore {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get(api_version, kind, namespace, name))
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }

    fn log_noop_status_write(
        &self,
        _operation: &'static str,
        _resource: &Resource,
        _reason: &'static str,
    ) {
    }
}

impl GcPodDeleteSink for TestStore {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            let identity = request.identity();
            let Some(current) = self.get("v1", "Pod", Some(&identity.namespace), &identity.name)
            else {
                return Err(GcPodDeleteError::not_found("Pod missing"));
            };
            if current.uid != identity.uid {
                return Err(GcPodDeleteError::identity_changed("Pod UID changed"));
            }
            let mut data = (*current.data).clone();
            data["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
            self.update_with_preconditions(
                "v1",
                "Pod",
                Some(&identity.namespace),
                &identity.name,
                data,
                ResourcePreconditions::from_resource(&current),
            )
            .map_err(|error| GcPodDeleteError::unavailable(error.to_string()))?;
            Ok(())
        })
    }
}

impl GcNonPodFinalizationPort for TestStore {
    fn finalize_non_pod(
        &self,
        request: GcNonPodFinalizationRequest,
    ) -> GcNonPodFinalizationFuture<'_> {
        Box::pin(async move {
            let resource = request.resource;
            self.delete_resource(
                &resource.api_version,
                &resource.kind,
                resource.namespace.as_deref(),
                &resource.name,
            )
            .await
            .map_err(|error| {
                klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
            })?;
            Ok(GcNonPodFinalizationOutcome::HardDeleted)
        })
    }
}

#[async_trait]
impl crate::endpoints::EndpointReconcileStore for TestStore {
    async fn endpoint_namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<bool> {
        Ok(self
            .get("v1", "Namespace", None, namespace)
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .is_some()
            }))
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self.get(api_version, kind, namespace, name))
    }

    async fn list_service_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .resources_of_kind("discovery.k8s.io/v1", "EndpointSlice", Some(namespace))
            .into_iter()
            .filter(|slice| {
                slice
                    .data
                    .pointer("/metadata/labels/kubernetes.io~1service-name")
                    .and_then(Value::as_str)
                    == Some(service_name)
            })
            .collect())
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> ControllerStoreResult<Resource> {
        TestStore::create_resource(self, api_version, kind, namespace, name, data).await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_with_preconditions(api_version, kind, namespace, name, data, preconditions)
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<()> {
        let current = self
            .get(api_version, kind, namespace, name)
            .ok_or_else(|| ControllerStoreError::not_found(format!("{kind} {name}")))?;
        if preconditions
            .uid
            .as_deref()
            .is_some_and(|uid| uid != current.uid)
            || preconditions
                .resource_version
                .is_some_and(|rv| rv != current.resource_version)
        {
            return Err(ControllerStoreError::conflict(format!(
                "stale {kind} {name}"
            )));
        }
        self.delete_resource(api_version, kind, namespace, name)
            .await
    }

    async fn apply_resource_batch(
        &self,
        operations: Vec<ResourceBatchOperation>,
    ) -> ControllerStoreResult<()> {
        for operation in operations {
            match operation {
                ResourceBatchOperation::Put {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data,
                    mode,
                    preconditions,
                } => match mode {
                    ResourceBatchPutMode::Create => {
                        self.create_resource(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            data,
                        )
                        .await?;
                    }
                    ResourceBatchPutMode::Update => {
                        self.update_with_preconditions(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            data,
                            preconditions,
                        )?;
                    }
                },
                ResourceBatchOperation::Delete {
                    api_version,
                    kind,
                    namespace,
                    name,
                    preconditions,
                } => {
                    crate::endpoints::EndpointReconcileStore::delete_resource_with_preconditions(
                        self,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        preconditions,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) async fn find_owned_pods(
    store: &TestStore,
    namespace: &str,
    owner_uid: &str,
) -> anyhow::Result<Vec<Resource>> {
    Ok(store
        .list_resources_by_owner_uid("v1", "Pod", Some(namespace), owner_uid)
        .await?)
}

pub(crate) fn pod_repository_for_test(store: &TestStore) -> Arc<TestStore> {
    Arc::new(store.clone())
}

pub(crate) fn inject_resource_version(data: Arc<Value>, resource_version: i64) -> Value {
    let mut value = Arc::unwrap_or_clone(data);
    value["metadata"]["resourceVersion"] = json!(resource_version.to_string());
    value
}

pub(crate) async fn store_and_prepare(
    store: &TestStore,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: Value,
) -> Value {
    let resource = store
        .create_resource(api_version, kind, namespace, name, data)
        .await
        .expect("store test resource");
    inject_resource_version(resource.data, resource.resource_version)
}

pub(crate) fn deterministic_controller_identity() -> Arc<dyn crate::ControllerIdentityGenerator> {
    Arc::new(crate::identity::DeterministicControllerIdentityGenerator::default())
}

pub(crate) struct ControllerIdentityTestGraph {
    identity: Arc<dyn crate::ControllerIdentityGenerator>,
}

impl Default for ControllerIdentityTestGraph {
    fn default() -> Self {
        Self {
            identity: deterministic_controller_identity(),
        }
    }
}

impl ControllerIdentityTestGraph {
    pub(crate) fn identity(&self) -> Arc<dyn crate::ControllerIdentityGenerator> {
        self.identity.clone()
    }
}

pub(crate) struct ScriptedControllerIdentityGenerator {
    uids: Vec<String>,
    next: AtomicUsize,
}

impl ScriptedControllerIdentityGenerator {
    pub(crate) fn with_uids(uids: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            uids: uids.into_iter().map(str::to_string).collect(),
            next: AtomicUsize::new(0),
        }
    }

    pub(crate) fn uid_calls(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }
}

impl crate::ControllerIdentityGenerator for ScriptedControllerIdentityGenerator {
    fn generate_name(&self, prefix: &str) -> String {
        format!("{prefix}00000")
    }

    fn new_uid(&self) -> String {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        self.uids[index].clone()
    }
}

pub(crate) fn test_reconcile_context<'a>(
    coordination: &'a crate::ControllerCoordination,
    node_name: &'a str,
) -> crate::ControllerReconcileContext<'a> {
    crate::ControllerReconcileContext::new(coordination, node_name)
}
