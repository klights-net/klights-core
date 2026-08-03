use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use serde_json::{Value, json};

use super::PvcStore;
use crate::common::ControllerStatusStore;

type ResourceKey = (String, String, Option<String>, String);

static NEXT_DATA_ROOT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct Datastore {
    resources: Mutex<BTreeMap<ResourceKey, Resource>>,
    next_resource_version: AtomicU64,
    local_path_root: PathBuf,
}

impl Default for Datastore {
    fn default() -> Self {
        let id = NEXT_DATA_ROOT.fetch_add(1, Ordering::Relaxed);
        Self {
            resources: Mutex::new(BTreeMap::new()),
            next_resource_version: AtomicU64::new(0),
            local_path_root: std::env::temp_dir()
                .join("klights-controller-tests")
                .join("pvc")
                .join(format!("{}-{id}", std::process::id())),
        }
    }
}

impl Drop for Datastore {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.local_path_root)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!(
                "failed to clean PVC test data root {}: {error}",
                self.local_path_root.display()
            );
        }
    }
}

impl Datastore {
    fn key(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> ResourceKey {
        (
            api_version.to_string(),
            kind.to_string(),
            namespace.map(str::to_string),
            name.to_string(),
        )
    }

    fn next_rv(&self) -> i64 {
        self.next_resource_version.fetch_add(1, Ordering::Relaxed) as i64 + 1
    }

    fn normalize_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
        resource_version: i64,
    ) -> Resource {
        let object = data
            .as_object_mut()
            .expect("test resource must be a JSON object");
        object.insert("apiVersion".to_string(), json!(api_version));
        object.insert("kind".to_string(), json!(kind));
        let metadata = object
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let metadata = metadata
            .as_object_mut()
            .expect("test metadata must be an object");
        metadata.insert("name".to_string(), json!(name));
        match namespace {
            Some(namespace) => {
                metadata.insert("namespace".to_string(), json!(namespace));
            }
            None => {
                metadata.remove("namespace");
            }
        }
        metadata.insert(
            "resourceVersion".to_string(),
            json!(resource_version.to_string()),
        );
        let uid = metadata
            .get("uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{resource_version:08x}-0000-4000-8000-000000000000"));
        metadata.insert("uid".to_string(), json!(&uid));
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

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> ControllerStoreResult<Resource> {
        let key = Self::key(api_version, kind, namespace, name);
        let mut resources = self.resources.lock().expect("PVC test resource lock");
        if resources.contains_key(&key) {
            return Err(ControllerStoreError::already_exists(format!(
                "{kind} {name} already exists"
            )));
        }
        let resource =
            self.normalize_resource(api_version, kind, namespace, name, data, self.next_rv());
        resources.insert(key, resource.clone());
        Ok(resource)
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self
            .resources
            .lock()
            .expect("PVC test resource lock")
            .get(&Self::key(api_version, kind, namespace, name))
            .cloned())
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<()> {
        self.resources
            .lock()
            .expect("PVC test resource lock")
            .remove(&Self::key(api_version, kind, namespace, name));
        Ok(())
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
        let mut resources = self.resources.lock().expect("PVC test resource lock");
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
        let updated =
            self.normalize_resource(api_version, kind, namespace, name, data, self.next_rv());
        resources.insert(key, updated.clone());
        Ok(updated)
    }

    async fn update_resource(
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

    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_resource_version: Option<i64>,
    ) -> ControllerStoreResult<Resource> {
        self.update_status(
            api_version,
            kind,
            namespace,
            name,
            status,
            ResourcePreconditions {
                uid: None,
                resource_version: expected_resource_version,
            },
        )
        .await
    }

    fn resources_of_kind(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<Resource> {
        self.resources
            .lock()
            .expect("PVC test resource lock")
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

    fn file_process_executor(&self) -> klights_supervisor::FileProcessExecutor {
        klights_supervisor::FileProcessExecutor::new(Arc::new(
            klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            ),
        ))
    }

    fn local_path_root(&self) -> &Path {
        &self.local_path_root
    }
}

#[async_trait]
impl ControllerStatusStore for Datastore {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name).await
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
        let current = self
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| ControllerStoreError::not_found(format!("{kind} {name}")))?;
        let mut data = (*current.data).clone();
        data["status"] = status;
        self.update_with_preconditions(api_version, kind, namespace, name, data, preconditions)
    }

    fn log_noop_status_write(
        &self,
        _operation: &'static str,
        _resource: &Resource,
        _reason: &'static str,
    ) {
    }
}

#[async_trait]
impl PvcStore for Datastore {
    async fn get_pvc(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn list_persistent_volumes(&self) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self.resources_of_kind("v1", "PersistentVolume", None))
    }

    async fn get_persistent_volume(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolume", None, name)
            .await
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "PersistentVolume", None, name, value)
            .await
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_with_preconditions("v1", "PersistentVolume", None, name, value, preconditions)
    }
}

async fn reconcile_pvc(db: &Datastore, pvc: &Value) -> Result<Value> {
    super::reconcile_pvc(&db.file_process_executor(), db.local_path_root(), db, pvc).await
}

/// Helper to fetch latest PVC from DB with resourceVersion injected
async fn get_pvc(db: &Datastore, namespace: &str, name: &str) -> Value {
    let resource = db
        .get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
        .await
        .unwrap()
        .unwrap();

    let pvc: Value =
        crate::ports::inject_resource_version(resource.data, resource.resource_version);
    pvc
}

/// Helper to fetch latest PV from DB with resourceVersion injected
async fn get_pv(db: &Datastore, name: &str) -> Value {
    let resource = db
        .get_resource("v1", "PersistentVolume", None, name)
        .await
        .unwrap()
        .unwrap();

    let pv: Value = crate::ports::inject_resource_version(resource.data, resource.resource_version);
    pv
}

#[tokio::test]
async fn test_pvc_stale_snapshot_after_delete_does_not_bind_pv() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "stale-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"},
            "persistentVolumeReclaimPolicy": "Retain"
        },
        "status": {"phase": "Available"}
    });
    db.create_resource("v1", "PersistentVolume", None, "stale-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "stale-pvc", "namespace": "default", "uid": "stale-pvc-uid"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });
    let created = db
        .create_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "stale-pvc",
            pvc,
        )
        .await
        .unwrap();
    let stale_snapshot =
        crate::ports::inject_resource_version(created.data, created.resource_version);

    db.delete_resource("v1", "PersistentVolumeClaim", Some("default"), "stale-pvc")
        .await
        .unwrap();

    reconcile_pvc(&db, &stale_snapshot).await.unwrap();

    let pv = db
        .get_resource("v1", "PersistentVolume", None, "stale-pv")
        .await
        .unwrap()
        .expect("PV should remain");
    assert_eq!(pv.data.pointer("/status/phase"), Some(&json!("Available")));
    assert!(
        pv.data.pointer("/spec/claimRef").is_none(),
        "stale deleted PVC reconcile must not bind a PV"
    );
}

#[tokio::test]
async fn test_pvc_binds_to_matching_pv() {
    let db = Datastore::default();

    // Create a PersistentVolume
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "test-pv"
        },
        "spec": {
            "capacity": {
                "storage": "1Gi"
            },
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {
                "path": "/mnt/data"
            },
            "persistentVolumeReclaimPolicy": "Retain"
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "test-pv", pv)
        .await
        .unwrap();

    // Create a PersistentVolumeClaim
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should bind to PV
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let _updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC status is Bound
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "test-pv");

    // Verify PV status is Bound
    let pv = get_pv(&db, "test-pv").await;
    assert_eq!(pv["status"]["phase"], "Bound");

    // Verify PV spec.claimRef is set to reference the PVC
    let claim_ref = &pv["spec"]["claimRef"];
    assert!(
        !claim_ref.is_null(),
        "PV spec.claimRef must not be null after binding"
    );
    assert_eq!(claim_ref["apiVersion"], "v1");
    assert_eq!(claim_ref["kind"], "PersistentVolumeClaim");
    assert_eq!(claim_ref["name"], "test-pvc");
    assert_eq!(claim_ref["namespace"], "default");
    assert!(
        claim_ref.get("uid").and_then(|u| u.as_str()).is_some(),
        "claimRef.uid must be set"
    );
    assert!(
        claim_ref
            .get("resourceVersion")
            .and_then(|v| v.as_str())
            .is_some(),
        "claimRef.resourceVersion must be set"
    );
}

#[tokio::test]
async fn test_pvc_long_exponent_quantity_selects_exact_smallest_sufficient_pv() {
    let db = Datastore::default();
    let one_long = format!("0.{}1e5000", "0".repeat(4999));

    for (name, capacity) in [
        ("too-small", "0".to_string()),
        ("exact-long", one_long),
        ("larger", "2".to_string()),
    ] {
        db.create_resource(
            "v1",
            "PersistentVolume",
            None,
            name,
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolume",
                "metadata": {"name": name},
                "spec": {
                    "capacity": {"storage": capacity},
                    "accessModes": ["ReadWriteOnce"],
                    "persistentVolumeReclaimPolicy": "Retain"
                },
                "status": {"phase": "Available"}
            }),
        )
        .await
        .unwrap();
    }

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "long-quantity-pvc",
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "long-quantity-pvc", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1"}}
            }
        }),
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "long-quantity-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "long-quantity-pvc").await;
    assert_eq!(pvc.pointer("/status/phase"), Some(&json!("Bound")));
    assert_eq!(
        pvc.pointer("/status/volumeName"),
        Some(&json!("exact-long")),
        "equivalent long and ordinary quantities must compare equal, while a smaller PV stays insufficient"
    );
    assert_eq!(
        get_pv(&db, "too-small").await.pointer("/status/phase"),
        Some(&json!("Available"))
    );
}

#[tokio::test]
async fn test_pvc_bind_preserves_status_conditions() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "condition-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain"
        },
        "status": {"phase": "Available"}
    });
    db.create_resource("v1", "PersistentVolume", None, "condition-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "condition-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        },
        "status": {
            "phase": "Pending",
            "conditions": [{
                "type": "StatusPatched",
                "status": "True",
                "reason": "E2E patchedStatus",
                "message": "Set from e2e test"
            }]
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "condition-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "condition-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "condition-pvc").await;
    assert_eq!(pvc.pointer("/status/phase"), Some(&json!("Bound")));
    assert_eq!(
        pvc.pointer("/status/conditions/0/type"),
        Some(&json!("StatusPatched")),
        "PVC binding must preserve conditions written through the status subresource"
    );
    assert_eq!(
        pvc.pointer("/status/conditions/0/reason"),
        Some(&json!("E2E patchedStatus"))
    );
    assert_eq!(
        pvc.pointer("/status/conditions/0/message"),
        Some(&json!("Set from e2e test"))
    );
}

#[tokio::test]
async fn test_pvc_status_writer_rejects_stale_snapshot_after_status_patch() {
    let db = Datastore::default();

    let created = db
        .create_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "stale-status-pvc",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": "stale-status-pvc",
                    "namespace": "default",
                    "uid": "stale-status-pvc-uid"
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "1Gi"}}
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "stale-status-pvc",
        json!({
            "phase": "Pending",
            "conditions": [{
                "type": "StatusPatched",
                "status": "True",
                "reason": "E2E patchedStatus",
                "message": "Set from e2e test"
            }]
        }),
        Some(created.resource_version),
    )
    .await
    .expect("user status patch should win the race first");

    let stale_write = crate::common::write_status_for_resource(
        &db,
        &created,
        &json!({
            "phase": "Bound",
            "accessModes": ["ReadWriteOnce"],
            "capacity": {"storage": "1Gi"},
            "volumeName": "pv-for-stale-status-pvc"
        }),
    )
    .await;
    let err = stale_write.expect_err("stale PVC controller status write must not rebase");
    assert!(
        err.downcast_ref::<ControllerStoreError>()
            .is_some_and(ControllerStoreError::is_conflict),
        "expected stale PVC status write conflict, got {err:#}"
    );

    let pvc = get_pvc(&db, "default", "stale-status-pvc").await;
    assert_eq!(pvc.pointer("/status/phase"), Some(&json!("Pending")));
    assert_eq!(
        pvc.pointer("/status/conditions/0/type"),
        Some(&json!("StatusPatched")),
        "stale controller write must not drop user-patched status conditions"
    );
}

#[tokio::test]
async fn test_pv_bind_preserves_status_reason_and_message() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "message-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain"
        },
        "status": {
            "phase": "Available",
            "reason": "E2E patchStatus",
            "message": "StatusPatched"
        }
    });
    db.create_resource("v1", "PersistentVolume", None, "message-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "message-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "message-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "message-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pv = get_pv(&db, "message-pv").await;
    assert_eq!(pv.pointer("/status/phase"), Some(&json!("Bound")));
    assert_eq!(
        pv.pointer("/status/reason"),
        Some(&json!("E2E patchStatus")),
        "PV binding must preserve reason written through the status subresource"
    );
    assert_eq!(
        pv.pointer("/status/message"),
        Some(&json!("StatusPatched")),
        "PV binding must preserve message written through the status subresource"
    );
}

#[tokio::test]
async fn test_pvc_status_pending_when_no_matching_pv() {
    let db = Datastore::default();

    // Create a PVC without any matching PV
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should set status to Pending
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let _updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC status is Pending
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[tokio::test]
async fn test_pvc_pending_reconcile_is_idempotent() {
    let db = Datastore::default();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        },
        "status": {
            "phase": "Pending",
            "conditions": [{
                "type": "StatusPatched",
                "status": "True"
            }]
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc_before = db
        .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
        .await
        .unwrap()
        .unwrap();

    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc_after = db
        .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        pvc_after.resource_version, pvc_before.resource_version,
        "already-Pending PVC reconcile must not write a no-op status update"
    );
    assert_eq!(updated_pvc["status"]["phase"], "Pending");
    assert_eq!(
        updated_pvc["status"]["conditions"][0]["type"],
        "StatusPatched"
    );
}

#[tokio::test]
async fn test_pvc_already_bound_no_change() {
    let db = Datastore::default();

    // Create a PVC that's already bound
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        },
        "status": {
            "phase": "Bound",
            "volumeName": "test-pv"
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should not change anything
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify status unchanged
    assert_eq!(updated_pvc["status"]["phase"], "Bound");
    assert_eq!(updated_pvc["status"]["volumeName"], "test-pv");
}

#[tokio::test]
async fn test_pvc_access_modes_must_match() {
    let db = Datastore::default();

    // Create a PV with ReadWriteMany
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "test-pv"
        },
        "spec": {
            "capacity": {
                "storage": "1Gi"
            },
            "accessModes": ["ReadWriteMany"],
            "hostPath": {
                "path": "/mnt/data"
            }
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "test-pv", pv)
        .await
        .unwrap();

    // Create a PVC requesting ReadWriteOnce (not in PV's access modes)
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile - should NOT bind (access modes don't match)
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let _updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC remains Pending
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");

    // Verify PV remains Available
    let pv = get_pv(&db, "test-pv").await;
    assert_eq!(pv["status"]["phase"], "Available");
}

#[tokio::test]
async fn test_pod_can_mount_bound_pvc() {
    // This is an integration test - it tests the full flow:
    // 1. Create PV
    // 2. Create PVC
    // 3. Reconcile PVC (binds to PV)
    // 4. Create Pod with PVC volume
    // 5. Verify Pod can reference the PVC

    let db = Datastore::default();

    // Create a PV
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "test-pv"
        },
        "spec": {
            "capacity": {
                "storage": "1Gi"
            },
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {
                "path": "/tmp/test-pv-data"
            }
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "test-pv", pv)
        .await
        .unwrap();

    // Create a PVC
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should bind to PV
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let _updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC is Bound
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "test-pv");

    // Now create a Pod that uses this PVC
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "test-container",
                "image": "busybox",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data"
                }]
            }],
            "volumes": [{
                "name": "data",
                "persistentVolumeClaim": {
                    "claimName": "test-pvc"
                }
            }]
        }
    });

    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // The actual pod creation would call pod_manager which resolves the PVC
    // For now, we just verify the PVC is bound and available
    // Full pod mounting is tested in pod_manager integration tests
}

#[tokio::test]
async fn test_provision_pv_for_local_path_pvc() {
    let db = Datastore::default();

    // Create a PVC with storageClassName "local-path"
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default",
            "uid": "test-uid-123"
        },
        "spec": {
            "storageClassName": "local-path",
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should auto-provision a PV
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    let _updated_pvc = reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC is Bound
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");

    // Verify a PV was created
    let pv_name = pvc["status"]["volumeName"].as_str().unwrap();
    assert!(pv_name.starts_with("pvc-"));

    let pv = get_pv(&db, pv_name).await;
    assert_eq!(pv["spec"]["capacity"]["storage"], "1Gi");
    assert_eq!(pv["spec"]["accessModes"], json!(["ReadWriteOnce"]));
    assert_eq!(pv["spec"]["storageClassName"], "local-path");
    assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Delete");
    assert!(
        pv["spec"]["hostPath"]["path"]
            .as_str()
            .unwrap()
            .contains("default/test-pvc")
    );
    assert_eq!(pv["status"]["phase"], "Bound");
}

#[tokio::test]
async fn test_provision_pv_binds_to_pvc() {
    let db = Datastore::default();

    // Create a PVC with storageClassName "local-path"
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "auto-pvc",
            "namespace": "default",
            "uid": "auto-uid-456"
        },
        "spec": {
            "storageClassName": "local-path",
            "accessModes": ["ReadWriteMany"],
            "resources": {
                "requests": {
                    "storage": "2Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "auto-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC
    let pvc = get_pvc(&db, "default", "auto-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC is Bound with volumeName
    let pvc = get_pvc(&db, "default", "auto-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert!(pvc["status"]["volumeName"].as_str().is_some());

    // Verify PV exists and is Bound
    let pv_name = pvc["status"]["volumeName"].as_str().unwrap();
    let pv = get_pv(&db, pv_name).await;
    assert_eq!(pv["status"]["phase"], "Bound");
    assert_eq!(pv["spec"]["capacity"]["storage"], "2Gi");

    // Verify PV spec.claimRef is set to reference the PVC
    let claim_ref = &pv["spec"]["claimRef"];
    assert!(
        !claim_ref.is_null(),
        "PV spec.claimRef must not be null after provisioning bind"
    );
    assert_eq!(claim_ref["apiVersion"], "v1");
    assert_eq!(claim_ref["kind"], "PersistentVolumeClaim");
    assert_eq!(claim_ref["name"], "auto-pvc");
    assert_eq!(claim_ref["namespace"], "default");
    assert_eq!(claim_ref["uid"], "auto-uid-456");
    assert!(
        claim_ref
            .get("resourceVersion")
            .and_then(|v| v.as_str())
            .is_some(),
        "claimRef.resourceVersion must be set"
    );
}

#[tokio::test]
async fn test_no_provision_for_unknown_storage_class() {
    let db = Datastore::default();

    // Create a PVC with unknown storageClassName
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "nfs-pvc",
            "namespace": "default"
        },
        "spec": {
            "storageClassName": "nfs-csi",
            "accessModes": ["ReadWriteMany"],
            "resources": {
                "requests": {
                    "storage": "5Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "nfs-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should NOT provision a PV
    let pvc = get_pvc(&db, "default", "nfs-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC remains Pending (no PV created)
    let pvc = get_pvc(&db, "default", "nfs-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
    assert!(pvc["status"]["volumeName"].is_null());

    // Verify no PV was created
    let pvs = db.resources_of_kind("v1", "PersistentVolume", None);
    assert_eq!(pvs.len(), 0);
}

#[tokio::test]
async fn test_no_provision_when_matching_pv_exists() {
    let db = Datastore::default();

    // Create a PV first
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "existing-pv"
        },
        "spec": {
            "capacity": {
                "storage": "1Gi"
            },
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "local-path",
            "hostPath": {
                "path": "/tmp/existing"
            },
            "persistentVolumeReclaimPolicy": "Retain"
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "existing-pv", pv)
        .await
        .unwrap();

    // Create a PVC with storageClassName "local-path"
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "storageClassName": "local-path",
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    // Reconcile PVC - should use existing PV, NOT provision a new one
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // Verify PVC is Bound to the existing PV
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "existing-pv");

    // Verify only one PV exists (no new PV created)
    let pvs = db.resources_of_kind("v1", "PersistentVolume", None);
    assert_eq!(pvs.len(), 1);
    assert_eq!(pvs[0].name, "existing-pv");
}

#[tokio::test]
async fn test_pvc_binds_to_pv_without_status() {
    // PV with no status field should be treated as Available
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "no-status-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        }
        // No "status" field at all
    });

    db.create_resource("v1", "PersistentVolume", None, "no-status-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "test-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "test-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "no-status-pv");
}

#[tokio::test]
async fn test_pvc_capacity_mismatch_no_bind() {
    // PV with 1023Mi should NOT bind to PVC requesting 1Gi (one unit below)
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "big-pv"},
        "spec": {
            "capacity": {"storage": "1023Mi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "big-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "small-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "small-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "small-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // Should remain Pending — no PV has capacity >= requested storage
    let pvc = get_pvc(&db, "default", "small-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[tokio::test]
async fn test_pvc_binds_when_pv_capacity_exceeds_storage_request() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "large-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "large-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "half-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "512Mi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "half-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "half-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "half-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "large-pv");
}

#[tokio::test]
async fn test_pvc_binds_when_request_equals_larger_pv_units() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "equal-pv"},
        "spec": {
            "capacity": {"storage": "1024Mi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "equal-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "equal-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "equal-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "equal-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "equal-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "equal-pv");
}

#[tokio::test]
async fn test_pvc_does_not_bind_when_decimal_and_binary_units_do_not_match_semantics() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "decimal-vs-binary-pv"},
        "spec": {
            "capacity": {"storage": "1G"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "decimal-vs-binary-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "decimal-vs-binary-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "decimal-vs-binary-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "decimal-vs-binary-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "decimal-vs-binary-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[tokio::test]
async fn test_pvc_binding_requires_matching_storage_class_and_volume_mode_and_selector() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "annotated-pv",
            "labels": {"zone": "us-west-1", "tier": "gold"}
        },
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "fast",
            "volumeMode": "Filesystem",
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "annotated-pv", pv)
        .await
        .unwrap();

    let invalid = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "invalid-pvc", "namespace": "default"},
        "spec": {
            "storageClassName": "slow",
            "volumeMode": "Block",
            "selector": {"matchLabels": {"tier": "gold"}},
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "invalid-pvc",
        invalid,
    )
    .await
    .unwrap();

    let invalid_pvc = get_pvc(&db, "default", "invalid-pvc").await;
    reconcile_pvc(&db, &invalid_pvc).await.unwrap();

    let invalid_pvc = get_pvc(&db, "default", "invalid-pvc").await;
    assert_eq!(invalid_pvc["status"]["phase"], "Pending");

    let mut valid = invalid_pvc.clone();
    valid["spec"]["storageClassName"] = json!("fast");
    valid["spec"]["volumeMode"] = json!("Filesystem");
    let valid_rv = valid["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    db.update_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "invalid-pvc",
        valid,
        valid_rv,
    )
    .await
    .unwrap();

    let valid_pvc = get_pvc(&db, "default", "invalid-pvc").await;
    reconcile_pvc(&db, &valid_pvc).await.unwrap();

    let valid_pvc = get_pvc(&db, "default", "invalid-pvc").await;
    assert_eq!(valid_pvc["status"]["phase"], "Bound");
    assert_eq!(valid_pvc["status"]["volumeName"], "annotated-pv");
}

#[tokio::test]
async fn test_pvc_binding_is_deterministic_by_pv_name() {
    let db = Datastore::default();

    let pv_z = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-z"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data-z"}
        },
        "status": {"phase": "Available"}
    });
    let pv_a = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-a"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data-a"}
        },
        "status": {"phase": "Available"}
    });

    // Create out of lexical order to verify deterministic selection is enforced.
    db.create_resource("v1", "PersistentVolume", None, "pv-z", pv_z)
        .await
        .unwrap();
    db.create_resource("v1", "PersistentVolume", None, "pv-a", pv_a)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "sorted-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "sorted-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "sorted-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "sorted-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "pv-a");
}

#[tokio::test]
async fn test_pvc_binding_empty_storage_class_matches_omitted_pv_class() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "classless-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/classless"}
        },
        "status": {"phase": "Available"}
    });
    db.create_resource("v1", "PersistentVolume", None, "classless-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "classless-pvc", "namespace": "default"},
        "spec": {
            "storageClassName": "",
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "classless-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "classless-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "classless-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "classless-pv");
}

#[tokio::test]
async fn test_pvc_binding_prefers_smallest_sufficient_pv_then_name() {
    let db = Datastore::default();

    for (name, storage) in [
        ("a-oversized", "100Gi"),
        ("b-exact", "1Gi"),
        ("z-also-exact", "1Gi"),
    ] {
        let pv = json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": name},
            "spec": {
                "capacity": {"storage": storage},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": format!("/mnt/{name}")}
            },
            "status": {"phase": "Available"}
        });
        db.create_resource("v1", "PersistentVolume", None, name, pv)
            .await
            .unwrap();
    }

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "smallest-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "smallest-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "smallest-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "smallest-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "b-exact");
}

#[tokio::test]
async fn test_pvc_binding_skips_invalid_pv_capacity_candidate() {
    let db = Datastore::default();

    for (name, storage) in [("bad-pv", "not-a-quantity"), ("good-pv", "1Gi")] {
        let pv = json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": name},
            "spec": {
                "capacity": {"storage": storage},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": format!("/mnt/{name}")}
            },
            "status": {"phase": "Available"}
        });
        db.create_resource("v1", "PersistentVolume", None, name, pv)
            .await
            .unwrap();
    }

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "skip-bad-pv-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "skip-bad-pv-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "skip-bad-pv-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "skip-bad-pv-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "good-pv");
}

#[tokio::test]
async fn test_pvc_binding_preserves_fractional_binary_quantity_ordering() {
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "fractional-pv"},
        "spec": {
            "capacity": {"storage": "1.2345Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/fractional-pv"}
        },
        "status": {"phase": "Available"}
    });
    db.create_resource("v1", "PersistentVolume", None, "fractional-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "fractional-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1.2348Gi"}}
        }
    });
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "fractional-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "fractional-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "default", "fractional-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
    assert!(pvc.pointer("/status/volumeName").is_none());
}

#[tokio::test]
async fn test_pvc_subset_access_modes_bind() {
    // PVC requesting ReadWriteOnce should bind to PV with [ReadWriteOnce, ReadOnlyMany]
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "multi-access-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce", "ReadOnlyMany"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Available"}
    });

    db.create_resource("v1", "PersistentVolume", None, "multi-access-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "test-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "test-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // PVC access modes are a subset of PV — should bind
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["status"]["volumeName"], "multi-access-pv");
}

#[tokio::test]
async fn test_provision_pv_without_uid_uses_namespace_name() {
    let db = Datastore::default();

    // PVC without UID — should use fallback pv name: pvc-{ns}-{name}
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "my-pvc",
            "namespace": "apps"
            // No "uid" field
        },
        "spec": {
            "storageClassName": "local-path",
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "500Mi"}}
        }
    });

    db.create_resource("v1", "PersistentVolumeClaim", Some("apps"), "my-pvc", pvc)
        .await
        .unwrap();

    let pvc = get_pvc(&db, "apps", "my-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    let pvc = get_pvc(&db, "apps", "my-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Bound");
    // DB auto-generates a UUID for uid when not provided, so PV name
    // will be pvc-{auto-generated-uid} rather than pvc-{ns}-{name}.
    // Verify the PV name follows the pvc-{uid} format.
    let pv_name = pvc["status"]["volumeName"].as_str().unwrap();
    assert!(
        pv_name.starts_with("pvc-"),
        "PV name should start with 'pvc-', got: {}",
        pv_name
    );
    // Should be pvc-{uuid} format (36 char UUID + 4 char prefix)
    assert_eq!(
        pv_name.len(),
        4 + 36,
        "PV name should be pvc-{{uuid}}, got: {}",
        pv_name
    );
}

#[tokio::test]
async fn test_pvc_skips_bound_pv() {
    // PV already Bound should be skipped during matching
    let db = Datastore::default();

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "bound-pv"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": "/mnt/data"}
        },
        "status": {"phase": "Bound"}
    });

    db.create_resource("v1", "PersistentVolume", None, "bound-pv", pv)
        .await
        .unwrap();

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "test-pvc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc,
    )
    .await
    .unwrap();

    let pvc = get_pvc(&db, "default", "test-pvc").await;
    reconcile_pvc(&db, &pvc).await.unwrap();

    // Should remain Pending — the only PV is already Bound
    let pvc = get_pvc(&db, "default", "test-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
}
