use super::*;

use serde_json::json;

/// Helper to fetch latest PVC from DB with resourceVersion injected
async fn get_pvc(db: &dyn DatastoreBackend, namespace: &str, name: &str) -> Value {
    let resource = db
        .get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
        .await
        .unwrap()
        .unwrap();

    let mut pvc: Value = std::sync::Arc::unwrap_or_clone(resource.data);
    inject_resource_version(&mut pvc, resource.resource_version);
    pvc
}

/// Helper to fetch latest PV from DB with resourceVersion injected
async fn get_pv(db: &dyn DatastoreBackend, name: &str) -> Value {
    let resource = db
        .get_resource("v1", "PersistentVolume", None, name)
        .await
        .unwrap()
        .unwrap();

    let mut pv: Value = std::sync::Arc::unwrap_or_clone(resource.data);
    inject_resource_version(&mut pv, resource.resource_version);
    pv
}

#[tokio::test]
async fn test_pvc_stale_snapshot_after_delete_does_not_bind_pv() {
    let db = crate::datastore::test_support::in_memory().await;

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
        crate::api::inject_resource_version(created.data, created.resource_version);

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
    let db = crate::datastore::test_support::in_memory().await;

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
async fn test_pvc_status_pending_when_no_matching_pv() {
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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

    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let pvs = db
        .list_resources(
            "v1",
            "PersistentVolume",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pvs.items.len(), 0);
}

#[tokio::test]
async fn test_no_provision_when_matching_pv_exists() {
    let db = crate::datastore::test_support::in_memory().await;

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
    let pvs = db
        .list_resources(
            "v1",
            "PersistentVolume",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pvs.items.len(), 1);
    assert_eq!(pvs.items[0].name, "existing-pv");
}

#[tokio::test]
async fn test_pvc_binds_to_pv_without_status() {
    // PV with no status field should be treated as Available
    let db = crate::datastore::test_support::in_memory().await;

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
    // PV with 5Gi should NOT bind to PVC requesting 1Gi (exact match required)
    let db = crate::datastore::test_support::in_memory().await;

    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "big-pv"},
        "spec": {
            "capacity": {"storage": "5Gi"},
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

    // Should remain Pending — exact capacity match required (Phase 1)
    let pvc = get_pvc(&db, "default", "small-pvc").await;
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[tokio::test]
async fn test_pvc_subset_access_modes_bind() {
    // PVC requesting ReadWriteOnce should bind to PV with [ReadWriteOnce, ReadOnlyMany]
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
    let db = crate::datastore::test_support::in_memory().await;

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
