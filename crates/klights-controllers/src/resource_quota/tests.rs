use super::*;
#[test]
fn test_parse_resource_quantity_storage_matches_kubernetes_semantics() {
    let equal_pairs = [
        ("1Gi", "1024Mi", 1_073_741_824),
        ("512Mi", "0.5Gi", 536_870_912),
    ];
    for (left, right, expected) in equal_pairs {
        assert_eq!(
            parse_resource_quantity("storage", left),
            Some(expected),
            "{left} should parse to {expected}"
        );
        assert_eq!(
            parse_resource_quantity("storage", right),
            Some(expected),
            "{right} should parse to {expected}"
        );
    }

    let ordered_pairs = [("1Gi", "512Mi"), ("1024Mi", "1Gi"), ("2Gi", "1024Mi")];
    for (left, right) in ordered_pairs {
        let left_qty = parse_resource_quantity("storage", left).expect("left quantity parses");
        let right_qty = parse_resource_quantity("storage", right).expect("right quantity parses");
        assert!(
            left_qty >= right_qty,
            "storage quantity ordering must be normalized: {left} >= {right}"
        );
    }

    let one_g = parse_resource_quantity("storage", "1G").expect("1G parses");
    let one_gi = parse_resource_quantity("storage", "1Gi").expect("1Gi parses");
    assert_ne!(one_g, one_gi);
    assert!(one_g < one_gi, "1G must be less than 1Gi");
}

#[test]
fn test_parse_resource_quantity_long_exponent_normalizes_before_quota_totals() {
    // A long-exponent quantity whose decimal scale cancels the exponent must
    // reduce to its exact value, not i64::MAX, so resource-quota totals are not
    // corrupted by the pre-normalization overflow cap.
    let one_long = format!("0.{}1e5000", "0".repeat(4999));
    let one = parse_resource_quantity("storage", &one_long);
    assert_eq!(one, parse_resource_quantity("storage", "1"));
    assert_eq!(one, Some(1));
    assert_ne!(one, Some(i64::MAX));

    // Equivalent long-exponent and ordinary spellings compare equal, so PVC
    // requests and PV capacities using either form bind deterministically.
    let ten_long = format!("0.{}1e5001", "0".repeat(4999));
    assert_eq!(
        parse_resource_quantity("storage", &ten_long),
        parse_resource_quantity("storage", "10")
    );

    // A genuinely huge value still caps so quota overflow accounting is preserved.
    assert_eq!(parse_resource_quantity("storage", "1e5000"), Some(i64::MAX));
}

#[test]
fn test_parse_resource_quantity_storage_rejects_malformed_or_invalid_values() {
    assert_eq!(
        parse_resource_quantity("storage", "1.2345Gi"),
        Some(1_325_534_282),
        "Kubernetes quantity precision is preserved through suffix scaling"
    );
    assert_eq!(
        parse_resource_quantity("storage", "+1Gi"),
        Some(1_073_741_824),
        "Kubernetes quantities permit a leading plus sign"
    );
    assert_eq!(parse_resource_quantity("storage", "1k"), Some(1000));
    for raw in [
        "1GiB",
        "-1Gi",
        "1K",
        "",
        "1.2.3",
        "abc",
        "++1Gi",
        "1e-9223372036854775808",
        "1e3Gi",
    ] {
        assert_eq!(
            parse_resource_quantity("storage", raw),
            None,
            "storage quantity '{raw}' should be rejected"
        );
    }
    assert_eq!(
        parse_resource_quantity("storage", "18446744073709551616"),
        Some(i64::MAX),
        "oversized valid storage quantities should cap to the Kubernetes maximum"
    );
}

#[test]
fn test_resource_quota_scope_selector_matches_priority_class_and_cross_namespace_affinity() {
    let high_priority_quota = json!({
        "spec": {
            "scopeSelector": {
                "matchExpressions": [{
                    "scopeName": "PriorityClass",
                    "operator": "In",
                    "values": ["high"]
                }]
            }
        }
    });
    let high_pod = json!({"spec": {"priorityClassName": "high", "containers": []}});
    let low_pod = json!({"spec": {"priorityClassName": "low", "containers": []}});
    assert!(pod_matches_resource_quota_scopes(
        &high_pod,
        &high_priority_quota
    ));
    assert!(!pod_matches_resource_quota_scopes(
        &low_pod,
        &high_priority_quota
    ));

    let cross_namespace_quota = json!({
        "spec": {
            "scopeSelector": {
                "matchExpressions": [{
                    "scopeName": "CrossNamespacePodAffinity",
                    "operator": "Exists"
                }]
            }
        }
    });
    let cross_namespace_pod = json!({
        "spec": {
            "affinity": {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": {"app": "db"}},
                        "namespaces": ["shared"]
                    }]
                }
            },
            "containers": []
        }
    });
    let same_namespace_pod = json!({
        "spec": {
            "affinity": {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": {"app": "db"}}
                    }]
                }
            },
            "containers": []
        }
    });
    assert!(pod_matches_resource_quota_scopes(
        &cross_namespace_pod,
        &cross_namespace_quota
    ));
    assert!(!pod_matches_resource_quota_scopes(
        &same_namespace_pod,
        &cross_namespace_quota
    ));
}

#[tokio::test]
async fn test_reconcile_resource_quotas_updates_secret_count() {
    let db = crate::test_support::in_memory().await;

    // Create a ResourceQuota tracking secrets
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {"hard": {"secrets": "10"}},
            "status": {"hard": {"secrets": "10"}, "used": {"secrets": "0"}}
        }),
    )
    .await
    .unwrap();

    // Create 2 secrets
    for i in 0..2 {
        db.create_resource(
            "v1",
            "Secret",
            Some("default"),
            &format!("secret-{}", i),
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": format!("secret-{}", i), "namespace": "default"}
            }),
        )
        .await
        .unwrap();
    }

    // Reconcile
    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    // Check status.used.secrets = "2"
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    let used_secrets = rq
        .data
        .pointer("/status/used/secrets")
        .and_then(|v| v.as_str())
        .unwrap_or("missing");
    assert_eq!(
        used_secrets, "2",
        "status.used.secrets must be 2 after creating 2 secrets"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quotas_decrements_on_delete() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {"hard": {"secrets": "10"}},
            "status": {"hard": {"secrets": "10"}, "used": {"secrets": "0"}}
        }),
    )
    .await
    .unwrap();

    // Create then delete a secret
    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "to-delete",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "to-delete", "namespace": "default"}
        }),
    )
    .await
    .unwrap();

    db.delete_resource("v1", "Secret", Some("default"), "to-delete")
        .await
        .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    let used_secrets = rq
        .data
        .pointer("/status/used/secrets")
        .and_then(|v| v.as_str())
        .unwrap_or("missing");
    assert_eq!(
        used_secrets, "0",
        "status.used.secrets must be 0 after deleting the only secret"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quotas_no_quota_is_noop() {
    let db = crate::test_support::in_memory().await;
    // Should not panic/error when no ResourceQuota exists
    let result = reconcile_resource_quotas_with_runtime(&db, "default").await;
    assert!(result.is_ok());
}

#[test]
fn test_pod_is_terminating_uses_active_deadline_seconds() {
    let terminating = json!({
        "spec": {"activeDeadlineSeconds": 30},
        "metadata": {}
    });
    let not_terminating = json!({
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
        "metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}
    });
    assert!(pod_is_terminating(&terminating));
    assert!(
        !pod_is_terminating(&not_terminating),
        "deletionTimestamp alone should not satisfy Terminating scope"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_notterminating_tracks_pod_compute_usage() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "quota-not-terminating",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "quota-not-terminating", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi"
                },
                "scopes": ["NotTerminating"]
            },
            "status": {"hard": {}, "used": {}}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("200Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.cpu")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.memory")
            .and_then(|v| v.as_str()),
        Some("400Mi")
    );
}

/// P0-S13-4: status.used.pods must reflect pod count immediately after reconcile is called
/// following pod creation — verifies the core counting logic used by the pod-create HTTP path.
/// Mirrors K8s conformance test resource_quota.go:280 "should create a ResourceQuota and
/// capture the life of a pod".
#[tokio::test]
async fn test_reconcile_resource_quotas_pod_create_updates_used_pods_immediately() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    for i in 0..3u8 {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("pod-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": format!("pod-{i}"), "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await
        .unwrap();
    }

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("3"),
        "status.used.pods must equal 3 immediately after creating 3 pods"
    );
}

/// P0-S17-33 regression: unscoped ResourceQuota must account pod compute and extended
/// resource requests (including ephemeral-storage and custom requests.* keys).
#[tokio::test]
async fn test_reconcile_resource_quota_unscoped_pod_compute_and_extended_requests() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "requests.ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi",
                    "ephemeral-storage": "50Gi"
                }
            },
            "status": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "requests.ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi",
                    "ephemeral-storage": "50Gi"
                },
                "used": {
                    "pods": "0",
                    "requests.cpu": "0",
                    "requests.memory": "0",
                    "requests.ephemeral-storage": "0",
                    "requests.example.com/dongle": "0",
                    "limits.cpu": "0",
                    "limits.memory": "0",
                    "ephemeral-storage": "0"
                }
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "memory": "252Mi",
                            "ephemeral-storage": "30Gi",
                            "example.com/dongle": "2"
                        },
                        "limits": {
                            "cpu": "1",
                            "memory": "400Mi"
                        }
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.example.com~1dongle")
            .and_then(|v| v.as_str()),
        Some("2")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.cpu")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.memory")
            .and_then(|v| v.as_str()),
        Some("400Mi")
    );
}

/// P0-S18-32 regression: legacy `cpu`/`memory` hard keys must be populated from pod
/// requests in status.used, matching upstream resource_quota.go:280.
#[tokio::test]
async fn test_reconcile_resource_quota_unscoped_legacy_cpu_memory_keys() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "legacy-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "legacy-quota", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "cpu": "1",
                    "memory": "500Mi",
                    "ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "resourcequotas": "1"
                }
            },
            "status": {
                "hard": {
                    "pods": "5",
                    "cpu": "1",
                    "memory": "500Mi",
                    "ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "resourcequotas": "1"
                },
                "used": {
                    "pods": "0",
                    "cpu": "0",
                    "memory": "0",
                    "ephemeral-storage": "0",
                    "requests.example.com/dongle": "0",
                    "resourcequotas": "1"
                }
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "memory": "252Mi",
                            "ephemeral-storage": "30Gi",
                            "example.com/dongle": "2"
                        }
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "legacy-quota")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.example.com~1dongle")
            .and_then(|v| v.as_str()),
        Some("2")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/resourcequotas")
            .and_then(|v| v.as_str()),
        Some("1")
    );
}

/// P0-S18-33 regression: terminating-scoped quotas must count terminating pods and ignore
/// non-terminating pods for requests/limits accounting.
#[tokio::test]
async fn test_reconcile_resource_quota_terminating_scope_tracks_only_terminating_pods() {
    let db = crate::test_support::in_memory().await;

    for (name, scope) in [
        ("quota-terminating", "Terminating"),
        ("quota-not-terminating", "NotTerminating"),
    ] {
        db.create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {
                    "hard": {
                        "pods": "5",
                        "requests.cpu": "1",
                        "requests.memory": "500Mi",
                        "limits.cpu": "2",
                        "limits.memory": "1Gi"
                    },
                    "scopes": [scope]
                },
                "status": {"hard": {}, "used": {}}
            }),
        )
        .await
        .unwrap();
    }

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "long-running",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "long-running", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let term = db
        .get_resource("v1", "ResourceQuota", Some("default"), "quota-terminating")
        .await
        .unwrap()
        .unwrap();
    let not_term = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        term.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );

    db.delete_resource("v1", "Pod", Some("default"), "long-running")
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "terminating",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "terminating", "namespace": "default"},
            "spec": {
                "activeDeadlineSeconds": 3600,
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let term = db
        .get_resource("v1", "ResourceQuota", Some("default"), "quota-terminating")
        .await
        .unwrap()
        .unwrap();
    let not_term = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        term.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        term.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        term.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("200Mi")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("0")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0")
    );
}

/// P0-S13-4: status.used.pods must decrement immediately after pod deletion —
/// the background tokio::spawn that performs the actual pod removal MUST call
/// reconcile_resource_quotas_with_runtime after db.delete_resource.
/// Without this, status.used.pods stays inflated until the 30s periodic reconciler fires,
/// causing resource_quota.go:280 to time out at 300s.
#[tokio::test]
async fn test_reconcile_resource_quotas_pod_delete_decrements_used_pods_immediately() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    for i in 0..3u8 {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("pod-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": format!("pod-{i}"), "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await
        .unwrap();
    }

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    // Simulate the actual pod removal that the background spawn performs
    db.delete_resource("v1", "Pod", Some("default"), "pod-0")
        .await
        .unwrap();
    db.delete_resource("v1", "Pod", Some("default"), "pod-1")
        .await
        .unwrap();

    // The background spawn must call reconcile after delete_resource
    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1"),
        "status.used.pods must decrement to 1 immediately after deleting 2 of 3 pods"
    );
}

/// Regression test for P0-1: after a /status PATCH diverges Status.Hard from Spec.Hard,
/// calling reconcile must re-sync Status.Hard back to Spec.Hard.
/// This models the K8s conformance test "should apply changes to a resourcequota status".
#[tokio::test]
async fn test_reconcile_resets_status_hard_to_spec_hard_after_status_patch() {
    let db = crate::test_support::in_memory().await;

    // Create RQ with Spec.Hard = {pods: "5"}
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "e2e-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "e2e-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5"}},
            "status": {"hard": {"pods": "5"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    // Simulate /status PATCH: diverge Status.Hard to {pods: "10"} (different from Spec)
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    let mut patched: serde_json::Value = (*rq.data).clone();
    patched["status"]["hard"]["pods"] = json!("10");
    db.update_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "e2e-rq",
        patched,
        rq.resource_version,
    )
    .await
    .unwrap();

    // Verify divergence was stored
    let before = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before
            .data
            .pointer("/status/hard/pods")
            .and_then(|v| v.as_str()),
        Some("10"),
        "status.hard.pods should be 10 after status patch"
    );

    // Reconcile: should reset Status.Hard back to Spec.Hard = {pods: "5"}
    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let after = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/hard/pods")
            .and_then(|v| v.as_str()),
        Some("5"),
        "reconcile must reset status.hard.pods back to spec.hard.pods=5"
    );
}

/// Pods with deletionTimestamp set must not count against quota.
/// This mirrors upstream K8s where the quota controller excludes
/// terminating pods from status.used.
#[tokio::test]
async fn test_terminating_pod_excluded_from_pod_count() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1", "ResourceQuota", Some("default"), "test-rq",
        json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi"}},
            "status": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi"}, "used": {"pods": "0", "cpu": "0", "memory": "0"}}
        }),
    ).await.unwrap();

    // Active pod — should count
    db.create_resource(
        "v1", "Pod", Some("default"), "active-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "active-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "200m", "memory": "100Mi"}}}]}
        }),
    ).await.unwrap();

    // Terminating pod (deletionTimestamp set) — must NOT count
    db.create_resource(
        "v1", "Pod", Some("default"), "terminating-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "terminating-pod", "namespace": "default", "deletionTimestamp": "2026-01-01T00:00:00Z"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "300m", "memory": "200Mi"}}}]}
        }),
    ).await.unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1"),
        "terminating pod must be excluded from pod count"
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("200m"),
        "terminating pod CPU must be excluded"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("100Mi"),
        "terminating pod memory must be excluded"
    );
}

/// When a pod transitions from active to terminating (deletionTimestamp added),
/// a subsequent reconcile must release all its resources from quota.
#[tokio::test]
async fn test_pod_becomes_terminating_releases_quota() {
    let db = crate::test_support::in_memory().await;

    db.create_resource(
        "v1", "ResourceQuota", Some("default"), "test-rq",
        json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi", "ephemeral-storage": "50Gi"}},
            "status": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi", "ephemeral-storage": "50Gi"}, "used": {"pods": "0", "cpu": "0", "memory": "0", "ephemeral-storage": "0"}}
        }),
    ).await.unwrap();

    db.create_resource(
        "v1", "Pod", Some("default"), "test-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "500m", "memory": "252Mi", "ephemeral-storage": "30Gi"}}}]}
        }),
    ).await.unwrap();

    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );

    // Simulate API delete: set deletionTimestamp
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();
    let mut updated = (*pod.data).clone();
    updated["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        updated,
        pod.resource_version,
    )
    .await
    .unwrap();

    // Reconcile again (side effect fires after deletionTimestamp is set)
    reconcile_resource_quotas_with_runtime(&db, "default")
        .await
        .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0"),
        "pods must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("0"),
        "cpu must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("0"),
        "memory must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("0"),
        "ephemeral-storage must be 0 after pod becomes terminating"
    );
}

/// Unit test for pod_has_deletion_timestamp helper.
#[test]
fn test_pod_has_deletion_timestamp_helper() {
    let with_ts = json!({"metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}});
    let with_empty = json!({"metadata": {"deletionTimestamp": ""}});
    let without = json!({"metadata": {"name": "foo"}});
    let no_meta = json!({"spec": {}});

    assert!(pod_has_deletion_timestamp(&with_ts));
    assert!(!pod_has_deletion_timestamp(&with_empty));
    assert!(!pod_has_deletion_timestamp(&without));
    assert!(!pod_has_deletion_timestamp(&no_meta));
}
