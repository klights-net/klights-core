use std::net::SocketAddr;

use k8s_native_service::admission::{
    AdmissionDependencyError, AdmissionRequestContext, WebhookTarget,
};
use serde_json::{Value, json};

use crate::bootstrap::native_api_composition::support::{
    admission_namespace_labels, build_test_app_state, resolve_admission_webhook_target,
    run_admission,
};

async fn assembled_datastore_with_handle() -> (
    klights_cluster_datastore::test_support::ResourceTestStore,
    klights_cluster_datastore::test_support::ResourceTestStore,
) {
    let state = build_test_app_state().await;
    let db = state.resource_store();
    (db.clone(), db)
}

struct IntegrationAdmissionEngine {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
}

impl IntegrationAdmissionEngine {
    async fn run_mutating(
        &self,
        resource: &Value,
        api_version: &str,
        kind: &str,
        operation: &str,
    ) -> anyhow::Result<Value> {
        self.run_with_context(
            &AdmissionRequestContext::from_legacy(resource, api_version, kind, operation),
            true,
        )
        .await
    }

    async fn run_validating(
        &self,
        resource: &Value,
        api_version: &str,
        kind: &str,
        operation: &str,
    ) -> anyhow::Result<Value> {
        self.run_with_context(
            &AdmissionRequestContext::from_legacy(resource, api_version, kind, operation),
            false,
        )
        .await
    }

    async fn run_with_context(
        &self,
        context: &AdmissionRequestContext,
        is_mutating: bool,
    ) -> anyhow::Result<Value> {
        run_admission(self.db.clone(), context, is_mutating).await
    }
}

macro_rules! admission_engine_for_db_handle {
    ($engine:ident, $db_handle:expr) => {
        let $engine = IntegrationAdmissionEngine { db: $db_handle };
    };
}

async fn resolve_webhook_target_for_test(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    client_config: &Value,
) -> Result<WebhookTarget, AdmissionDependencyError> {
    resolve_admission_webhook_target(db, client_config).await
}

#[tokio::test]
async fn test_resolve_webhook_target_from_url_field() {
    let (_db, db_handle) = assembled_datastore_with_handle().await;
    let client_config = json!({"url": "https://webhook.example.com/validate"});

    let target = resolve_webhook_target_for_test(db_handle, &client_config)
        .await
        .unwrap();
    assert_eq!(target.base_url, "https://webhook.example.com/validate");
    assert_eq!(target.dns_override, None);
}

#[tokio::test]
async fn test_resolve_webhook_target_from_service_reference() {
    let (db, db_handle) = assembled_datastore_with_handle().await;

    // Create a Service in the DB
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "cert-manager"
        },
        "spec": {
            "clusterIP": "10.43.200.50",
            "ports": [{"port": 443}]
        }
    });
    db.create_resource(
        "v1",
        "Service",
        Some("cert-manager"),
        "webhook-service",
        service,
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("cert-manager"),
        "webhook-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "name": "webhook-service",
                "namespace": "cert-manager"
            },
            "subsets": [{
                "addresses": [{"ip": "10.42.1.10"}],
                "ports": [{"port": 443}]
            }]
        }),
    )
    .await
    .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "cert-manager",
            "path": "/validate"
        }
    });

    let target = resolve_webhook_target_for_test(db_handle, &client_config)
        .await
        .unwrap();
    assert_eq!(
        target.base_url,
        "https://webhook-service.cert-manager.svc:443/validate"
    );
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.cert-manager.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 200, 50), 443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_service_with_port_specified() {
    let (db, db_handle) = assembled_datastore_with_handle().await;

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": "10.43.128.100",
            "ports": [{"port": 8443}, {"port": 9443}]
        }
    });
    db.create_resource("v1", "Service", Some("default"), "webhook-service", service)
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "webhook-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "name": "webhook-service",
                "namespace": "default"
            },
            "subsets": [{
                "addresses": [{"ip": "10.42.1.20"}],
                "ports": [{"port": 9443}]
            }]
        }),
    )
    .await
    .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "default",
            "port": 9443
        }
    });

    let target = resolve_webhook_target_for_test(db_handle, &client_config)
        .await
        .unwrap();
    assert_eq!(target.base_url, "https://webhook-service.default.svc:9443");
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.default.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 128, 100), 9443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_leaves_target_port_translation_to_service_dataplane() {
    let (db, db_handle) = assembled_datastore_with_handle().await;

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": "10.43.128.100",
            "ports": [{"name":"https","port":443}]
        }
    });
    db.create_resource("v1", "Service", Some("default"), "webhook-service", service)
        .await
        .unwrap();

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "subsets": [{
            "addresses": [{"ip": "10.42.0.55"}],
            "ports": [{"name":"https","port":9443}]
        }]
    });
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "webhook-service",
        endpoints,
    )
    .await
    .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "default"
        }
    });

    let target = resolve_webhook_target_for_test(db_handle, &client_config)
        .await
        .unwrap();
    assert_eq!(target.base_url, "https://webhook-service.default.svc:443");
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.default.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 128, 100), 443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_keeps_remote_endpoint_behind_service_dataplane() {
    let (db, db_handle) = assembled_datastore_with_handle().await;

    db.create_resource(
        "v1",
        "Service",
        Some("webhook-7540"),
        "e2e-test-webhook",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "e2e-test-webhook",
                "namespace": "webhook-7540"
            },
            "spec": {
                "clusterIP": "10.43.128.100",
                "ports": [{"name": "https", "port": 8443, "targetPort": 8444}]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("webhook-7540"),
        "e2e-test-webhook-remote",
        json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": "e2e-test-webhook-remote",
                "namespace": "webhook-7540",
                "labels": {"kubernetes.io/service-name": "e2e-test-webhook"}
            },
            "ports": [{"name": "https", "port": 8444, "protocol": "TCP"}],
            "endpoints": [{
                "addresses": ["10.42.2.55"],
                "conditions": {"ready": true},
                "nodeName": "mn-replica"
            }]
        }),
    )
    .await
    .unwrap();

    let target = resolve_webhook_target_for_test(
        db_handle,
        &json!({
            "service": {
                "name": "e2e-test-webhook",
                "namespace": "webhook-7540",
                "path": "/always-allow-delay-5s",
                "port": 8443
            }
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        target.base_url,
        "https://e2e-test-webhook.webhook-7540.svc:8443/always-allow-delay-5s"
    );
    assert_eq!(
        target.dns_override,
        Some((
            "e2e-test-webhook.webhook-7540.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 128, 100), 8443)),
        )),
        "the apiserver must enter the Service dataplane; it must not pin the first remote Pod endpoint"
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_service_not_found_returns_error() {
    let (_db, db_handle) = assembled_datastore_with_handle().await;

    let client_config = json!({
        "service": {
            "name": "nonexistent",
            "namespace": "default"
        }
    });

    let result = resolve_webhook_target_for_test(db_handle, &client_config).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Service not found")
    );
}

#[tokio::test]
async fn test_get_namespace_labels_reads_namespace_table() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    db.create_namespace(
        "label-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "label-ns",
                "labels": {
                    "webhook-ready": "true",
                    "team": "platform"
                }
            }
        }),
    )
    .await
    .unwrap();

    let labels = admission_namespace_labels(db_handle, "label-ns").await;
    assert_eq!(
        labels.get("webhook-ready").map(String::as_str),
        Some("true")
    );
    assert_eq!(labels.get("team").map(String::as_str), Some("platform"));
}

#[tokio::test]
async fn test_admission_engine_shared_runner_no_webhooks_keeps_resource() {
    let (_db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let mutated = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(mutated, pod);

    let validated = engine
        .run_validating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(validated, pod);
}
#[tokio::test]
async fn test_engine_skips_non_write_operations_even_if_webhook_exists() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    // Matching webhook exists, but non-write ops must not trigger callout.
    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-skip-read-ops"},
        "webhooks": [{
            "name": "m.example.com",
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["*"],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-skip-read-ops",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let got = engine.run_mutating(&pod, "v1", "Pod", "GET").await.unwrap();
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_non_match_skips_namespaced_call() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "default", "labels": {"team": "a"}}
    });
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-ns-selector-skip"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-ns-selector-skip",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    // Selector mismatch must skip callout (no error despite unreachable webhook URL).
    let got = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_non_match_skips_failing_match_condition() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "default", "labels": {"team": "a"}}
    });
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-ns-selector-before-match-condition"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "matchConditions": [{
                "name": "would-error-if-evaluated",
                "expression": "request.doesNotExist.field == 'x'"
            }],
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-ns-selector-before-match-condition",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let got = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .expect("selector mismatch must skip matchConditions and callout");
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_ignored_for_cluster_scoped_request() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-cluster-scope"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["namespaces"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-cluster-scope",
        mwc,
    )
    .await
    .unwrap();

    let ns_obj = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "ns-a"}
    });

    // Cluster-scoped request: namespaceSelector must be ignored, so webhook call is attempted.
    let err = engine
        .run_mutating(&ns_obj, "v1", "Namespace", "CREATE")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Webhook call failed"));
}

#[tokio::test]
async fn test_dry_run_rejects_webhook_with_side_effects() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);
    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-dryrun-sideeffects"},
        "webhooks": [{
            "name": "m.example.com",
            "sideEffects": "Some",
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-dryrun-sideeffects",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let mut ctx = AdmissionRequestContext::from_legacy(&pod, "v1", "Pod", "CREATE");
    ctx.dry_run = Some(true);
    let err = engine
        .run_with_context(&ctx, true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("sideEffects does not allow dryRun"));
}

#[tokio::test]
async fn test_webhook_call_error_includes_timeout_query_parameter() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    let vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-timeout-query"},
        "webhooks": [{
            "name": "v.example.com",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/always-allow-delay-5s"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfiguration",
        None,
        "vwc-timeout-query",
        vwc,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm0", "namespace": "default"}
    });
    let err = engine
        .run_validating(&cm, "v1", "ConfigMap", "CREATE")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("/always-allow-delay-5s?timeout=1s"),
        "webhook errors must include timeout query parameter, got: {}",
        err
    );
}

#[tokio::test]
async fn test_webhook_configuration_objects_are_exempt_from_dynamic_admission() {
    let (db, db_handle) = assembled_datastore_with_handle().await;
    admission_engine_for_db_handle!(engine, db_handle);

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-block-webhook-config-create"},
        "webhooks": [{
            "name": "m.blocker.example.com",
            "failurePolicy": "Fail",
            "clientConfig": {"url": "https://127.0.0.1:1/block"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": ["admissionregistration.k8s.io"],
                "apiVersions": ["v1"],
                "resources": ["mutatingwebhookconfigurations", "validatingwebhookconfigurations"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-block-webhook-config-create",
        mwc,
    )
    .await
    .unwrap();

    let create_target = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "target-vwc"},
        "webhooks": []
    });
    let ctx = AdmissionRequestContext::from_legacy(
        &create_target,
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfiguration",
        "CREATE",
    );
    let got = engine.run_with_context(&ctx, true).await.unwrap();
    assert_eq!(
        got, create_target,
        "webhook configuration objects must bypass dynamic mutating admission"
    );
}
