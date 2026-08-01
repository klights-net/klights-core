use klights_controllers::apiservice::*;
use serde_json::{Value, json};

async fn evaluate_apiservice_status(
    db: &crate::datastore::sqlite::Datastore,
    apiservice: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Value> {
    let name = apiservice["metadata"]["name"]
        .as_str()
        .expect("APIService test fixture name");
    db.create_resource(
        "apiregistration.k8s.io/v1",
        "APIService",
        None,
        name,
        apiservice.clone(),
    )
    .await?;
    reconcile_apiservice(
        db as &dyn crate::datastore::DatastoreBackend,
        apiservice,
        now,
    )
    .await?;
    let current = db
        .get_resource("apiregistration.k8s.io/v1", "APIService", None, name)
        .await?
        .expect("APIService should remain present");
    Ok(current.data.get("status").cloned().unwrap_or(Value::Null))
}

#[tokio::test]
async fn apiservice_available_when_ready_endpointslice_exists_without_legacy_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "wardle-service",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "wardle-service", "namespace": "default"},
                "spec": {"ports": [{"name": "https", "port": 443, "targetPort": 8443, "protocol": "TCP"}]}
            }),
        )
        .await
        .unwrap();
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("default"),
        "wardle-service-abc",
        json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": "wardle-service-abc",
                "namespace": "default",
                "labels": {"kubernetes.io/service-name": "wardle-service"}
            },
            "addressType": "IPv4",
            "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}],
            "endpoints": [{"addresses": ["10.42.0.25"], "conditions": {"ready": true}}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "service": {"namespace": "default", "name": "wardle-service"}
        }
    });

    let status = evaluate_apiservice_status(&db, &apiservice, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(available_condition_status(&status), Some("True"));
}

#[tokio::test]
async fn apiservice_unavailable_when_endpointslice_has_no_ready_addresses() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "wardle-service",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "wardle-service", "namespace": "default"},
                "spec": {"ports": [{"name": "https", "port": 443, "targetPort": 8443, "protocol": "TCP"}]}
            }),
        )
        .await
        .unwrap();
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("default"),
        "wardle-service-empty",
        json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": "wardle-service-empty",
                "namespace": "default",
                "labels": {"kubernetes.io/service-name": "wardle-service"}
            },
            "addressType": "IPv4",
            "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}],
            "endpoints": [{"addresses": ["10.42.0.25"], "conditions": {"ready": false}}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {"service": {"namespace": "default", "name": "wardle-service"}}
    });

    let status = evaluate_apiservice_status(&db, &apiservice, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(available_condition_status(&status), Some("False"));
    assert_eq!(
        available_condition_reason(&status),
        Some("MissingEndpoints")
    );
}

fn available_condition_status(status: &Value) -> Option<&str> {
    status
        .pointer("/conditions")
        .and_then(Value::as_array)?
        .iter()
        .find(|condition| condition.pointer("/type").and_then(Value::as_str) == Some("Available"))
        .and_then(|condition| condition.pointer("/status").and_then(Value::as_str))
}

fn available_condition_reason(status: &Value) -> Option<&str> {
    status
        .pointer("/conditions")
        .and_then(Value::as_array)?
        .iter()
        .find(|condition| condition.pointer("/type").and_then(Value::as_str) == Some("Available"))
        .and_then(|condition| condition.pointer("/reason").and_then(Value::as_str))
}
