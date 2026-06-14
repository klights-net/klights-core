//! Test-only Pod→Service endpoint reconciliation driver.
//!
//! Production no longer calls this directly. The leader's outbox apply path
//! (`replication::grpc::server::enqueue_forwarded_pod_status_effects`) and the
//! in-process `pod_repository` side-effect path
//! (`side_effects::service_pod::enqueue_services_after_pod_update`) own
//! Pod→Service reconcile end-to-end. Letting kubelet code call this function
//! again would re-introduce the multinode bug where workers (which have no
//! cluster.db write surface) blew up trying to write Endpoints locally, and
//! would also violate the one-way side-effect edge: Endpoints and EndpointSlice
//! side effects must not enqueue Service reconcile back.

#[cfg(test)]
use crate::datastore::DatastoreBackend;
#[cfg(test)]
use crate::kubelet::pod_repository::PodReader;
#[cfg(test)]
use anyhow::Result;

#[cfg(test)]
pub async fn reconcile_endpoints_for_pod(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod: &serde_json::Value,
    service_router: Option<&dyn crate::networking::ServiceRouter>,
) -> Result<()> {
    // Extract pod metadata
    let metadata = pod
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata"))?;
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing namespace"))?;
    let pod_labels = metadata.get("labels").and_then(|l| l.as_object());

    // If pod has no labels, no services can select it
    if pod_labels.is_none() {
        return Ok(());
    }

    let pod_labels = pod_labels.unwrap();

    // List all services in the same namespace
    let services = db
        .list_resources(
            "v1",
            "Service",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    // For each service, check if its selector matches this pod's labels
    for service_resource in services.items {
        let service_data = &service_resource.data;

        if let Some(spec) = service_data.get("spec")
            && let Some(selector) = spec.get("selector")
        {
            // Check if selector matches pod labels
            let selector_obj = selector.as_object();
            if selector_obj.is_none() {
                continue;
            }

            let selector_obj = selector_obj.unwrap();
            let matches = selector_obj
                .iter()
                .all(|(k, v)| pod_labels.get(k) == Some(v));

            if matches {
                // This service selects this pod - reconcile its endpoints
                let service_name = service_data
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Service missing name"))?;
                let service_uid = service_data
                    .get("metadata")
                    .and_then(|m| m.get("uid"))
                    .and_then(|u| u.as_str())
                    .unwrap_or("");

                tracing::debug!(
                    "Pod {} matches service {} selector, reconciling endpoints",
                    metadata
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown"),
                    service_name
                );

                // Check publishNotReadyAddresses on the service
                let publish_not_ready = spec
                    .get("publishNotReadyAddresses")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                    || service_data
                        .get("metadata")
                        .and_then(|m| m.get("annotations"))
                        .and_then(|a| {
                            a.get("service.alpha.kubernetes.io/tolerate-unready-endpoints")
                        })
                        .and_then(|v| v.as_str())
                        .map(|v| v == "true")
                        .unwrap_or(false);

                crate::controllers::endpoints::reconcile_endpoints(
                    db,
                    pod_reader,
                    service_name,
                    namespace,
                    Some(selector),
                    spec.get("ports"),
                    publish_not_ready,
                )
                .await?;

                // Also update EndpointSlice
                crate::controllers::endpoints::reconcile_endpointslice(
                    db,
                    pod_reader,
                    service_name,
                    service_uid,
                    namespace,
                    Some(selector),
                    spec.get("ports"),
                )
                .await?;
            }
        }
    }

    // Trigger coalesced nft sync for all services after endpoints update.
    // Ensures NodePort and ClusterIP DNAT rules converge once endpoints
    // are available.
    if let Some(router) = service_router {
        router.request_services_sync();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_kubelet_production_caller_of_reconcile_endpoints_for_pod() {
        // R4: invariant now enforced by check_kubelet_invariants.sh + check_networking_invariants.sh
    }

    #[tokio::test]
    #[ignore = "Requires root for nftables/netlink"]
    async fn test_reconcile_endpoints_for_pod_matches_service() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create namespace
        let ns = serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
        db.create_resource("v1", "Namespace", None, "test", ns)
            .await
            .unwrap();

        // Create service with selector
        let service = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "nginx-svc",
                "namespace": "test"
            },
            "spec": {
                "selector": {"app": "nginx"},
                "ports": [{"port": 80, "targetPort": 8080}]
            }
        });
        db.create_resource("v1", "Service", Some("test"), "nginx-svc", service)
            .await
            .unwrap();

        // Create empty endpoints (simulating service creation)
        let empty_endpoints = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "name": "nginx-svc",
                "namespace": "test"
            },
            "subsets": []
        });
        db.create_resource(
            "v1",
            "Endpoints",
            Some("test"),
            "nginx-svc",
            empty_endpoints,
        )
        .await
        .unwrap();

        // Create pod with matching labels and podIP
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "nginx-1",
                "namespace": "test",
                "labels": {"app": "nginx"}
            },
            "status": {"podIP": "10.42.0.5"}
        });
        db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod.clone())
            .await
            .unwrap();

        // Call reconcile_endpoints_for_pod (simulating pod watcher event)
        reconcile_endpoints_for_pod(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pod,
            None,
        )
        .await
        .unwrap();

        // Verify endpoints were updated with pod IP
        let endpoints = db
            .get_resource("v1", "Endpoints", Some("test"), "nginx-svc")
            .await
            .unwrap()
            .unwrap();
        let subsets = endpoints.data["subsets"].as_array().unwrap();
        assert_eq!(subsets.len(), 1, "Should have 1 subset");

        let addresses = subsets[0]["addresses"].as_array().unwrap();
        assert_eq!(addresses.len(), 1, "Should have 1 address");
        assert_eq!(addresses[0]["ip"], "10.42.0.5");
    }
}
