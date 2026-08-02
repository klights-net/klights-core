//! Side effect to recount ResourceQuota after namespaced resource mutations.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait ResourceQuotaSideEffectPort: Send + Sync {
    async fn recount_namespace(&self, namespace: &str) -> Result<()>;
}

pub async fn apply_resource_quota_event<Port: ResourceQuotaSideEffectPort + ?Sized>(
    resource: &Value,
    port: &Port,
) -> Result<()> {
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if namespace.is_empty() {
        return Ok(());
    }

    let kind = resource
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    tracing::info!(
        kind = %kind,
        name = %name,
        namespace = %namespace,
        "ResourceQuotaEffect firing"
    );
    port.recount_namespace(namespace).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeResourceQuotaPort {
        namespaces: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl ResourceQuotaSideEffectPort for FakeResourceQuotaPort {
        async fn recount_namespace(&self, namespace: &str) -> anyhow::Result<()> {
            self.namespaces.lock().unwrap().push(namespace.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn namespaced_event_recounts_exact_namespace_through_port() {
        let port = FakeResourceQuotaPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_resource_quota_event(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "work", "name": "settings"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert_eq!(*port.namespaces.lock().unwrap(), vec!["work"]);
    }

    #[tokio::test]
    async fn cluster_scoped_event_does_not_recount() {
        let port = FakeResourceQuotaPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_resource_quota_event(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert!(port.namespaces.lock().unwrap().is_empty());
    }
}
