//! Event-driven maintenance for namespace default ServiceAccounts.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait DefaultServiceAccountPort: Send + Sync {
    async fn ensure_default_service_account(&self, namespace: &str) -> Result<()>;
}

pub async fn apply_default_service_account_delete<Port: DefaultServiceAccountPort + ?Sized>(
    resource: &Value,
    port: &Port,
) -> Result<()> {
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if name != "default" {
        return Ok(());
    }
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if namespace.is_empty() {
        return Ok(());
    }
    port.ensure_default_service_account(namespace).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeDefaultServiceAccountPort {
        namespaces: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl DefaultServiceAccountPort for FakeDefaultServiceAccountPort {
        async fn ensure_default_service_account(&self, namespace: &str) -> Result<()> {
            self.namespaces.lock().unwrap().push(namespace.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn deleting_default_service_account_recreates_its_namespace_default() {
        let port = FakeDefaultServiceAccountPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_default_service_account_delete(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"namespace": "work", "name": "default"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert_eq!(*port.namespaces.lock().unwrap(), vec!["work"]);
    }

    #[tokio::test]
    async fn deleting_non_default_service_account_does_nothing() {
        let port = FakeDefaultServiceAccountPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_default_service_account_delete(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"namespace": "work", "name": "builder"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert!(port.namespaces.lock().unwrap().is_empty());
    }
}
