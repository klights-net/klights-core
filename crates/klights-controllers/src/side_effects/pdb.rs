//! Side effect to update PDB status after Pod mutations.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait PdbSideEffectPort: Send + Sync {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()>;
}

pub fn pdb_event_namespace(resource: &Value) -> Option<&str> {
    resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .filter(|namespace| !namespace.is_empty())
}

pub async fn apply_pdb_event<Port: PdbSideEffectPort + ?Sized>(
    resource: &Value,
    port: &Port,
) -> Result<()> {
    let Some(namespace) = pdb_event_namespace(resource) else {
        return Ok(());
    };
    port.reconcile_namespace(namespace).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakePdbPort {
        namespaces: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl PdbSideEffectPort for FakePdbPort {
        async fn reconcile_namespace(&self, namespace: &str) -> anyhow::Result<()> {
            self.namespaces.lock().unwrap().push(namespace.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn namespaced_pod_event_reconciles_exact_namespace() {
        let port = FakePdbPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_pdb_event(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "work", "name": "web"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert_eq!(*port.namespaces.lock().unwrap(), vec!["work"]);
    }

    #[tokio::test]
    async fn namespace_less_event_does_not_reconcile() {
        let port = FakePdbPort {
            namespaces: Mutex::new(Vec::new()),
        };
        apply_pdb_event(
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "web"}
            }),
            &port,
        )
        .await
        .unwrap();
        assert!(port.namespaces.lock().unwrap().is_empty());
    }
}
