//! Event-driven maintenance for namespace default ServiceAccounts.

use super::SideEffect;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

pub struct DefaultServiceAccountEffect;

#[async_trait]
impl SideEffect for DefaultServiceAccountEffect {
    fn name(&self) -> &'static str {
        "default_serviceaccount"
    }

    async fn apply(&self, _resource: &Value, _db: &dyn DatastoreBackend) -> Result<()> {
        Ok(())
    }

    async fn apply_delete(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
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
        crate::controllers::namespace::reconcile_default_service_account(db, namespace).await
    }
}

pub fn default_serviceaccount() -> std::sync::Arc<dyn SideEffect> {
    std::sync::Arc::new(DefaultServiceAccountEffect)
}
