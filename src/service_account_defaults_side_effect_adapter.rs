use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::controllers::namespace;
use crate::datastore::DatastoreHandle;
use crate::side_effects::service_account_defaults::{
    DefaultServiceAccountPort, apply_default_service_account_delete,
};
use klights_controllers::side_effects::SideEffect;

struct DefaultServiceAccountEffect {
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[async_trait]
impl SideEffect for DefaultServiceAccountEffect {
    fn name(&self) -> &'static str {
        "default_serviceaccount"
    }

    async fn apply(&self, _resource: &Value) -> Result<()> {
        Ok(())
    }

    async fn apply_delete(&self, resource: &Value) -> Result<()> {
        apply_default_service_account_delete(resource, self).await
    }
}

#[async_trait]
impl DefaultServiceAccountPort for DefaultServiceAccountEffect {
    async fn ensure_default_service_account(&self, namespace: &str) -> Result<()> {
        namespace::reconcile_default_service_account_at(
            self.db.as_ref(),
            namespace,
            chrono::Utc::now(),
            self.identity.as_ref(),
        )
        .await
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn SideEffect> {
    Arc::new(DefaultServiceAccountEffect { db, identity })
}
