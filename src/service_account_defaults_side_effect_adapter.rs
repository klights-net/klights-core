use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::DatastoreHandle;
use klights_controllers::namespace;
use klights_controllers::side_effects::service_account_defaults::DefaultServiceAccountPort;

struct RootDefaultServiceAccountPort {
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[async_trait]
impl DefaultServiceAccountPort for RootDefaultServiceAccountPort {
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

pub(crate) fn port(
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn DefaultServiceAccountPort> {
    Arc::new(RootDefaultServiceAccountPort { db, identity })
}
