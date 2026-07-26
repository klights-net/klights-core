use klights_cluster_core::Resource;
use klights_reconcile_api::{
    GcOwnerIdentity, GcOwnerLifecyclePort, ReconcileSinkError, ReconcileSinkFuture,
};

pub(crate) fn reconcile_owner_references(
    gc: &dyn GcOwnerLifecyclePort,
    resource: Resource,
) -> ReconcileSinkFuture<'_> {
    gc.reconcile_owner_references(resource)
}

pub(crate) fn cascade_delete(
    gc: &dyn GcOwnerLifecyclePort,
    owner: GcOwnerIdentity,
) -> ReconcileSinkFuture<'_> {
    gc.cascade_delete(owner)
}

pub(crate) async fn sweep_dependents(
    gc: &dyn GcOwnerLifecyclePort,
    owner: GcOwnerIdentity,
) -> Result<bool, ReconcileSinkError> {
    gc.sweep_dependents(owner).await
}

pub(crate) async fn finalize_foreground_owner(
    gc: &dyn GcOwnerLifecyclePort,
    owner: Resource,
) -> Result<bool, ReconcileSinkError> {
    gc.finalize_foreground_owner(owner).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingGc {
        cascades: Mutex<Vec<GcOwnerIdentity>>,
    }

    impl GcOwnerLifecyclePort for RecordingGc {
        fn reconcile_owner_references(&self, _resource: Resource) -> ReconcileSinkFuture<'_> {
            Box::pin(async { Ok(()) })
        }

        fn cascade_delete(&self, owner: GcOwnerIdentity) -> ReconcileSinkFuture<'_> {
            self.cascades
                .lock()
                .expect("cascade lock poisoned")
                .push(owner);
            Box::pin(async { Ok(()) })
        }

        fn sweep_dependents(
            &self,
            _owner: GcOwnerIdentity,
        ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
            Box::pin(async { Ok(false) })
        }

        fn finalize_foreground_owner(
            &self,
            _owner: Resource,
        ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
            Box::pin(async { Ok(true) })
        }
    }

    #[tokio::test]
    async fn cascade_preserves_complete_owner_identity() {
        let gc = RecordingGc::default();
        let owner = GcOwnerIdentity::new(
            "apps/v1",
            "Deployment",
            Some("default".to_string()),
            "web",
            "deployment-uid",
        );

        cascade_delete(&gc, owner.clone()).await.expect("cascade");

        assert_eq!(
            gc.cascades
                .lock()
                .expect("cascade lock poisoned")
                .as_slice(),
            &[owner]
        );
    }

    #[tokio::test]
    async fn boolean_lifecycle_outcomes_are_not_erased() {
        let gc = RecordingGc::default();
        let owner = GcOwnerIdentity::new(
            "apps/v1",
            "Deployment",
            Some("default".to_string()),
            "web",
            "deployment-uid",
        );
        let resource = Resource::try_from_data(std::sync::Arc::new(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "deployment-uid",
                "resourceVersion": "7"
            }
        })))
        .expect("resource");

        assert!(!sweep_dependents(&gc, owner).await.expect("sweep"));
        assert!(
            finalize_foreground_owner(&gc, resource)
                .await
                .expect("finalize")
        );
    }
}
