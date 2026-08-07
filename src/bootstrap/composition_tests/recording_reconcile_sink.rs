//! Private reconciliation intent recorder for root composition tests.

#[derive(Default)]
pub(crate) struct RecordingControllerReconcileSink {
    keys: tokio::sync::Mutex<Vec<klights_reconcile_api::ReconcileKey>>,
}

impl RecordingControllerReconcileSink {
    async fn record(&self, keys: impl IntoIterator<Item = klights_reconcile_api::ReconcileKey>) {
        let mut recorded = self.keys.lock().await;
        for key in keys {
            if !recorded.contains(&key) {
                recorded.push(key);
            }
        }
    }

    pub(crate) async fn enqueue_key(&self, key: klights_reconcile_api::ReconcileKey) {
        self.record([key]).await;
    }

    pub(crate) async fn pending_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.keys.lock().await.clone()
    }
}

impl klights_reconcile_api::ControllerReconcileSink for RecordingControllerReconcileSink {
    fn enqueue_reconcile_batch(
        &self,
        keys: Vec<klights_reconcile_api::ReconcileKey>,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            if keys
                .iter()
                .any(|key| key.api_version() == "v1" && key.kind() == "Service")
            {
                return Err(klights_reconcile_api::ReconcileSinkError::unsupported_key(
                    "Service reconcile keys must use ServiceReconcileSink",
                ));
            }
            self.record(keys).await;
            Ok(())
        })
    }
}

impl klights_reconcile_api::ServiceReconcileSink for RecordingControllerReconcileSink {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<klights_reconcile_api::ServiceReconcileKey>,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.record(
                keys.into_iter()
                    .map(klights_reconcile_api::ServiceReconcileKey::into_reconcile_key),
            )
            .await;
            Ok(())
        })
    }
}

pub(crate) fn recording_reconcile_sink() -> RecordingControllerReconcileSink {
    RecordingControllerReconcileSink::default()
}
