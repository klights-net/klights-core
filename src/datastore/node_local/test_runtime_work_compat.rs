//! Test-only compatibility DTOs retained for legacy root regression coverage.

#[cfg(any(test, feature = "pod-repository-test-support"))]
use serde_json::Value;

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodWorkqueueKind {
    Pod,
    Namespace,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone, Debug, PartialEq)]
pub struct PodWorkqueueEntry {
    pub id: i64,
    pub kind: PodWorkqueueKind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub payload: Value,
    pub attempt_count: i64,
    pub next_attempt_at_ms: i64,
}

#[cfg(test)]
pub use super::sqlite::DeadLetterTestInsert;
#[cfg(test)]
pub use super::sqlite::{DeadLetterRow, OutboxInsert, OutboxRow};

impl super::NodeLocalStores {
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        let identity = match kind {
            PodWorkqueueKind::Pod => klights_node_store::PodWorkIdentity::try_pod(pod.clone())?,
            PodWorkqueueKind::Namespace => {
                klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
            }
        };
        let entry = klights_node_store::PodWorkqueueEnqueue::try_new(
            identity,
            serde_json::to_vec(&payload)?,
            attempt_count,
            min_delay_ms,
            last_error.map(str::to_string),
        )?;
        self.pod_workqueue()
            .enqueue_work(entry)
            .await
            .map_err(anyhow::Error::from)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn peek_workqueue_next_due(&self) -> anyhow::Result<Option<i64>> {
        self.pod_workqueue()
            .peek_next_due_ms()
            .await
            .map_err(anyhow::Error::from)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn claim_workqueue_due(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<Option<PodWorkqueueEntry>> {
        let row = self
            .pod_workqueue()
            .claim_due_work(klights_node_store::DueTimeMs::try_new(now_ms)?)
            .await?;
        row.map(|row| {
            let (id, identity, payload, attempt_count, next_due_ms) = row.into_parts();
            let (kind, pod) = identity.into_persisted();
            Ok(PodWorkqueueEntry {
                id: id.get(),
                kind: match kind {
                    klights_node_store::PodWorkqueueKind::Pod => PodWorkqueueKind::Pod,
                    klights_node_store::PodWorkqueueKind::Namespace => PodWorkqueueKind::Namespace,
                },
                namespace: pod.namespace,
                name: pod.name,
                uid: pod.uid,
                payload: serde_json::from_slice(&payload)?,
                attempt_count,
                next_attempt_at_ms: next_due_ms.get(),
            })
        })
        .transpose()
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn complete_workqueue(&self, _id: i64) -> anyhow::Result<()> {
        Ok(())
    }
}
