//! Focused SQLite runtime-work fixture.

use std::sync::Arc;

#[derive(Clone)]
pub struct RuntimeWorkTestStore {
    store: Arc<crate::SqliteRuntimeWorkStore>,
}

impl RuntimeWorkTestStore {
    pub async fn open(
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let executor =
            crate::open::open_with_opts(crate::open::in_memory_opts(), supervisor, connection_key)
                .await?;
        Ok(Self {
            store: Arc::new(crate::SqliteRuntimeWorkStore::new(
                executor,
                Arc::new(klights_supervisor::SystemWallClock),
            )),
        })
    }

    pub fn pod_workqueue(&self) -> Arc<dyn klights_node_store::PodWorkqueueStore> {
        self.store.clone()
    }
}
