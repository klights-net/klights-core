use std::sync::Arc;

use anyhow::Result;
use klights_kubelet::probe_manager::{ProbeManager, ProbeType};

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::{PodReader, PodRepository, PodStatusWriter};

fn probe_manager_for_test(db_handle: &DatastoreHandle) -> ProbeManager {
    let (lifecycle_tx, _rx) = tokio::sync::mpsc::channel(1);
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
    let pod_reader: Arc<dyn klights_pod_api::PodQuery> = Arc::new(PodRepository::new(
        db_handle.clone(),
        supervisor.clone(),
        side_effects,
        metrics,
    ));
    ProbeManager::new_with_lifecycle(
        supervisor,
        pod_reader,
        Some(Arc::new(
            klights_kubelet::runtime::test_support::MockCriRuntime::new(),
        )),
        lifecycle_tx,
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
    )
}

struct PodConditionProbeUpdate<'a> {
    namespace: &'a str,
    name: &'a str,
    pod_uid: &'a str,
    container_name: &'a str,
    probe_type: ProbeType,
    success: bool,
}

async fn update_pod_condition(
    db_handle: &DatastoreHandle,
    pod_repo: &Arc<PodRepository>,
    pod_key: &str,
    container_name: &str,
    probe_type: ProbeType,
    success: bool,
) -> Result<()> {
    update_pod_condition_with_supervisor(
        db_handle,
        pod_repo,
        pod_key,
        container_name,
        probe_type,
        success,
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
    )
    .await
}

async fn update_pod_condition_for_uid(
    db_handle: &DatastoreHandle,
    pod_repo: &Arc<PodRepository>,
    update: PodConditionProbeUpdate<'_>,
) -> Result<()> {
    update_pod_condition_for_uid_with_supervisor(
        db_handle,
        pod_repo,
        update,
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
    )
    .await
}

async fn update_pod_condition_with_supervisor(
    db_handle: &DatastoreHandle,
    pod_repo: &Arc<PodRepository>,
    pod_key: &str,
    container_name: &str,
    probe_type: ProbeType,
    success: bool,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<()> {
    let parts: Vec<&str> = pod_key.split('/').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid pod key: {pod_key}");
    }
    let namespace = parts[0];
    let name = parts[1];
    let Some(pod_resource) = pod_repo.get_pod(namespace, name).await? else {
        return Ok(());
    };

    update_pod_condition_for_uid_with_supervisor(
        db_handle,
        pod_repo,
        PodConditionProbeUpdate {
            namespace,
            name,
            pod_uid: &pod_resource.uid,
            container_name,
            probe_type,
            success,
        },
        task_supervisor,
    )
    .await
}

async fn update_pod_condition_for_uid_with_supervisor(
    _db_handle: &DatastoreHandle,
    pod_repo: &Arc<PodRepository>,
    update: PodConditionProbeUpdate<'_>,
    _task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<()> {
    let PodConditionProbeUpdate {
        namespace,
        name,
        pod_uid,
        container_name,
        probe_type,
        success,
    } = update;
    if !matches!(probe_type, ProbeType::Readiness) {
        return Ok(());
    }

    let pod_resource = match pod_repo.get_pod_for_uid(namespace, name, pod_uid).await? {
        Some(pod) => pod,
        None => return Ok(()),
    };
    pod_repo
        .set_probe_readiness_for_uid(
            namespace,
            name,
            &pod_resource.uid,
            container_name,
            success,
            None,
        )
        .await?;
    Ok(())
}

#[path = "probe_manager_integration_tests.rs"]
mod tests;
