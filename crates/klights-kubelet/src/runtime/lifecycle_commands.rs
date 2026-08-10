use crate::lifecycle::LifecycleCommand;
use crate::pod_sandbox_config::build_sandbox_config_with_runtime_inputs;
use crate::pod_termination::find_pod_container_spec;
use crate::runtime::pod_identity::get_pod_for_uid;
use crate::runtime::service::{ContainerConfigBuildRequest, RealPodRuntimeService};
use crate::runtime::status_helpers::{
    pod_status_container_id_by_name, pod_status_ip, replace_container_status,
    restart_last_state_from_runtime_status, restarted_running_container_status,
};
use crate::runtime_types::PodRuntimeKey;

pub(super) async fn handle_lifecycle_command(
    service: &RealPodRuntimeService,
    command: LifecycleCommand,
) -> anyhow::Result<()> {
    match &command {
        LifecycleCommand::ReadinessChanged {
            pod_uid,
            namespace,
            pod_name,
            container_name,
            ready,
        } => {
            service
                .handle_readiness_changed(namespace, pod_name, pod_uid, container_name, *ready)
                .await?;
        }
        LifecycleCommand::RestartRequested {
            pod_uid,
            namespace,
            pod_name,
            container_name,
            reason,
        } => {
            let operation_now = service.clock.now_utc();
            tracing::info!(
                namespace = namespace,
                pod = pod_name,
                uid = pod_uid,
                container = container_name,
                reason = format!("{:?}", reason),
                "restart requested"
            );
            let key = PodRuntimeKey::new(namespace, pod_name, pod_uid);
            let Some(pod_resource) =
                get_pod_for_uid(service.pod_query.as_ref(), namespace, pod_name, pod_uid).await?
            else {
                return Ok(());
            };
            let pod = pod_resource.data.as_ref().clone();
            let Some(sandbox_id) = service.store.get_sandbox_id(&key).await? else {
                tracing::warn!(
                    namespace = namespace,
                    pod = pod_name,
                    uid = pod_uid,
                    "restart requested but sandbox id is missing"
                );
                return Ok(());
            };

            let observed_container_status = pod
                .pointer("/status/containerStatuses")
                .and_then(|value| value.as_array())
                .and_then(|statuses| {
                    statuses
                        .iter()
                        .find(|status| {
                            status.get("name").and_then(|value| value.as_str())
                                == Some(container_name.as_str())
                        })
                        .cloned()
                });
            let mut old_container_id = pod_status_container_id_by_name(&pod, container_name);
            if old_container_id.is_none() {
                let containers = service
                    .container_control
                    .list_containers(Some(&sandbox_id))
                    .await?;
                for (candidate_id, _state) in containers {
                    let runtime_name = service
                        .cri
                        .container_status(&candidate_id)
                        .await?
                        .status
                        .and_then(|status| status.metadata.map(|metadata| metadata.name))
                        .filter(|name| !name.is_empty());
                    if runtime_name.as_deref() == Some(container_name.as_str()) {
                        old_container_id = Some(candidate_id);
                        break;
                    }
                }
            }
            let Some(old_container_id) = old_container_id else {
                tracing::warn!(
                    namespace = namespace,
                    pod = pod_name,
                    uid = pod_uid,
                    container = container_name,
                    "restart requested but runtime container id is missing"
                );
                return Ok(());
            };

            service.cri.stop_container(&old_container_id, 10).await?;
            let stopped_status = service
                .cri
                .container_status(&old_container_id)
                .await?
                .status;
            let last_state =
                restart_last_state_from_runtime_status(stopped_status.as_ref(), operation_now);
            let _ = service
                .pod_status_writer
                .note_container_restart_for_uid(
                    namespace,
                    pod_name,
                    pod_uid,
                    container_name,
                    last_state.clone(),
                    None,
                )
                .await;
            service.cri.remove_container(&old_container_id).await?;

            let volume_paths = service.volumes.process_volumes(&key, &pod).await?;
            if pod
                .pointer("/spec/securityContext/fsGroup")
                .and_then(|v| v.as_u64())
                .is_some()
            {
                let _ = service.filesystem.apply_fs_group(&key, &pod).await;
            }

            let Some(container) = find_pod_container_spec(&pod, container_name) else {
                tracing::warn!(
                    namespace = namespace,
                    pod = pod_name,
                    uid = pod_uid,
                    container = container_name,
                    "restart requested but container spec is missing"
                );
                return Ok(());
            };
            let dns_ip = klights_types::dns_service_ipv4(&service.config.service_cidr);
            let kubernetes_service_ip =
                klights_types::first_usable_ipv4(&service.config.service_cidr);
            let container_config = service
                .build_container_config_with_env(ContainerConfigBuildRequest {
                    key: &key,
                    pod: &pod,
                    container,
                    container_name,
                    kubernetes_service_ip: &kubernetes_service_ip,
                    volume_paths: &volume_paths,
                    ignore_mount_errors: false,
                })
                .await?;
            let default_spec = serde_json::json!({});
            let pod_spec = pod.get("spec").unwrap_or(&default_spec);
            let sandbox_config = build_sandbox_config_with_runtime_inputs(
                crate::pod_sandbox_config::SandboxIdentity {
                    pod_name,
                    namespace,
                    pod_uid,
                    containerd_namespace: &service.config.containerd_namespace,
                },
                pod_status_ip(&pod),
                &dns_ip,
                pod_spec,
                &service.config.sandbox_inputs,
                &service.config.paths,
            );

            let new_container_id = service
                .cri
                .create_container(container_config, &sandbox_id, sandbox_config)
                .await?;
            service.cri.start_container(&new_container_id).await?;
            if let Some(observed_status) = observed_container_status.as_ref() {
                let mut container_statuses = pod
                    .pointer("/status/containerStatuses")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default();
                if let Some(replacement) = restarted_running_container_status(
                    &pod,
                    container_name,
                    &new_container_id,
                    observed_status,
                    &last_state,
                    operation_now,
                ) {
                    replace_container_status(&mut container_statuses, container_name, replacement);
                    let mut status = serde_json::json!({
                        "phase": "Running",
                        "containerStatuses": container_statuses,
                    });
                    klights_types::merge_pod_status_for_update(
                        "v1",
                        "Pod",
                        &pod,
                        &mut status,
                        klights_types::PodStatusOwner::KubeletRuntime,
                    );
                    service.write_pod_status(&key, status).await?;
                }
            }
        }
        LifecycleCommand::StartupPassed {
            pod_uid,
            namespace,
            pod_name,
            container_name,
        } => {
            tracing::info!(
                namespace = namespace,
                pod = pod_name,
                uid = pod_uid,
                container = container_name,
                "startup probe passed"
            );
            // Startup passed: the container is now ready for liveness probes.
            // The probe manager handles this transition internally.
        }
    }
    Ok(())
}
