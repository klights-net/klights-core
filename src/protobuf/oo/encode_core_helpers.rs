use crate::protobuf::*;
pub fn json_pdb_to_pb(
    pdb: &k8s_openapi::api::policy::v1::PodDisruptionBudget,
) -> k8s_pb::api::policy::v1::PodDisruptionBudget {
    k8s_pb::api::policy::v1::PodDisruptionBudget {
        metadata: Some(json_meta_to_pb(&pdb.metadata)),
        spec: pdb
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::policy::v1::PodDisruptionBudgetSpec {
                min_available: spec.min_available.as_ref().map(openapi_intorstring_to_pb),
                max_unavailable: spec.max_unavailable.as_ref().map(openapi_intorstring_to_pb),
                selector: spec.selector.as_ref().map(json_label_selector_to_pb),
                unhealthy_pod_eviction_policy: spec.unhealthy_pod_eviction_policy.clone(),
            }),
        status: pdb.status.as_ref().map(|status| {
            k8s_pb::api::policy::v1::PodDisruptionBudgetStatus {
                observed_generation: status.observed_generation,
                disruptions_allowed: Some(status.disruptions_allowed),
                current_healthy: Some(status.current_healthy),
                desired_healthy: Some(status.desired_healthy),
                expected_pods: Some(status.expected_pods),
                disrupted_pods: std::collections::BTreeMap::new(),
                conditions: vec![],
            }
        }),
    }
}

pub fn json_cronjob_to_pb(
    cj: &k8s_openapi::api::batch::v1::CronJob,
) -> k8s_pb::api::batch::v1::CronJob {
    k8s_pb::api::batch::v1::CronJob {
        metadata: Some(json_meta_to_pb(&cj.metadata)),
        spec: cj.spec.as_ref().map(|spec| {
            k8s_pb::api::batch::v1::CronJobSpec {
                schedule: Some(spec.schedule.clone()),
                starting_deadline_seconds: spec.starting_deadline_seconds,
                concurrency_policy: spec.concurrency_policy.clone(),
                suspend: spec.suspend,
                successful_jobs_history_limit: spec.successful_jobs_history_limit,
                failed_jobs_history_limit: spec.failed_jobs_history_limit,
                time_zone: spec.time_zone.clone(),
                job_template: Some(k8s_pb::api::batch::v1::JobTemplateSpec {
                    metadata: spec
                        .job_template
                        .metadata
                        .as_ref()
                        .map(json_meta_to_pb_from_obj),
                    spec: None, // Job spec encoding is complex, leave empty for now
                }),
            }
        }),
        status: cj
            .status
            .as_ref()
            .map(|status| k8s_pb::api::batch::v1::CronJobStatus {
                active: status
                    .active
                    .as_ref()
                    .map(|refs| refs.iter().map(json_obj_ref_to_pb).collect())
                    .unwrap_or_default(),
                last_schedule_time: status.last_schedule_time.as_ref().map(json_time_to_pb),
                last_successful_time: status.last_successful_time.as_ref().map(json_time_to_pb),
            }),
    }
}

pub fn json_storageclass_to_pb(
    sc: &k8s_openapi::api::storage::v1::StorageClass,
) -> k8s_pb::api::storage::v1::StorageClass {
    k8s_pb::api::storage::v1::StorageClass {
        metadata: Some(json_meta_to_pb(&sc.metadata)),
        provisioner: Some(sc.provisioner.clone()),
        parameters: sc
            .parameters
            .as_ref()
            .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        reclaim_policy: sc.reclaim_policy.clone(),
        mount_options: sc.mount_options.clone().unwrap_or_default(),
        allow_volume_expansion: sc.allow_volume_expansion,
        volume_binding_mode: sc.volume_binding_mode.clone(),
        allowed_topologies: vec![], // TopologySelectorTerm is complex, omit for now
    }
}

pub fn json_csistoragecapacity_to_pb(
    cap: &k8s_openapi::api::storage::v1::CSIStorageCapacity,
) -> k8s_pb::api::storage::v1::CSIStorageCapacity {
    k8s_pb::api::storage::v1::CSIStorageCapacity {
        metadata: Some(json_meta_to_pb(&cap.metadata)),
        storage_class_name: Some(cap.storage_class_name.clone()),
        capacity: cap.capacity.as_ref().map(|q| {
            k8s_pb::apimachinery::pkg::api::resource::Quantity {
                string: Some(q.0.clone()),
            }
        }),
        maximum_volume_size: cap.maximum_volume_size.as_ref().map(|q| {
            k8s_pb::apimachinery::pkg::api::resource::Quantity {
                string: Some(q.0.clone()),
            }
        }),
        node_topology: cap.node_topology.as_ref().map(json_label_selector_to_pb),
    }
}

pub fn json_csinode_to_pb(
    node: &k8s_openapi::api::storage::v1::CSINode,
) -> k8s_pb::api::storage::v1::CSINode {
    k8s_pb::api::storage::v1::CSINode {
        metadata: Some(json_meta_to_pb(&node.metadata)),
        spec: Some(k8s_pb::api::storage::v1::CSINodeSpec {
            drivers: node
                .spec
                .drivers
                .iter()
                .map(|d| k8s_pb::api::storage::v1::CSINodeDriver {
                    name: Some(d.name.clone()),
                    node_id: Some(d.node_id.clone()),
                    topology_keys: d.topology_keys.clone().unwrap_or_default(),
                    allocatable: None,
                })
                .collect(),
        }),
    }
}

pub fn json_csidriver_to_pb(
    driver: &k8s_openapi::api::storage::v1::CSIDriver,
) -> k8s_pb::api::storage::v1::CSIDriver {
    k8s_pb::api::storage::v1::CSIDriver {
        metadata: Some(json_meta_to_pb(&driver.metadata)),
        spec: Some({
            let spec = &driver.spec;
            k8s_pb::api::storage::v1::CSIDriverSpec {
                attach_required: spec.attach_required,
                pod_info_on_mount: spec.pod_info_on_mount,
                volume_lifecycle_modes: spec.volume_lifecycle_modes.clone().unwrap_or_default(),
                storage_capacity: spec.storage_capacity,
                fs_group_policy: spec.fs_group_policy.clone(),
                token_requests: vec![], // TokenRequest is complex, omit for now
                requires_republish: spec.requires_republish,
                se_linux_mount: spec.se_linux_mount,
                node_allocatable_update_period_seconds: None,
                service_account_token_in_secrets: None,
            }
        }),
    }
}

pub fn json_volumeattachment_to_pb(
    va: &k8s_openapi::api::storage::v1::VolumeAttachment,
) -> k8s_pb::api::storage::v1::VolumeAttachment {
    k8s_pb::api::storage::v1::VolumeAttachment {
        metadata: Some(json_meta_to_pb(&va.metadata)),
        spec: Some(k8s_pb::api::storage::v1::VolumeAttachmentSpec {
            attacher: Some(va.spec.attacher.clone()),
            node_name: Some(va.spec.node_name.clone()),
            source: Some(k8s_pb::api::storage::v1::VolumeAttachmentSource {
                persistent_volume_name: va.spec.source.persistent_volume_name.clone(),
                inline_volume_spec: None, // Complex nested spec, leave empty for now
            }),
        }),
        status: va.status.as_ref().map(|status| {
            k8s_pb::api::storage::v1::VolumeAttachmentStatus {
                attached: Some(status.attached),
                attachment_metadata: status
                    .attachment_metadata
                    .as_ref()
                    .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default(),
                attach_error: None, // VolumeError is complex, leave empty for now
                detach_error: None,
            }
        }),
    }
}

pub fn json_replicationcontroller_to_pb(
    rc: &k8s_openapi::api::core::v1::ReplicationController,
) -> k8s_pb::api::core::v1::ReplicationController {
    k8s_pb::api::core::v1::ReplicationController {
        metadata: Some(json_meta_to_pb(&rc.metadata)),
        spec: rc
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::ReplicationControllerSpec {
                replicas: spec.replicas,
                selector: spec
                    .selector
                    .as_ref()
                    .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default(),
                template: spec
                    .template
                    .as_ref()
                    .map(json_pod_template_spec_to_pb_encode),
                min_ready_seconds: spec.min_ready_seconds,
            }),
        status: rc.status.as_ref().map(|status| {
            k8s_pb::api::core::v1::ReplicationControllerStatus {
                replicas: Some(status.replicas),
                fully_labeled_replicas: status.fully_labeled_replicas,
                ready_replicas: status.ready_replicas,
                available_replicas: status.available_replicas,
                observed_generation: status.observed_generation,
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::api::core::v1::ReplicationControllerCondition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                last_transition_time: c
                                    .last_transition_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                reason: c.reason.clone(),
                                message: c.message.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        }),
    }
}

pub fn json_replicationcontrollerlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::ReplicationControllerList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ReplicationControllerList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::ReplicationController::deserialize(item)?;
            Ok(json_replicationcontroller_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::core::v1::ReplicationControllerList {
        metadata,
        items: pb_items,
    })
}

pub fn json_label_selector_to_pb(
    sel: &k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
    k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
        match_labels: sel
            .match_labels
            .as_ref()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        match_expressions: sel
            .match_expressions
            .as_ref()
            .map(|exprs| {
                exprs
                    .iter()
                    .map(
                        |e| k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
                            key: Some(e.key.clone()),
                            operator: Some(e.operator.clone()),
                            values: e.values.clone().unwrap_or_default(),
                        },
                    )
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub fn openapi_intorstring_to_pb(
    v: &k8s_openapi::apimachinery::pkg::util::intstr::IntOrString,
) -> k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
    match v {
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => {
            k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(0),
                int_val: Some(*n),
                str_val: None,
            }
        }
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => {
            k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(1),
                int_val: None,
                str_val: Some(s.clone()),
            }
        }
    }
}

pub fn json_meta_to_pb_from_obj(
    meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
    json_meta_to_pb(meta)
}
