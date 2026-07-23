use crate::protobuf::*;
pub fn json_serviceaccount_to_pb(
    sa: &k8s_openapi::api::core::v1::ServiceAccount,
) -> anyhow::Result<k8s_pb::api::core::v1::ServiceAccount> {
    Ok(k8s_pb::api::core::v1::ServiceAccount {
        metadata: Some(json_meta_to_pb(&sa.metadata)),
        secrets: vec![], // Deprecated field
        image_pull_secrets: sa
            .image_pull_secrets
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .map(|r| k8s_pb::api::core::v1::LocalObjectReference {
                        name: Some(r.name.clone()),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        automount_service_account_token: sa.automount_service_account_token,
    })
}

/// Convert k8s-openapi PodTemplate to k8s-pb PodTemplate
pub fn json_podtemplate_to_pb(
    pt: &k8s_openapi::api::core::v1::PodTemplate,
) -> anyhow::Result<k8s_pb::api::core::v1::PodTemplate> {
    Ok(k8s_pb::api::core::v1::PodTemplate {
        metadata: Some(json_meta_to_pb(&pt.metadata)),
        template: pt
            .template
            .as_ref()
            .map(json_pod_template_spec_to_pb_encode),
    })
}

pub fn json_podtemplatelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::PodTemplateList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PodTemplateList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::PodTemplate::deserialize(item)?;
            json_podtemplate_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(k8s_pb::api::core::v1::PodTemplateList {
        metadata,
        items: pb_items,
    })
}

/// Convert k8s-openapi Endpoints to k8s-pb Endpoints
pub fn json_endpoints_to_pb(
    ep: &k8s_openapi::api::core::v1::Endpoints,
) -> anyhow::Result<k8s_pb::api::core::v1::Endpoints> {
    Ok(k8s_pb::api::core::v1::Endpoints {
        metadata: Some(json_meta_to_pb(&ep.metadata)),
        subsets: ep
            .subsets
            .as_ref()
            .map(|subsets| subsets.iter().map(json_endpoint_subset_to_pb).collect())
            .unwrap_or_default(),
    })
}

pub fn json_persistentvolume_to_pb(
    pv: &k8s_openapi::api::core::v1::PersistentVolume,
) -> anyhow::Result<k8s_pb::api::core::v1::PersistentVolume> {
    Ok(k8s_pb::api::core::v1::PersistentVolume {
        metadata: Some(json_meta_to_pb(&pv.metadata)),
        spec: pv.spec.as_ref().map(|spec| {
            k8s_pb::api::core::v1::PersistentVolumeSpec {
                capacity: spec
                    .capacity
                    .as_ref()
                    .map(|cap| {
                        cap.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                access_modes: spec.access_modes.clone().unwrap_or_default(),
                persistent_volume_reclaim_policy: spec.persistent_volume_reclaim_policy.clone(),
                storage_class_name: spec.storage_class_name.clone(),
                volume_mode: spec.volume_mode.clone(),
                ..Default::default() // Volume source fields (hostPath, nfs, etc.) - add as needed
            }
        }),
        status: pv
            .status
            .as_ref()
            .map(|status| k8s_pb::api::core::v1::PersistentVolumeStatus {
                phase: status.phase.clone(),
                message: status.message.clone(),
                reason: status.reason.clone(),
                ..Default::default()
            }),
    })
}

/// Convert k8s-openapi PersistentVolumeClaim to k8s-pb PersistentVolumeClaim
pub fn json_persistentvolumeclaim_to_pb(
    pvc: &k8s_openapi::api::core::v1::PersistentVolumeClaim,
) -> anyhow::Result<k8s_pb::api::core::v1::PersistentVolumeClaim> {
    Ok(k8s_pb::api::core::v1::PersistentVolumeClaim {
        metadata: Some(json_meta_to_pb(&pvc.metadata)),
        spec: pvc
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::PersistentVolumeClaimSpec {
                access_modes: spec.access_modes.clone().unwrap_or_default(),
                storage_class_name: spec.storage_class_name.clone(),
                volume_mode: spec.volume_mode.clone(),
                volume_name: spec.volume_name.clone(),
                resources: spec.resources.as_ref().map(|res| {
                    k8s_pb::api::core::v1::VolumeResourceRequirements {
                        limits: res
                            .limits
                            .as_ref()
                            .map(|lim| {
                                lim.iter()
                                    .map(|(k, v)| {
                                        (
                                            k.clone(),
                                            k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                                string: Some(v.0.clone()),
                                            },
                                        )
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        requests: res
                            .requests
                            .as_ref()
                            .map(|req| {
                                req.iter()
                                    .map(|(k, v)| {
                                        (
                                            k.clone(),
                                            k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                                string: Some(v.0.clone()),
                                            },
                                        )
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }
                }),
                ..Default::default()
            }),
        status: pvc.status.as_ref().map(|status| {
            k8s_pb::api::core::v1::PersistentVolumeClaimStatus {
                phase: status.phase.clone(),
                access_modes: status.access_modes.clone().unwrap_or_default(),
                capacity: status
                    .capacity
                    .as_ref()
                    .map(|cap| {
                        cap.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::api::core::v1::PersistentVolumeClaimCondition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                last_probe_time: c.last_probe_time.as_ref().map(json_time_to_pb),
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
                allocated_resources: status
                    .allocated_resources
                    .as_ref()
                    .map(|resources| {
                        resources
                            .iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                allocated_resource_statuses: status
                    .allocated_resource_statuses
                    .clone()
                    .map(|m| m.into_iter().collect())
                    .unwrap_or_default(),
                current_volume_attributes_class_name: status
                    .current_volume_attributes_class_name
                    .clone(),
                modify_volume_status: status.modify_volume_status.as_ref().map(|mv| {
                    k8s_pb::api::core::v1::ModifyVolumeStatus {
                        target_volume_attributes_class_name: mv
                            .target_volume_attributes_class_name
                            .clone(),
                        status: Some(mv.status.clone()),
                    }
                }),
            }
        }),
    })
}

/// Convert k8s-openapi Event to k8s-pb Event
pub fn json_event_to_pb(
    ev: &k8s_openapi::api::core::v1::Event,
) -> anyhow::Result<k8s_pb::api::core::v1::Event> {
    let involved_object = Some(json_obj_ref_to_pb(&ev.involved_object));

    let source = ev
        .source
        .as_ref()
        .map(|s| k8s_pb::api::core::v1::EventSource {
            component: s.component.clone(),
            host: s.host.clone(),
        });

    let first_timestamp =
        ev.first_timestamp
            .as_ref()
            .map(|t| k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
                seconds: Some(t.0.timestamp()),
                nanos: Some(t.0.timestamp_subsec_nanos() as i32),
            });

    let last_timestamp =
        ev.last_timestamp
            .as_ref()
            .map(|t| k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
                seconds: Some(t.0.timestamp()),
                nanos: Some(t.0.timestamp_subsec_nanos() as i32),
            });

    let event_time =
        ev.event_time
            .as_ref()
            .map(|t| k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
                seconds: Some(t.0.timestamp()),
                nanos: Some(t.0.timestamp_subsec_nanos() as i32),
            });

    let series = ev
        .series
        .as_ref()
        .map(|s| k8s_pb::api::core::v1::EventSeries {
            count: s.count,
            last_observed_time: s.last_observed_time.as_ref().map(|t| {
                k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
                    seconds: Some(t.0.timestamp()),
                    nanos: Some(t.0.timestamp_subsec_nanos() as i32),
                }
            }),
        });

    Ok(k8s_pb::api::core::v1::Event {
        metadata: Some(json_meta_to_pb(&ev.metadata)),
        involved_object,
        reason: ev.reason.clone(),
        message: ev.message.clone(),
        source,
        r#type: ev.type_.clone(),
        count: ev.count,
        first_timestamp,
        last_timestamp,
        event_time,
        series,
        action: ev.action.clone(),
        related: ev.related.as_ref().map(json_obj_ref_to_pb),
        reporting_component: ev.reporting_component.clone(),
        reporting_instance: ev.reporting_instance.clone(),
    })
}

/// Convert k8s-openapi ObjectReference to k8s-pb ObjectReference
pub fn json_obj_ref_to_pb(
    obj: &k8s_openapi::api::core::v1::ObjectReference,
) -> k8s_pb::api::core::v1::ObjectReference {
    k8s_pb::api::core::v1::ObjectReference {
        kind: obj.kind.clone(),
        namespace: obj.namespace.clone(),
        name: obj.name.clone(),
        uid: obj.uid.clone(),
        api_version: obj.api_version.clone(),
        resource_version: obj.resource_version.clone(),
        field_path: obj.field_path.clone(),
    }
}

/// Convert k8s-openapi events.k8s.io/v1 Event to k8s-pb events.k8s.io/v1 Event
pub fn json_events_v1_event_to_pb(
    ev: &k8s_openapi::api::events::v1::Event,
) -> anyhow::Result<k8s_pb::api::events::v1::Event> {
    let event_time =
        ev.event_time
            .as_ref()
            .map(|t| k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
                seconds: Some(t.0.timestamp()),
                nanos: Some(t.0.timestamp_subsec_nanos() as i32),
            });

    let series = ev
        .series
        .as_ref()
        .map(|s| k8s_pb::api::events::v1::EventSeries {
            count: Some(s.count),
            last_observed_time: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
                seconds: Some(s.last_observed_time.0.timestamp()),
                nanos: Some(s.last_observed_time.0.timestamp_subsec_nanos() as i32),
            }),
        });

    let deprecated_source =
        ev.deprecated_source
            .as_ref()
            .map(|s| k8s_pb::api::core::v1::EventSource {
                component: s.component.clone(),
                host: s.host.clone(),
            });

    let deprecated_first_timestamp = ev.deprecated_first_timestamp.as_ref().map(|t| {
        k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
            seconds: Some(t.0.timestamp()),
            nanos: Some(t.0.timestamp_subsec_nanos() as i32),
        }
    });

    let deprecated_last_timestamp = ev.deprecated_last_timestamp.as_ref().map(|t| {
        k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
            seconds: Some(t.0.timestamp()),
            nanos: Some(t.0.timestamp_subsec_nanos() as i32),
        }
    });

    Ok(k8s_pb::api::events::v1::Event {
        metadata: Some(json_meta_to_pb(&ev.metadata)),
        event_time,
        series,
        reporting_controller: ev.reporting_controller.clone(),
        reporting_instance: ev.reporting_instance.clone(),
        action: ev.action.clone(),
        reason: ev.reason.clone(),
        regarding: ev.regarding.as_ref().map(json_obj_ref_to_pb),
        related: ev.related.as_ref().map(json_obj_ref_to_pb),
        note: ev.note.clone(),
        r#type: ev.type_.clone(),
        deprecated_source,
        deprecated_first_timestamp,
        deprecated_last_timestamp,
        deprecated_count: ev.deprecated_count,
    })
}

/// Convert k8s-openapi Node to k8s-pb Node
pub fn json_node_to_pb(
    node: &k8s_openapi::api::core::v1::Node,
) -> anyhow::Result<k8s_pb::api::core::v1::Node> {
    Ok(k8s_pb::api::core::v1::Node {
        metadata: Some(json_meta_to_pb(&node.metadata)),
        spec: node
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::NodeSpec {
                pod_cidr: spec.pod_cidr.clone(),
                pod_cid_rs: spec.pod_cidrs.clone().unwrap_or_default(),
                provider_id: spec.provider_id.clone(),
                unschedulable: spec.unschedulable,
                taints: spec
                    .taints
                    .as_ref()
                    .map(|taints| {
                        taints
                            .iter()
                            .map(|t| k8s_pb::api::core::v1::Taint {
                                key: Some(t.key.clone()),
                                value: t.value.clone(),
                                effect: Some(t.effect.clone()),
                                time_added: t.time_added.as_ref().map(json_time_to_pb),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                ..Default::default()
            }),
        status: node
            .status
            .as_ref()
            .map(|status| k8s_pb::api::core::v1::NodeStatus {
                capacity: status
                    .capacity
                    .as_ref()
                    .map(|cap| {
                        cap.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                allocatable: status
                    .allocatable
                    .as_ref()
                    .map(|alloc| {
                        alloc
                            .iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                        string: Some(v.0.clone()),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                phase: status.phase.clone(),
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::api::core::v1::NodeCondition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                last_heartbeat_time: c
                                    .last_heartbeat_time
                                    .as_ref()
                                    .map(json_time_to_pb),
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
                addresses: status
                    .addresses
                    .as_ref()
                    .map(|addrs| {
                        addrs
                            .iter()
                            .map(|a| k8s_pb::api::core::v1::NodeAddress {
                                r#type: Some(a.type_.clone()),
                                address: Some(a.address.clone()),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                node_info: status.node_info.as_ref().map(|info| {
                    k8s_pb::api::core::v1::NodeSystemInfo {
                        machine_id: Some(info.machine_id.clone()),
                        system_uuid: Some(info.system_uuid.clone()),
                        boot_id: Some(info.boot_id.clone()),
                        kernel_version: Some(info.kernel_version.clone()),
                        os_image: Some(info.os_image.clone()),
                        container_runtime_version: Some(info.container_runtime_version.clone()),
                        kubelet_version: Some(info.kubelet_version.clone()),
                        kube_proxy_version: Some(info.kube_proxy_version.clone()),
                        operating_system: Some(info.operating_system.clone()),
                        architecture: Some(info.architecture.clone()),
                        swap: None,
                    }
                }),
                daemon_endpoints: status.daemon_endpoints.as_ref().map(|de| {
                    k8s_pb::api::core::v1::NodeDaemonEndpoints {
                        kubelet_endpoint: de.kubelet_endpoint.as_ref().map(|ke| {
                            k8s_pb::api::core::v1::DaemonEndpoint {
                                port: Some(ke.port),
                            }
                        }),
                    }
                }),
                ..Default::default()
            }),
    })
}
