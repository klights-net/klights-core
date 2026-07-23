// ============================================================================
// List type encoders: JSON list → k8s-pb list protobuf
// ============================================================================

/// Encode NodeList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_nodelist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::core::v1::NodeList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("NodeList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Node::deserialize(item)?;
            json_node_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::NodeList {
        metadata,
        items: pb_items,
    })
}

/// Encode PodList from JSON value to protobuf
pub fn json_podlist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::core::v1::PodList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PodList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Pod::deserialize(item)?;
            json_pod_to_pb(&openapi, item)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::PodList {
        metadata,
        items: pb_items,
    })
}

/// Encode NamespaceList from JSON value to protobuf
pub fn json_namespacelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::NamespaceList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("NamespaceList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Namespace::deserialize(item)?;
            json_namespace_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::NamespaceList {
        metadata,
        items: pb_items,
    })
}

/// Encode ConfigMapList from JSON value to protobuf
pub fn json_configmaplist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::ConfigMapList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ConfigMapList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::ConfigMap::deserialize(item)?;
            json_configmap_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::ConfigMapList {
        metadata,
        items: pb_items,
    })
}

/// Encode SecretList from JSON value to protobuf
pub fn json_secretlist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::core::v1::SecretList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("SecretList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Secret::deserialize(item)?;
            json_secret_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::SecretList {
        metadata,
        items: pb_items,
    })
}

/// Encode ServiceList from JSON value to protobuf
pub fn json_servicelist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::core::v1::ServiceList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ServiceList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Service::deserialize(item)?;
            json_service_to_pb(&openapi, item)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::ServiceList {
        metadata,
        items: pb_items,
    })
}

/// Encode ServiceAccountList from JSON value to protobuf
pub fn json_serviceaccountlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::ServiceAccountList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ServiceAccountList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::ServiceAccount::deserialize(item)?;
            json_serviceaccount_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::ServiceAccountList {
        metadata,
        items: pb_items,
    })
}

/// Encode EndpointsList from JSON value to protobuf
pub fn json_endpointslist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::EndpointsList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("EndpointsList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Endpoints::deserialize(item)?;
            json_endpoints_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::EndpointsList {
        metadata,
        items: pb_items,
    })
}

/// Encode PersistentVolumeList from JSON value to protobuf
pub fn json_persistentvolumelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::PersistentVolumeList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PersistentVolumeList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::PersistentVolume::deserialize(item)?;
            json_persistentvolume_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::PersistentVolumeList {
        metadata,
        items: pb_items,
    })
}

/// Encode PersistentVolumeClaimList from JSON value to protobuf
pub fn json_persistentvolumeclaimlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::PersistentVolumeClaimList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PersistentVolumeClaimList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::PersistentVolumeClaim::deserialize(item)?;
            json_persistentvolumeclaim_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::PersistentVolumeClaimList {
        metadata,
        items: pb_items,
    })
}

/// Encode EventList (core/v1) from JSON value to protobuf
pub fn json_eventlist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::core::v1::EventList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("EventList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::Event::deserialize(item)?;
            json_event_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::EventList {
        metadata,
        items: pb_items,
    })
}

/// Encode EventList (events.k8s.io/v1) from JSON value to protobuf
pub fn json_events_v1_eventlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::events::v1::EventList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("EventList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::events::v1::Event::deserialize(item)?;
            json_events_v1_event_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::events::v1::EventList {
        metadata,
        items: pb_items,
    })
}

/// Encode DeploymentList from JSON value to protobuf
pub fn json_deploymentlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::apps::v1::DeploymentList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("DeploymentList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::apps::v1::Deployment::deserialize(item)?;
            json_deployment_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::apps::v1::DeploymentList {
        metadata,
        items: pb_items,
    })
}

/// Encode ReplicaSetList from JSON value to protobuf
pub fn json_replicasetlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::apps::v1::ReplicaSetList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ReplicaSetList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::apps::v1::ReplicaSet::deserialize(item)?;
            json_replicaset_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::apps::v1::ReplicaSetList {
        metadata,
        items: pb_items,
    })
}

/// Encode StatefulSetList from JSON value to protobuf
pub fn json_statefulsetlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::apps::v1::StatefulSetList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("StatefulSetList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::apps::v1::StatefulSet::deserialize(item)?;
            json_statefulset_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::apps::v1::StatefulSetList {
        metadata,
        items: pb_items,
    })
}

/// Encode DaemonSetList from JSON value to protobuf
pub fn json_daemonsetlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::apps::v1::DaemonSetList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("DaemonSetList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::apps::v1::DaemonSet::deserialize(item)?;
            json_daemonset_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::apps::v1::DaemonSetList {
        metadata,
        items: pb_items,
    })
}

/// Encode JobList from JSON value to protobuf
pub fn json_joblist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::batch::v1::JobList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("JobList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::batch::v1::Job::deserialize(item)?;
            json_job_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::batch::v1::JobList {
        metadata,
        items: pb_items,
    })
}

/// Encode CronJobList from JSON value to protobuf
pub fn json_cronjoblist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::batch::v1::CronJobList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CronJobList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::batch::v1::CronJob::deserialize(item)?;
            Ok(json_cronjob_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::batch::v1::CronJobList {
        metadata,
        items: pb_items,
    })
}

/// Encode ResourceQuotaList from JSON value to protobuf
pub fn json_resourcequotalist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::ResourceQuotaList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ResourceQuotaList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::ResourceQuota::deserialize(item)?;
            json_resourcequota_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::ResourceQuotaList {
        metadata,
        items: pb_items,
    })
}

/// Encode LimitRangeList from JSON value to protobuf
pub fn json_limitrangelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::LimitRangeList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("LimitRangeList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::core::v1::LimitRange::deserialize(item)?;
            json_limitrange_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::core::v1::LimitRangeList {
        metadata,
        items: pb_items,
    })
}

/// Encode StorageClassList from JSON value to protobuf
pub fn json_storageclasslist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::storage::v1::StorageClassList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("StorageClassList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::storage::v1::StorageClass::deserialize(item)?;
            Ok(json_storageclass_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::storage::v1::StorageClassList {
        metadata,
        items: pb_items,
    })
}

/// Encode CSINodeList from JSON value to protobuf
pub fn json_csinodelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::storage::v1::CSINodeList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CSINodeList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::storage::v1::CSINode::deserialize(item)?;
            Ok(json_csinode_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    Ok(k8s_pb::api::storage::v1::CSINodeList {
        metadata,
        items: pb_items,
    })
}
