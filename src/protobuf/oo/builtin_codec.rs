//! OO protobuf codec for built-in resources that still use shared field
//! conversion helpers.

use crate::protobuf::*;
use crate::protobuf::{OoCodecRegistry, ResourceProtoCodec};
use prost::Message;
use serde::Deserialize;
use serde_json::Value;

type DecodeFn = fn(&str, &[u8]) -> anyhow::Result<Value>;
type EncodeFn = fn(&str, &Value) -> anyhow::Result<Vec<u8>>;

struct BuiltinCodecEntry {
    api_version_prefix: &'static str,
    kind: &'static str,
    decode: DecodeFn,
    encode: EncodeFn,
}

pub struct BuiltinResourceCodec;

impl BuiltinResourceCodec {
    fn lookup_entry(api_version: &str, kind: &str) -> Option<&'static BuiltinCodecEntry> {
        if api_version.is_empty() {
            return BUILTIN_ENTRIES
                .iter()
                .find(|entry| entry.kind == kind && entry.api_version_prefix.is_empty())
                .or_else(|| BUILTIN_ENTRIES.iter().find(|entry| entry.kind == kind));
        }

        let api_group = OoCodecRegistry::api_group_prefix(api_version);

        BUILTIN_ENTRIES
            .iter()
            .find(|entry| {
                entry.kind == kind
                    && !entry.api_version_prefix.is_empty()
                    && api_version_matches(entry.api_version_prefix, api_version)
            })
            .or_else(|| {
                if api_group.is_empty() {
                    BUILTIN_ENTRIES
                        .iter()
                        .find(|entry| entry.kind == kind && entry.api_version_prefix.is_empty())
                } else {
                    None
                }
            })
    }
}

impl ResourceProtoCodec for BuiltinResourceCodec {
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
        &[]
    }

    fn handles(&self, api_version: &str, kind: &str) -> bool {
        Self::lookup_entry(api_version, kind).is_some()
    }

    fn decode_to_json(&self, api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        let entry = Self::lookup_entry(api_version, kind).ok_or_else(|| {
            anyhow::anyhow!("BuiltinResourceCodec: unknown kind {api_version}/{kind}")
        })?;
        (entry.decode)(api_version, data)
    }

    fn encode_from_json(
        &self,
        api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>> {
        let entry = Self::lookup_entry(api_version, kind).ok_or_else(|| {
            anyhow::anyhow!("BuiltinResourceCodec: unknown kind {api_version}/{kind}")
        })?;
        (entry.encode)(api_version, value)
    }
}

fn api_version_matches(prefix: &str, api_version: &str) -> bool {
    api_version == prefix
        || api_version
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn has_events_v1_only_fields(decoded: &Value) -> bool {
    decoded.get("reportingController").is_some()
        || decoded.get("reportingInstance").is_some()
        || decoded.get("eventTime").is_some()
        || decoded.get("regarding").is_some()
        || decoded.get("note").is_some()
        || decoded.get("action").is_some()
}

fn unsupported_encode(api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    anyhow::bail!("Unknown kind for protobuf encoding: {api_version}/{kind}")
}

fn decode_generic_kind(api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
    decode_generic_protobuf(api_version, kind, data)
}

macro_rules! decode_pb_fn {
    ($fn_name:ident, $proto_ty:ty, $converter:path) => {
        fn $fn_name(_api_version: &str, data: &[u8]) -> anyhow::Result<Value> {
            let pb = <$proto_ty>::decode(data)?;
            $converter(&pb)
        }
    };
}

macro_rules! decode_generic_fn {
    ($fn_name:ident, $kind:expr_2021) => {
        fn $fn_name(api_version: &str, data: &[u8]) -> anyhow::Result<Value> {
            decode_generic_kind(api_version, $kind, data)
        }
    };
}

macro_rules! encode_openapi_result_fn {
    ($fn_name:ident, $openapi_ty:ty, $converter:path) => {
        fn $fn_name(_api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
            let openapi = <$openapi_ty as Deserialize>::deserialize(value)?;
            let pb = $converter(&openapi)?;
            encode_message_to_vec(&pb)
        }
    };
}

macro_rules! encode_openapi_value_result_fn {
    ($fn_name:ident, $openapi_ty:ty, $converter:path) => {
        fn $fn_name(_api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
            let openapi = <$openapi_ty as Deserialize>::deserialize(value)?;
            let pb = $converter(&openapi, value)?;
            encode_message_to_vec(&pb)
        }
    };
}

macro_rules! encode_openapi_plain_fn {
    ($fn_name:ident, $openapi_ty:ty, $converter:path) => {
        fn $fn_name(_api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
            let openapi = <$openapi_ty as Deserialize>::deserialize(value)?;
            let pb = $converter(&openapi);
            encode_message_to_vec(&pb)
        }
    };
}

macro_rules! encode_value_result_fn {
    ($fn_name:ident, $converter:path) => {
        fn $fn_name(_api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
            let pb = $converter(value)?;
            encode_message_to_vec(&pb)
        }
    };
}

macro_rules! encode_value_plain_fn {
    ($fn_name:ident, $converter:path) => {
        fn $fn_name(_api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
            let pb = $converter(value);
            encode_message_to_vec(&pb)
        }
    };
}

decode_pb_fn!(
    decode_namespace,
    k8s_pb::api::core::v1::Namespace,
    pb_namespace_to_json
);
decode_pb_fn!(
    decode_configmap,
    k8s_pb::api::core::v1::ConfigMap,
    pb_configmap_to_json
);
decode_pb_fn!(
    decode_secret,
    k8s_pb::api::core::v1::Secret,
    pb_secret_to_json
);
decode_pb_fn!(decode_pod, k8s_pb::api::core::v1::Pod, pb_pod_to_json);
decode_pb_fn!(
    decode_service,
    k8s_pb::api::core::v1::Service,
    pb_service_to_json
);
decode_pb_fn!(
    decode_serviceaccount,
    k8s_pb::api::core::v1::ServiceAccount,
    pb_serviceaccount_to_json
);
decode_pb_fn!(
    decode_endpoints,
    k8s_pb::api::core::v1::Endpoints,
    pb_endpoints_to_json
);
decode_pb_fn!(
    decode_persistentvolume,
    k8s_pb::api::core::v1::PersistentVolume,
    pb_persistentvolume_to_json
);
decode_pb_fn!(
    decode_persistentvolumeclaim,
    k8s_pb::api::core::v1::PersistentVolumeClaim,
    pb_persistentvolumeclaim_to_json
);
decode_pb_fn!(decode_node, k8s_pb::api::core::v1::Node, pb_node_to_json);
decode_pb_fn!(
    decode_podtemplate,
    k8s_pb::api::core::v1::PodTemplate,
    pb_podtemplate_to_json
);
decode_pb_fn!(
    decode_replicationcontroller,
    k8s_pb::api::core::v1::ReplicationController,
    pb_replicationcontroller_to_json
);
decode_generic_fn!(
    decode_replicationcontrollerlist,
    "ReplicationControllerList"
);
decode_pb_fn!(
    decode_resourcequota,
    k8s_pb::api::core::v1::ResourceQuota,
    pb_resourcequota_to_json
);
decode_pb_fn!(
    decode_limitrange,
    k8s_pb::api::core::v1::LimitRange,
    pb_limitrange_to_json
);
decode_pb_fn!(
    decode_deployment,
    k8s_pb::api::apps::v1::Deployment,
    pb_deployment_to_json
);
decode_pb_fn!(
    decode_replicaset,
    k8s_pb::api::apps::v1::ReplicaSet,
    pb_replicaset_to_json
);
decode_pb_fn!(
    decode_statefulset,
    k8s_pb::api::apps::v1::StatefulSet,
    pb_statefulset_to_json
);
decode_pb_fn!(
    decode_daemonset,
    k8s_pb::api::apps::v1::DaemonSet,
    pb_daemonset_to_json
);
decode_pb_fn!(decode_job, k8s_pb::api::batch::v1::Job, pb_job_to_json);
decode_pb_fn!(
    decode_cronjob,
    k8s_pb::api::batch::v1::CronJob,
    pb_cronjob_to_json
);
decode_pb_fn!(
    decode_tokenreview,
    k8s_pb::api::authentication::v1::TokenReview,
    pb_tokenreview_to_json
);
decode_pb_fn!(
    decode_apiservice,
    k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIService,
    pb_apiservice_to_json
);
decode_pb_fn!(
    decode_validatingadmissionpolicy,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicy,
    pb_validating_admission_policy_to_json
);
decode_pb_fn!(
    decode_validatingadmissionpolicybinding,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding,
    pb_validating_admission_policy_binding_to_json
);
decode_pb_fn!(
    decode_crd,
    k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    pb_crd_to_json
);
decode_pb_fn!(
    decode_lease,
    k8s_pb::api::coordination::v1::Lease,
    pb_lease_to_json
);
decode_pb_fn!(
    decode_priorityclass,
    k8s_pb::api::scheduling::v1::PriorityClass,
    pb_priorityclass_to_json
);
decode_pb_fn!(
    decode_volumeattachment,
    k8s_pb::api::storage::v1::VolumeAttachment,
    pb_volumeattachment_to_json
);
decode_pb_fn!(
    decode_storageclass,
    k8s_pb::api::storage::v1::StorageClass,
    pb_storageclass_to_json
);
decode_pb_fn!(
    decode_csistoragecapacity,
    k8s_pb::api::storage::v1::CSIStorageCapacity,
    pb_csistoragecapacity_to_json
);
decode_pb_fn!(
    decode_csinode,
    k8s_pb::api::storage::v1::CSINode,
    pb_csinode_to_json
);
decode_pb_fn!(
    decode_csidriver,
    k8s_pb::api::storage::v1::CSIDriver,
    pb_csidriver_to_json
);
decode_pb_fn!(
    decode_runtimeclass,
    k8s_pb::api::node::v1::RuntimeClass,
    pb_runtimeclass_to_json
);
decode_pb_fn!(
    decode_pdb,
    k8s_pb::api::policy::v1::PodDisruptionBudget,
    pb_poddisruptionbudget_to_json
);
decode_pb_fn!(
    decode_scale,
    k8s_pb::api::autoscaling::v1::Scale,
    pb_scale_to_json
);
decode_pb_fn!(
    decode_endpointslice,
    k8s_pb::api::discovery::v1::EndpointSlice,
    pb_endpointslice_to_json
);
decode_pb_fn!(
    decode_flowschema,
    k8s_pb::api::flowcontrol::v1::FlowSchema,
    pb_flowschema_to_json
);
decode_pb_fn!(
    decode_prioritylevelconfiguration,
    k8s_pb::api::flowcontrol::v1::PriorityLevelConfiguration,
    pb_prioritylevelconfiguration_to_json
);
decode_pb_fn!(
    decode_flowschemalist,
    k8s_pb::api::flowcontrol::v1::FlowSchemaList,
    pb_flowschemalist_to_json
);
decode_pb_fn!(
    decode_prioritylevelconfigurationlist,
    k8s_pb::api::flowcontrol::v1::PriorityLevelConfigurationList,
    pb_prioritylevelconfigurationlist_to_json
);
decode_pb_fn!(
    decode_nodelist,
    k8s_pb::api::core::v1::NodeList,
    pb_nodelist_to_json
);
decode_pb_fn!(
    decode_podlist,
    k8s_pb::api::core::v1::PodList,
    pb_podlist_to_json
);
decode_pb_fn!(
    decode_podtemplatelist,
    k8s_pb::api::core::v1::PodTemplateList,
    pb_podtemplatelist_to_json
);
decode_pb_fn!(
    decode_namespacelist,
    k8s_pb::api::core::v1::NamespaceList,
    pb_namespacelist_to_json
);
decode_pb_fn!(
    decode_configmaplist,
    k8s_pb::api::core::v1::ConfigMapList,
    pb_configmaplist_to_json
);
decode_pb_fn!(
    decode_secretlist,
    k8s_pb::api::core::v1::SecretList,
    pb_secretlist_to_json
);
decode_pb_fn!(
    decode_servicelist,
    k8s_pb::api::core::v1::ServiceList,
    pb_servicelist_to_json
);
decode_pb_fn!(
    decode_serviceaccountlist,
    k8s_pb::api::core::v1::ServiceAccountList,
    pb_serviceaccountlist_to_json
);
decode_pb_fn!(
    decode_endpointslist,
    k8s_pb::api::core::v1::EndpointsList,
    pb_endpointslist_to_json
);
decode_pb_fn!(
    decode_persistentvolumelist,
    k8s_pb::api::core::v1::PersistentVolumeList,
    pb_persistentvolumelist_to_json
);
decode_pb_fn!(
    decode_persistentvolumeclaimlist,
    k8s_pb::api::core::v1::PersistentVolumeClaimList,
    pb_persistentvolumeclaimlist_to_json
);
decode_pb_fn!(
    decode_deploymentlist,
    k8s_pb::api::apps::v1::DeploymentList,
    pb_deploymentlist_to_json
);
decode_pb_fn!(
    decode_replicasetlist,
    k8s_pb::api::apps::v1::ReplicaSetList,
    pb_replicasetlist_to_json
);
decode_pb_fn!(
    decode_statefulsetlist,
    k8s_pb::api::apps::v1::StatefulSetList,
    pb_statefulsetlist_to_json
);
decode_pb_fn!(
    decode_daemonsetlist,
    k8s_pb::api::apps::v1::DaemonSetList,
    pb_daemonsetlist_to_json
);
decode_pb_fn!(
    decode_joblist,
    k8s_pb::api::batch::v1::JobList,
    pb_joblist_to_json
);
decode_pb_fn!(
    decode_cronjoblist,
    k8s_pb::api::batch::v1::CronJobList,
    pb_cronjoblist_to_json
);
decode_pb_fn!(
    decode_resourcequotalist,
    k8s_pb::api::core::v1::ResourceQuotaList,
    pb_resourcequotalist_to_json
);
decode_pb_fn!(
    decode_limitrangelist,
    k8s_pb::api::core::v1::LimitRangeList,
    pb_limitrangelist_to_json
);
decode_pb_fn!(
    decode_priorityclasslist,
    k8s_pb::api::scheduling::v1::PriorityClassList,
    pb_priorityclasslist_to_json
);
decode_pb_fn!(
    decode_runtimeclasslist,
    k8s_pb::api::node::v1::RuntimeClassList,
    pb_runtimeclasslist_to_json
);
decode_pb_fn!(
    decode_storageclasslist,
    k8s_pb::api::storage::v1::StorageClassList,
    pb_storageclasslist_to_json
);
decode_pb_fn!(
    decode_csinodelist,
    k8s_pb::api::storage::v1::CSINodeList,
    pb_csinodelist_to_json
);
decode_pb_fn!(
    decode_csidriverlist,
    k8s_pb::api::storage::v1::CSIDriverList,
    pb_csidriverlist_to_json
);
decode_pb_fn!(
    decode_volumeattachmentlist,
    k8s_pb::api::storage::v1::VolumeAttachmentList,
    pb_volumeattachmentlist_to_json
);
decode_pb_fn!(
    decode_controllerrevision,
    k8s_pb::api::apps::v1::ControllerRevision,
    pb_controllerrevision_to_json
);
decode_pb_fn!(
    decode_controllerrevisionlist,
    k8s_pb::api::apps::v1::ControllerRevisionList,
    pb_controllerrevisionlist_to_json
);
decode_pb_fn!(
    decode_leaselist,
    k8s_pb::api::coordination::v1::LeaseList,
    pb_leaselist_to_json
);
decode_pb_fn!(
    decode_endpointslicelist,
    k8s_pb::api::discovery::v1::EndpointSliceList,
    pb_endpointslicelist_to_json
);
decode_pb_fn!(
    decode_ingress,
    k8s_pb::api::networking::v1::Ingress,
    pb_single_ingress_to_json
);
decode_pb_fn!(
    decode_ingressclass,
    k8s_pb::api::networking::v1::IngressClass,
    pb_single_ingressclass_to_json
);
decode_pb_fn!(
    decode_ingresslist,
    k8s_pb::api::networking::v1::IngressList,
    pb_ingresslist_to_json
);
decode_pb_fn!(
    decode_ingressclasslist,
    k8s_pb::api::networking::v1::IngressClassList,
    pb_ingressclasslist_to_json
);
decode_pb_fn!(
    decode_networkpolicy,
    k8s_pb::api::networking::v1::NetworkPolicy,
    pb_single_networkpolicy_to_json
);
decode_pb_fn!(
    decode_networkpolicylist,
    k8s_pb::api::networking::v1::NetworkPolicyList,
    pb_networkpolicylist_to_json
);
decode_pb_fn!(
    decode_pdblist,
    k8s_pb::api::policy::v1::PodDisruptionBudgetList,
    pb_pdblist_to_json
);
decode_pb_fn!(
    decode_apiservicelist,
    k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceList,
    pb_apiservicelist_to_json
);
decode_pb_fn!(
    decode_validatingadmissionpolicylist,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyList,
    pb_validatingadmissionpolicylist_to_json
);
decode_pb_fn!(
    decode_validatingadmissionpolicybindinglist,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingList,
    pb_validatingadmissionpolicybindinglist_to_json
);
decode_pb_fn!(
    decode_mutatingwebhookconfiguration,
    k8s_pb::api::admissionregistration::v1::MutatingWebhookConfiguration,
    pb_mutatingwebhookconfiguration_to_json
);
decode_pb_fn!(
    decode_validatingwebhookconfiguration,
    k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfiguration,
    pb_validatingwebhookconfiguration_to_json
);
decode_pb_fn!(
    decode_mutatingwebhookconfigurationlist,
    k8s_pb::api::admissionregistration::v1::MutatingWebhookConfigurationList,
    pb_mutatingwebhookconfigurationlist_to_json
);
decode_pb_fn!(
    decode_validatingwebhookconfigurationlist,
    k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfigurationList,
    pb_validatingwebhookconfigurationlist_to_json
);
decode_generic_fn!(decode_crdlist, "CustomResourceDefinitionList");
decode_generic_fn!(decode_csistoragecapacitylist, "CSIStorageCapacityList");
decode_generic_fn!(decode_servicecidr, "ServiceCIDR");
decode_generic_fn!(decode_servicecidrlist, "ServiceCIDRList");

fn decode_event(api_version: &str, data: &[u8]) -> anyhow::Result<Value> {
    if let Ok(event) = k8s_pb::api::events::v1::Event::decode(data) {
        let decoded = pb_events_v1_event_to_json(&event)?;
        if api_version_matches("events.k8s.io", api_version) || has_events_v1_only_fields(&decoded)
        {
            return Ok(decoded);
        }
    }
    let event = k8s_pb::api::core::v1::Event::decode(data)?;
    pb_event_to_json(&event)
}

fn decode_eventlist(api_version: &str, data: &[u8]) -> anyhow::Result<Value> {
    if api_version_matches("events.k8s.io", api_version) {
        let event_list = k8s_pb::api::events::v1::EventList::decode(data)?;
        return pb_events_v1_eventlist_to_json(&event_list);
    }
    let event_list = k8s_pb::api::core::v1::EventList::decode(data)?;
    pb_eventlist_to_json(&event_list)
}

encode_openapi_result_fn!(
    encode_namespace,
    k8s_openapi::api::core::v1::Namespace,
    json_namespace_to_pb
);
encode_openapi_result_fn!(
    encode_configmap,
    k8s_openapi::api::core::v1::ConfigMap,
    json_configmap_to_pb
);
encode_openapi_result_fn!(
    encode_secret,
    k8s_openapi::api::core::v1::Secret,
    json_secret_to_pb
);
encode_openapi_value_result_fn!(encode_pod, k8s_openapi::api::core::v1::Pod, json_pod_to_pb);
encode_openapi_value_result_fn!(
    encode_service,
    k8s_openapi::api::core::v1::Service,
    json_service_to_pb
);
encode_openapi_result_fn!(
    encode_serviceaccount,
    k8s_openapi::api::core::v1::ServiceAccount,
    json_serviceaccount_to_pb
);
encode_openapi_result_fn!(
    encode_podtemplate,
    k8s_openapi::api::core::v1::PodTemplate,
    json_podtemplate_to_pb
);
encode_value_result_fn!(encode_podtemplatelist, json_podtemplatelist_to_pb);
encode_openapi_result_fn!(
    encode_endpoints,
    k8s_openapi::api::core::v1::Endpoints,
    json_endpoints_to_pb
);
encode_openapi_result_fn!(
    encode_persistentvolume,
    k8s_openapi::api::core::v1::PersistentVolume,
    json_persistentvolume_to_pb
);
encode_openapi_result_fn!(
    encode_persistentvolumeclaim,
    k8s_openapi::api::core::v1::PersistentVolumeClaim,
    json_persistentvolumeclaim_to_pb
);
encode_openapi_result_fn!(
    encode_node,
    k8s_openapi::api::core::v1::Node,
    json_node_to_pb
);
encode_openapi_result_fn!(
    encode_deployment,
    k8s_openapi::api::apps::v1::Deployment,
    json_deployment_to_pb
);
encode_openapi_result_fn!(
    encode_replicaset,
    k8s_openapi::api::apps::v1::ReplicaSet,
    json_replicaset_to_pb
);
encode_openapi_result_fn!(
    encode_statefulset,
    k8s_openapi::api::apps::v1::StatefulSet,
    json_statefulset_to_pb
);
encode_openapi_result_fn!(
    encode_daemonset,
    k8s_openapi::api::apps::v1::DaemonSet,
    json_daemonset_to_pb
);
encode_openapi_result_fn!(encode_job, k8s_openapi::api::batch::v1::Job, json_job_to_pb);
encode_openapi_result_fn!(
    encode_apiservice,
    k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIService,
    json_apiservice_to_pb
);
encode_value_result_fn!(encode_apiservicelist, json_apiservicelist_to_pb);
encode_openapi_value_result_fn!(
    encode_crd,
    k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    json_crd_to_pb
);
encode_value_result_fn!(encode_crdlist, json_crdlist_to_pb);
encode_openapi_result_fn!(
    encode_lease,
    k8s_openapi::api::coordination::v1::Lease,
    json_lease_to_pb
);
encode_openapi_result_fn!(
    encode_resourcequota,
    k8s_openapi::api::core::v1::ResourceQuota,
    json_resourcequota_to_pb
);
encode_openapi_result_fn!(
    encode_limitrange,
    k8s_openapi::api::core::v1::LimitRange,
    json_limitrange_to_pb
);
encode_openapi_plain_fn!(
    encode_pdb,
    k8s_openapi::api::policy::v1::PodDisruptionBudget,
    json_pdb_to_pb
);
encode_openapi_plain_fn!(
    encode_cronjob,
    k8s_openapi::api::batch::v1::CronJob,
    json_cronjob_to_pb
);
encode_openapi_plain_fn!(
    encode_priorityclass,
    k8s_openapi::api::scheduling::v1::PriorityClass,
    json_priorityclass_to_pb
);
encode_openapi_plain_fn!(
    encode_volumeattachment,
    k8s_openapi::api::storage::v1::VolumeAttachment,
    json_volumeattachment_to_pb
);
encode_openapi_plain_fn!(
    encode_storageclass,
    k8s_openapi::api::storage::v1::StorageClass,
    json_storageclass_to_pb
);
encode_openapi_plain_fn!(
    encode_csistoragecapacity,
    k8s_openapi::api::storage::v1::CSIStorageCapacity,
    json_csistoragecapacity_to_pb
);
encode_openapi_plain_fn!(
    encode_csinode,
    k8s_openapi::api::storage::v1::CSINode,
    json_csinode_to_pb
);
encode_openapi_plain_fn!(
    encode_csidriver,
    k8s_openapi::api::storage::v1::CSIDriver,
    json_csidriver_to_pb
);
encode_openapi_plain_fn!(
    encode_replicationcontroller,
    k8s_openapi::api::core::v1::ReplicationController,
    json_replicationcontroller_to_pb
);
encode_value_result_fn!(
    encode_replicationcontrollerlist,
    json_replicationcontrollerlist_to_pb
);
encode_openapi_plain_fn!(
    encode_runtimeclass,
    k8s_openapi::api::node::v1::RuntimeClass,
    json_runtimeclass_to_pb
);
encode_value_plain_fn!(encode_scale, json_scale_to_pb);
encode_openapi_plain_fn!(
    encode_endpointslice,
    k8s_openapi::api::discovery::v1::EndpointSlice,
    json_endpointslice_to_pb
);
encode_value_result_fn!(encode_nodelist, json_nodelist_to_pb);
encode_value_result_fn!(encode_podlist, json_podlist_to_pb);
encode_value_result_fn!(encode_namespacelist, json_namespacelist_to_pb);
encode_value_result_fn!(encode_configmaplist, json_configmaplist_to_pb);
encode_value_result_fn!(encode_secretlist, json_secretlist_to_pb);
encode_value_result_fn!(encode_servicelist, json_servicelist_to_pb);
encode_value_result_fn!(encode_serviceaccountlist, json_serviceaccountlist_to_pb);
encode_value_result_fn!(encode_endpointslist, json_endpointslist_to_pb);
encode_value_result_fn!(encode_persistentvolumelist, json_persistentvolumelist_to_pb);
encode_value_result_fn!(
    encode_persistentvolumeclaimlist,
    json_persistentvolumeclaimlist_to_pb
);
encode_value_result_fn!(encode_deploymentlist, json_deploymentlist_to_pb);
encode_value_result_fn!(encode_replicasetlist, json_replicasetlist_to_pb);
encode_value_result_fn!(encode_statefulsetlist, json_statefulsetlist_to_pb);
encode_value_result_fn!(encode_daemonsetlist, json_daemonsetlist_to_pb);
encode_value_result_fn!(encode_joblist, json_joblist_to_pb);
encode_value_result_fn!(encode_cronjoblist, json_cronjoblist_to_pb);
encode_value_result_fn!(encode_resourcequotalist, json_resourcequotalist_to_pb);
encode_value_result_fn!(encode_limitrangelist, json_limitrangelist_to_pb);
encode_value_result_fn!(encode_priorityclasslist, json_priorityclasslist_to_pb);
encode_value_result_fn!(encode_runtimeclasslist, json_runtimeclasslist_to_pb);
encode_value_result_fn!(encode_storageclasslist, json_storageclasslist_to_pb);
encode_value_result_fn!(encode_csinodelist, json_csinodelist_to_pb);
encode_value_result_fn!(encode_csidriverlist, json_csidriverlist_to_pb);
encode_value_result_fn!(
    encode_csistoragecapacitylist,
    json_csistoragecapacitylist_to_pb
);
encode_value_result_fn!(encode_volumeattachmentlist, json_volumeattachmentlist_to_pb);
encode_value_result_fn!(
    encode_controllerrevisionlist,
    json_controllerrevisionlist_to_pb
);
encode_value_result_fn!(encode_leaselist, json_leaselist_to_pb);
encode_value_result_fn!(encode_endpointslicelist, json_endpointslicelist_to_pb);
encode_openapi_result_fn!(
    encode_ingress,
    k8s_openapi::api::networking::v1::Ingress,
    json_ingress_to_pb
);
encode_openapi_result_fn!(
    encode_ingressclass,
    k8s_openapi::api::networking::v1::IngressClass,
    json_ingressclass_to_pb
);
encode_value_result_fn!(encode_ingresslist, json_ingresslist_to_pb);
encode_value_result_fn!(encode_ingressclasslist, json_ingressclasslist_to_pb);
encode_openapi_result_fn!(
    encode_networkpolicy,
    k8s_openapi::api::networking::v1::NetworkPolicy,
    json_networkpolicy_to_pb
);
encode_value_result_fn!(encode_networkpolicylist, json_networkpolicylist_to_pb);
encode_value_result_fn!(encode_servicecidr, json_servicecidr_to_pb);
encode_value_result_fn!(encode_servicecidrlist, json_servicecidrlist_to_pb);
encode_value_result_fn!(encode_pdblist, json_pdblist_to_pb);
encode_openapi_result_fn!(
    encode_validatingadmissionpolicy,
    k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicy,
    json_validating_admission_policy_to_pb
);
encode_openapi_result_fn!(
    encode_validatingadmissionpolicybinding,
    k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding,
    json_validating_admission_policy_binding_to_pb
);
encode_value_result_fn!(
    encode_validatingadmissionpolicylist,
    json_validatingadmissionpolicylist_to_pb
);
encode_value_result_fn!(
    encode_validatingadmissionpolicybindinglist,
    json_validatingadmissionpolicybindinglist_to_pb
);
encode_openapi_result_fn!(
    encode_mutatingwebhookconfiguration,
    k8s_openapi::api::admissionregistration::v1::MutatingWebhookConfiguration,
    json_mutating_webhook_configuration_to_pb
);
encode_openapi_result_fn!(
    encode_validatingwebhookconfiguration,
    k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration,
    json_validating_webhook_configuration_to_pb
);
encode_value_result_fn!(
    encode_mutatingwebhookconfigurationlist,
    json_mutatingwebhookconfigurationlist_to_pb
);
encode_value_result_fn!(
    encode_validatingwebhookconfigurationlist,
    json_validatingwebhookconfigurationlist_to_pb
);
encode_value_plain_fn!(encode_flowschema, json_flowschema_to_pb);
encode_value_result_fn!(encode_flowschemalist, json_flowschemalist_to_pb);
encode_value_plain_fn!(
    encode_prioritylevelconfiguration,
    json_prioritylevelconfiguration_to_pb
);
encode_value_result_fn!(
    encode_prioritylevelconfigurationlist,
    json_prioritylevelconfigurationlist_to_pb
);

fn encode_event(api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
    if api_version_matches("events.k8s.io", api_version) {
        let mut normalized = value.clone();
        normalize_event_microtime_fields(&mut normalized);
        let openapi =
            <k8s_openapi::api::events::v1::Event as Deserialize>::deserialize(&normalized)?;
        let pb = json_events_v1_event_to_pb(&openapi)?;
        encode_message_to_vec(&pb)
    } else {
        let mut normalized = value.clone();
        normalize_event_microtime_fields(&mut normalized);
        let openapi = <k8s_openapi::api::core::v1::Event as Deserialize>::deserialize(&normalized)?;
        let pb = json_event_to_pb(&openapi)?;
        encode_message_to_vec(&pb)
    }
}

fn encode_eventlist(api_version: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
    if api_version_matches("events.k8s.io", api_version) {
        let pb = json_events_v1_eventlist_to_pb(value)?;
        encode_message_to_vec(&pb)
    } else {
        let pb = json_eventlist_to_pb(value)?;
        encode_message_to_vec(&pb)
    }
}

pub fn bare_list_decode_candidates() -> &'static [(&'static str, &'static str)] {
    BARE_LIST_DECODE_CANDIDATES
}

static BARE_LIST_DECODE_CANDIDATES: &[(&str, &str)] = &[
    ("v1", "PodList"),
    ("v1", "NodeList"),
    ("v1", "NamespaceList"),
    ("v1", "ConfigMapList"),
    ("v1", "SecretList"),
    ("v1", "ServiceList"),
    ("v1", "ServiceAccountList"),
    ("v1", "EndpointsList"),
    ("v1", "PersistentVolumeList"),
    ("v1", "PersistentVolumeClaimList"),
    ("v1", "EventList"),
    ("events.k8s.io/v1", "EventList"),
    ("apps/v1", "DeploymentList"),
    ("apps/v1", "ReplicaSetList"),
    ("apps/v1", "StatefulSetList"),
    ("apps/v1", "DaemonSetList"),
    ("batch/v1", "JobList"),
    ("batch/v1", "CronJobList"),
    ("v1", "ResourceQuotaList"),
    ("v1", "LimitRangeList"),
    ("scheduling.k8s.io/v1", "PriorityClassList"),
    ("node.k8s.io/v1", "RuntimeClassList"),
    ("storage.k8s.io/v1", "StorageClassList"),
    ("apps/v1", "ControllerRevisionList"),
    ("coordination.k8s.io/v1", "LeaseList"),
    ("discovery.k8s.io/v1", "EndpointSliceList"),
    ("networking.k8s.io/v1", "IngressList"),
    ("networking.k8s.io/v1", "IngressClassList"),
    ("networking.k8s.io/v1", "NetworkPolicyList"),
    ("policy/v1", "PodDisruptionBudgetList"),
    ("apiregistration.k8s.io/v1", "APIServiceList"),
    (
        "admissionregistration.k8s.io/v1",
        "ValidatingAdmissionPolicyList",
    ),
    (
        "admissionregistration.k8s.io/v1",
        "ValidatingAdmissionPolicyBindingList",
    ),
    (
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfigurationList",
    ),
    (
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfigurationList",
    ),
];

// Do not grow this table. It is the remaining built-in compatibility bucket
// and is capped by scripts/check_rbac_task_codecs.sh. New protobuf API group
// coverage should be implemented as dedicated per-group ResourceProtoCodec
// objects, then this table should shrink as entries migrate out.
static BUILTIN_ENTRIES: &[BuiltinCodecEntry] = &[
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Namespace",
        decode: decode_namespace,
        encode: encode_namespace,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ConfigMap",
        decode: decode_configmap,
        encode: encode_configmap,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Secret",
        decode: decode_secret,
        encode: encode_secret,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Pod",
        decode: decode_pod,
        encode: encode_pod,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Service",
        decode: decode_service,
        encode: encode_service,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ServiceAccount",
        decode: decode_serviceaccount,
        encode: encode_serviceaccount,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Endpoints",
        decode: decode_endpoints,
        encode: encode_endpoints,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PersistentVolume",
        decode: decode_persistentvolume,
        encode: encode_persistentvolume,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PersistentVolumeClaim",
        decode: decode_persistentvolumeclaim,
        encode: encode_persistentvolumeclaim,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Node",
        decode: decode_node,
        encode: encode_node,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PodTemplate",
        decode: decode_podtemplate,
        encode: encode_podtemplate,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ReplicationController",
        decode: decode_replicationcontroller,
        encode: encode_replicationcontroller,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ReplicationControllerList",
        decode: decode_replicationcontrollerlist,
        encode: encode_replicationcontrollerlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ResourceQuota",
        decode: decode_resourcequota,
        encode: encode_resourcequota,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "LimitRange",
        decode: decode_limitrange,
        encode: encode_limitrange,
    },
    BuiltinCodecEntry {
        api_version_prefix: "events.k8s.io",
        kind: "Event",
        decode: decode_event,
        encode: encode_event,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "Event",
        decode: decode_event,
        encode: encode_event,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "Deployment",
        decode: decode_deployment,
        encode: encode_deployment,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "ReplicaSet",
        decode: decode_replicaset,
        encode: encode_replicaset,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "StatefulSet",
        decode: decode_statefulset,
        encode: encode_statefulset,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "DaemonSet",
        decode: decode_daemonset,
        encode: encode_daemonset,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "ControllerRevision",
        decode: decode_controllerrevision,
        encode: unsupported_encode,
    },
    BuiltinCodecEntry {
        api_version_prefix: "batch",
        kind: "Job",
        decode: decode_job,
        encode: encode_job,
    },
    BuiltinCodecEntry {
        api_version_prefix: "batch",
        kind: "CronJob",
        decode: decode_cronjob,
        encode: encode_cronjob,
    },
    BuiltinCodecEntry {
        api_version_prefix: "authentication.k8s.io",
        kind: "TokenReview",
        decode: decode_tokenreview,
        encode: unsupported_encode,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apiregistration.k8s.io",
        kind: "APIService",
        decode: decode_apiservice,
        encode: encode_apiservice,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingAdmissionPolicy",
        decode: decode_validatingadmissionpolicy,
        encode: encode_validatingadmissionpolicy,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingAdmissionPolicyBinding",
        decode: decode_validatingadmissionpolicybinding,
        encode: encode_validatingadmissionpolicybinding,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apiextensions.k8s.io",
        kind: "CustomResourceDefinition",
        decode: decode_crd,
        encode: encode_crd,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apiextensions.k8s.io",
        kind: "CustomResourceDefinitionList",
        decode: decode_crdlist,
        encode: encode_crdlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "coordination.k8s.io",
        kind: "Lease",
        decode: decode_lease,
        encode: encode_lease,
    },
    BuiltinCodecEntry {
        api_version_prefix: "scheduling.k8s.io",
        kind: "PriorityClass",
        decode: decode_priorityclass,
        encode: encode_priorityclass,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "VolumeAttachment",
        decode: decode_volumeattachment,
        encode: encode_volumeattachment,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "StorageClass",
        decode: decode_storageclass,
        encode: encode_storageclass,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSIStorageCapacity",
        decode: decode_csistoragecapacity,
        encode: encode_csistoragecapacity,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSINode",
        decode: decode_csinode,
        encode: encode_csinode,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSIDriver",
        decode: decode_csidriver,
        encode: encode_csidriver,
    },
    BuiltinCodecEntry {
        api_version_prefix: "node.k8s.io",
        kind: "RuntimeClass",
        decode: decode_runtimeclass,
        encode: encode_runtimeclass,
    },
    BuiltinCodecEntry {
        api_version_prefix: "policy",
        kind: "PodDisruptionBudget",
        decode: decode_pdb,
        encode: encode_pdb,
    },
    BuiltinCodecEntry {
        api_version_prefix: "autoscaling",
        kind: "Scale",
        decode: decode_scale,
        encode: encode_scale,
    },
    BuiltinCodecEntry {
        api_version_prefix: "discovery.k8s.io",
        kind: "EndpointSlice",
        decode: decode_endpointslice,
        encode: encode_endpointslice,
    },
    BuiltinCodecEntry {
        api_version_prefix: "flowcontrol.apiserver.k8s.io",
        kind: "FlowSchema",
        decode: decode_flowschema,
        encode: encode_flowschema,
    },
    BuiltinCodecEntry {
        api_version_prefix: "flowcontrol.apiserver.k8s.io",
        kind: "PriorityLevelConfiguration",
        decode: decode_prioritylevelconfiguration,
        encode: encode_prioritylevelconfiguration,
    },
    BuiltinCodecEntry {
        api_version_prefix: "flowcontrol.apiserver.k8s.io",
        kind: "FlowSchemaList",
        decode: decode_flowschemalist,
        encode: encode_flowschemalist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "flowcontrol.apiserver.k8s.io",
        kind: "PriorityLevelConfigurationList",
        decode: decode_prioritylevelconfigurationlist,
        encode: encode_prioritylevelconfigurationlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "NodeList",
        decode: decode_nodelist,
        encode: encode_nodelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PodList",
        decode: decode_podlist,
        encode: encode_podlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PodTemplateList",
        decode: decode_podtemplatelist,
        encode: encode_podtemplatelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "NamespaceList",
        decode: decode_namespacelist,
        encode: encode_namespacelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ConfigMapList",
        decode: decode_configmaplist,
        encode: encode_configmaplist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "SecretList",
        decode: decode_secretlist,
        encode: encode_secretlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ServiceList",
        decode: decode_servicelist,
        encode: encode_servicelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ServiceAccountList",
        decode: decode_serviceaccountlist,
        encode: encode_serviceaccountlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "EndpointsList",
        decode: decode_endpointslist,
        encode: encode_endpointslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PersistentVolumeList",
        decode: decode_persistentvolumelist,
        encode: encode_persistentvolumelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "PersistentVolumeClaimList",
        decode: decode_persistentvolumeclaimlist,
        encode: encode_persistentvolumeclaimlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "events.k8s.io",
        kind: "EventList",
        decode: decode_eventlist,
        encode: encode_eventlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "EventList",
        decode: decode_eventlist,
        encode: encode_eventlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "DeploymentList",
        decode: decode_deploymentlist,
        encode: encode_deploymentlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "ReplicaSetList",
        decode: decode_replicasetlist,
        encode: encode_replicasetlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "StatefulSetList",
        decode: decode_statefulsetlist,
        encode: encode_statefulsetlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "DaemonSetList",
        decode: decode_daemonsetlist,
        encode: encode_daemonsetlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "batch",
        kind: "JobList",
        decode: decode_joblist,
        encode: encode_joblist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "batch",
        kind: "CronJobList",
        decode: decode_cronjoblist,
        encode: encode_cronjoblist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "ResourceQuotaList",
        decode: decode_resourcequotalist,
        encode: encode_resourcequotalist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "",
        kind: "LimitRangeList",
        decode: decode_limitrangelist,
        encode: encode_limitrangelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "scheduling.k8s.io",
        kind: "PriorityClassList",
        decode: decode_priorityclasslist,
        encode: encode_priorityclasslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "node.k8s.io",
        kind: "RuntimeClassList",
        decode: decode_runtimeclasslist,
        encode: encode_runtimeclasslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "StorageClassList",
        decode: decode_storageclasslist,
        encode: encode_storageclasslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSINodeList",
        decode: decode_csinodelist,
        encode: encode_csinodelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSIDriverList",
        decode: decode_csidriverlist,
        encode: encode_csidriverlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "CSIStorageCapacityList",
        decode: decode_csistoragecapacitylist,
        encode: encode_csistoragecapacitylist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "storage.k8s.io",
        kind: "VolumeAttachmentList",
        decode: decode_volumeattachmentlist,
        encode: encode_volumeattachmentlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apps",
        kind: "ControllerRevisionList",
        decode: decode_controllerrevisionlist,
        encode: encode_controllerrevisionlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "coordination.k8s.io",
        kind: "LeaseList",
        decode: decode_leaselist,
        encode: encode_leaselist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "discovery.k8s.io",
        kind: "EndpointSliceList",
        decode: decode_endpointslicelist,
        encode: encode_endpointslicelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "Ingress",
        decode: decode_ingress,
        encode: encode_ingress,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "IngressClass",
        decode: decode_ingressclass,
        encode: encode_ingressclass,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "IngressList",
        decode: decode_ingresslist,
        encode: encode_ingresslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "IngressClassList",
        decode: decode_ingressclasslist,
        encode: encode_ingressclasslist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "NetworkPolicy",
        decode: decode_networkpolicy,
        encode: encode_networkpolicy,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "NetworkPolicyList",
        decode: decode_networkpolicylist,
        encode: encode_networkpolicylist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "ServiceCIDR",
        decode: decode_servicecidr,
        encode: encode_servicecidr,
    },
    BuiltinCodecEntry {
        api_version_prefix: "networking.k8s.io",
        kind: "ServiceCIDRList",
        decode: decode_servicecidrlist,
        encode: encode_servicecidrlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "policy",
        kind: "PodDisruptionBudgetList",
        decode: decode_pdblist,
        encode: encode_pdblist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "apiregistration.k8s.io",
        kind: "APIServiceList",
        decode: decode_apiservicelist,
        encode: encode_apiservicelist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingAdmissionPolicyList",
        decode: decode_validatingadmissionpolicylist,
        encode: encode_validatingadmissionpolicylist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingAdmissionPolicyBindingList",
        decode: decode_validatingadmissionpolicybindinglist,
        encode: encode_validatingadmissionpolicybindinglist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "MutatingWebhookConfiguration",
        decode: decode_mutatingwebhookconfiguration,
        encode: encode_mutatingwebhookconfiguration,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingWebhookConfiguration",
        decode: decode_validatingwebhookconfiguration,
        encode: encode_validatingwebhookconfiguration,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "MutatingWebhookConfigurationList",
        decode: decode_mutatingwebhookconfigurationlist,
        encode: encode_mutatingwebhookconfigurationlist,
    },
    BuiltinCodecEntry {
        api_version_prefix: "admissionregistration.k8s.io",
        kind: "ValidatingWebhookConfigurationList",
        decode: decode_validatingwebhookconfigurationlist,
        encode: encode_validatingwebhookconfigurationlist,
    },
];
