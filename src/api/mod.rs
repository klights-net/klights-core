// `macros` must be declared with `#[macro_use]` BEFORE any module that
// invokes its macros (generated_handlers uses cluster_delete_collection_handler!).
#[macro_use]
pub mod macros;

pub mod apiservice_proxy;
pub(crate) mod backend_proxy_headers;
mod crd_conversion;
mod custom_resources;
mod debug;
mod defaulting;
mod errors;
mod extractors;
pub mod finalizer_delete;
pub mod generated_handlers;
mod handlers;
pub mod helpers;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod mod_tests;
mod namespace;
mod patch;
#[cfg(test)]
mod patch_tests;
mod pod_handlers;
mod pod_security;
mod query;
mod quotas;
pub mod raft_proxy;
mod rbac_admission;
mod response;
#[cfg(test)]
mod response_tests;
mod routes;
pub mod server_side_apply;
mod state;
#[cfg(test)]
pub mod test_support;
mod validation;
mod watch_stream;

#[cfg(test)]
mod defaulting_tests;

#[cfg(test)]
pub use crate::watch::EventType;
pub(crate) use apiservice_proxy::proxy_apiservice_request;
use crd_conversion::{
    convert_crd_objects_to_requested_version,
    convert_custom_resource_watch_event_to_requested_version,
    gather_custom_resource_events_across_served_versions,
    gather_custom_resources_across_served_versions, load_crd_conversion_config,
};
use custom_resources::{
    create_cluster_custom_resource, create_custom_resource, delete_cluster_custom_resource,
    delete_collection_cluster_custom_resources, delete_collection_custom_resources,
    delete_custom_resource, get_cluster_custom_resource, get_custom_resource,
    list_cluster_custom_resources, list_custom_resources, patch_cluster_custom_resource,
    patch_custom_resource, proxy_cluster_custom_resource_subresource,
    proxy_namespaced_custom_resource_subresource, update_cluster_custom_resource,
    update_custom_resource,
};
pub use debug::pod_lifecycle_debug_dump;
pub use defaulting::{
    apply_pod_create_defaults, apply_pod_service_account_defaults, apply_pod_spec_create_defaults,
    apply_pv_create_defaults, apply_pvc_create_defaults,
    apply_replicationcontroller_selector_default, apply_resourcequota_create_status,
    apply_workload_replicas_default, increment_generation_for_spec_change,
    increment_generation_if_spec_changed, inject_create_metadata, set_deletion_timestamp,
};
pub use errors::AppError;
use errors::{map_mutating_admission_error, map_validating_admission_error};
pub use extractors::LenientJson;
use extractors::{decode_json_or_proto, parse_lenient_value_from_bytes};
pub use generated_handlers::*;
#[cfg(test)]
pub use handlers::apiextensions_v1::add_crd_established_condition;
pub use handlers::apiextensions_v1::merge_stored_versions;
pub use handlers::apiextensions_v1::validate_api_approval;
pub use handlers::apiregistration_v1::{
    delete_apiservice_with_cache_invalidation, delete_collection_apiservices, get_apiservice_status,
};
use handlers::authentication_v1::create_token_review;
use handlers::authorization_v1::{
    create_local_subject_access_review, create_self_subject_access_review,
    create_self_subject_rules_review, create_subject_access_review,
};
pub use handlers::flowcontrol_v1::{
    delete_collection_flowschemas, delete_collection_prioritylevelconfigurations,
};
#[cfg(test)]
pub use helpers::watch_event_from_type;
pub use helpers::{
    NamespaceTerminationOutcome, apply_default_storage_class_admission,
    apply_limitrange_defaults_to_pod, apply_patch, apply_pod_runtimeclass_admission,
    enforce_limitrange_constraints_for_pod, enforce_limitrange_constraints_for_pvc, ensure_array,
    ensure_namespace_status_phase_active, ensure_object, normalize_resource_for_read,
    normalize_resource_for_storage, preserve_status_subresource_on_main_update,
    process_secret_stringdata, reconcile_namespace_termination,
    reconcile_namespace_termination_for_uid_with_outcome, resource_has_finalizers,
    set_namespace_terminating_status, validate_secret_data,
};
pub use namespace::{
    create_namespace, delete_namespace, finalize_namespace, get_namespace, is_protected_namespace,
    list_namespaces, patch_namespace, update_namespace,
};
pub use pod_handlers::{
    create_pod, delete_collection_pods, delete_pod, get_pod, list_all_pods, list_pods, patch_pod,
    update_pod,
};
pub use pod_security::enforce_pod_security_admission;
#[cfg(test)]
pub use query::{
    CONTINUE_TOKEN_TTL_SECS, ContinueTokenData, encode_continue_token,
    encode_inconsistent_continue_token,
};
use query::{CreateUpdateQuery, DeleteCollectionQuery, ListQuery, process_continue_token};
// Used only by mod_tests; the production list handlers now go through
// `query::resolve_list_page`, which calls this internally.
#[cfg(test)]
use query::resolve_list_response_resource_version;
pub use quotas::{
    check_resource_quota_for_creation, check_resource_quota_for_pod_update,
    check_resource_quota_for_pvc_update,
};
pub use response::K8sResponse;
#[cfg(test)]
pub use response::prefers_protobuf;
use response::{
    deployment_list_to_table, generic_list_to_table, node_list_to_table, pod_list_to_table,
    replicaset_list_to_table, statefulset_list_to_table, wants_table_format, watch_event_to_table,
};
pub use routes::build_router;
pub use state::AppState;
pub use validation::{
    AdmissionContextRequest, DeleteOptions, apply_crd_defaults, apply_crd_pruning,
    build_admission_context, check_content_type, check_cr_field_validation_strict,
    check_deployment_strict_decode_from_raw_json, check_field_validation_strict,
    check_field_validation_strict_typed, check_immutable_fields, inject_resource_version,
    parse_apply_yaml, parse_delete_options_body, prepare_admissionregistration_resource,
    run_admission_for_request, validate_builtin_field_selector, validate_builtin_resource_spec,
    validate_crd_field_selector, validate_pod_resource_requirements_immutable,
    validate_pod_sysctls, validate_priorityclass_update_immutable,
};
#[cfg(test)]
pub use validation::{
    apply_schema_defaults_pub, validate_against_schema, validate_metadata_fields,
    validate_webhook_configuration,
};
use watch_stream::{
    LabelSelectorWatchStreamRequest, WatchCatchUpMode, apply_selector_transition_event,
    build_label_selector_watch_stream, maybe_spawn_bookmark_tick_stream,
    maybe_spawn_watch_timeout_stream, object_matches_field_selector, recv_bookmark_tick,
    recv_watch_timeout,
};

#[cfg(test)]
pub use apiservice_proxy::resolve_service_proxy_target;
#[cfg(test)]
pub use crd_conversion::{CrdConversionConfig, build_crd_conversion_webhook_client};

use ::json_patch as json_patch_crate;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::api_discovery::{
    admissionregistration_v1_resources, api_group_by_name, api_groups, api_v1_resources,
    api_versions, apiextensions_group, apiextensions_v1_resources, apiregistration_v1_resources,
    apps_v1_resources, authentication_v1_resources, authorization_v1_resources,
    autoscaling_v1_resources, autoscaling_v2_resources, batch_v1_resources,
    certificates_v1_resources, coordination_v1_resources, custom_resource_discovery,
    discovery_v1_resources, events_k8s_io_v1_resources, flowcontrol_v1_resources, get_openapi_v2,
    get_openapi_v3_api_v1, get_openapi_v3_apis, get_openapi_v3_discovery,
    get_openapi_v3_group_version, metrics_v1beta1_resources, networking_v1_resources,
    node_k8s_io_group, node_k8s_io_v1_resources, policy_v1_resources, rbac_v1_resources,
    scheduling_group, scheduling_v1_resources, storage_v1_resources,
};
use crate::api_pod_subresources::{
    get_pod_ephemeral_containers, get_pod_log, get_pod_status, node_proxy, node_proxy_with_path,
    patch_pod_ephemeral_containers, patch_pod_status_subresource, pod_attach, pod_binding,
    pod_eviction, pod_exec, pod_portforward, pod_proxy, pod_proxy_with_path, service_proxy,
    service_proxy_with_path, update_pod_ephemeral_containers, update_pod_status_subresource,
};
use crate::api_status::{
    get_crd_status, get_csinode_status, get_csr_approval, get_csr_status, get_deployment_scale,
    get_flowschema_status, get_mutatingwebhookconfiguration_status, get_namespace_status,
    get_persistentvolume_status, get_persistentvolumeclaim_status,
    get_prioritylevelconfiguration_status, get_replicaset_scale, get_replicationcontroller_scale,
    get_statefulset_scale, get_validatingadmissionpolicy_status,
    get_validatingadmissionpolicybinding_status, get_validatingwebhookconfiguration_status,
    get_volumeattachment_status, patch_apiservice_status, patch_crd_status, patch_cronjob_status,
    patch_csinode_status, patch_csr_approval, patch_csr_status, patch_daemonset_status,
    patch_deployment_scale, patch_deployment_status, patch_flowschema_status, patch_hpa_v1_status,
    patch_hpa_v2_status, patch_ingress_status, patch_job_status,
    patch_mutatingwebhookconfiguration_status, patch_namespace_status, patch_node_status,
    patch_persistentvolume_status, patch_persistentvolumeclaim_status,
    patch_poddisruptionbudget_status, patch_prioritylevelconfiguration_status,
    patch_replicaset_scale, patch_replicaset_status, patch_replicationcontroller_scale,
    patch_replicationcontroller_status, patch_resourcequota_status, patch_service_status,
    patch_statefulset_scale, patch_statefulset_status, patch_validatingadmissionpolicy_status,
    patch_validatingadmissionpolicybinding_status, patch_validatingwebhookconfiguration_status,
    patch_volumeattachment_status, update_apiservice_status, update_crd_status,
    update_cronjob_status, update_csinode_status, update_csr_approval, update_csr_status,
    update_daemonset_status, update_deployment_scale, update_deployment_status,
    update_flowschema_status, update_hpa_v1_status, update_hpa_v2_status, update_ingress_status,
    update_job_status, update_mutatingwebhookconfiguration_status, update_namespace_status,
    update_node_status, update_persistentvolume_status, update_persistentvolumeclaim_status,
    update_poddisruptionbudget_status, update_prioritylevelconfiguration_status,
    update_replicaset_scale, update_replicaset_status, update_replicationcontroller_scale,
    update_replicationcontroller_status, update_resourcequota_status, update_service_status,
    update_statefulset_scale, update_statefulset_status, update_validatingadmissionpolicy_status,
    update_validatingadmissionpolicybinding_status, update_validatingwebhookconfiguration_status,
    update_volumeattachment_status,
};
use crate::controllers;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{CatchUpResource, DatastoreBackend, Resource, WatchTarget};
use crate::label_selector::LabelSelector;
use crate::watch::{WatchCursorError, WatchEvent};

// APIService proxy helpers moved to apiservice_proxy.rs

pub fn apply_pod_container_defaults(spec_obj: &mut serde_json::Map<String, Value>) {
    let needs_restart_policy_default = spec_obj
        .get("restartPolicy")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.is_empty()));
    if needs_restart_policy_default {
        spec_obj.insert("restartPolicy".to_string(), serde_json::json!("Always"));
    }

    fn apply_container_defaults(container: &mut Value) {
        let Some(container_obj) = container.as_object_mut() else {
            return;
        };

        let needs_term_path_default = container_obj
            .get("terminationMessagePath")
            .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.is_empty()));
        if needs_term_path_default {
            container_obj.insert(
                "terminationMessagePath".to_string(),
                serde_json::json!("/dev/termination-log"),
            );
        }

        let needs_term_policy_default = container_obj
            .get("terminationMessagePolicy")
            .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.is_empty()));
        if needs_term_policy_default {
            container_obj.insert(
                "terminationMessagePolicy".to_string(),
                serde_json::json!("File"),
            );
        }

        let image_pull_policy_missing_or_empty = container_obj
            .get("imagePullPolicy")
            .is_none_or(|v| v.is_null() || v.as_str().is_some_and(str::is_empty));
        if image_pull_policy_missing_or_empty {
            let policy =
                default_image_pull_policy(container_obj.get("image").and_then(|v| v.as_str()));
            container_obj.insert("imagePullPolicy".to_string(), serde_json::json!(policy));
        }

        for probe_key in ["livenessProbe", "readinessProbe", "startupProbe"] {
            let Some(http_get) = container_obj
                .get_mut(probe_key)
                .and_then(|probe| probe.get_mut("httpGet"))
                .and_then(|http_get| http_get.as_object_mut())
            else {
                continue;
            };

            let needs_path_default = http_get
                .get("path")
                .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.is_empty()));
            if needs_path_default {
                http_get.insert("path".to_string(), serde_json::json!("/"));
            }

            let needs_scheme_default = http_get
                .get("scheme")
                .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.is_empty()));
            if needs_scheme_default {
                http_get.insert("scheme".to_string(), serde_json::json!("HTTP"));
            }
        }
    }

    fn default_image_pull_policy(image: Option<&str>) -> &'static str {
        let image = image.unwrap_or_default();
        let image_without_digest = image.split_once('@').map(|(name, _)| name).unwrap_or(image);
        let last_component = image_without_digest
            .rsplit('/')
            .next()
            .unwrap_or(image_without_digest);
        match last_component.rsplit_once(':') {
            Some((_, "latest")) | None => "Always",
            Some((_, "")) => "Always",
            Some(_) => "IfNotPresent",
        }
    }

    for list_key in ["initContainers", "containers"] {
        if let Some(containers) = spec_obj.get_mut(list_key).and_then(|v| v.as_array_mut()) {
            for container in containers.iter_mut() {
                apply_container_defaults(container);
            }
        }
    }
}

/// Compute QOS class for a Pod based on container resource requests and limits.
///
/// QOS classes:
/// - Guaranteed: ALL containers have BOTH limits.cpu AND limits.memory set,
///   AND requests == limits (or requests not set, which defaults to limits)
/// - BestEffort: NO containers have ANY resource requests or limits
/// - Burstable: At least one container has a resource request or limit set,
///   but doesn't meet Guaranteed criteria
pub fn compute_qos_class(pod: &Value) -> &'static str {
    let spec = match pod.get("spec") {
        Some(s) => s,
        None => return "BestEffort",
    };

    // Collect all containers (init + regular)
    let mut all_containers = Vec::new();
    if let Some(init_containers) = spec.get("initContainers").and_then(|c| c.as_array()) {
        all_containers.extend(init_containers.iter());
    }
    if let Some(containers) = spec.get("containers").and_then(|c| c.as_array()) {
        all_containers.extend(containers.iter());
    }

    if all_containers.is_empty() {
        return "BestEffort";
    }

    let mut has_any_resources = false;
    let mut all_guaranteed = true;

    for container in all_containers {
        let resources = match container.get("resources") {
            Some(r) => r,
            None => {
                // No resources means not Guaranteed
                all_guaranteed = false;
                continue;
            }
        };

        let limits = resources.get("limits");
        let requests = resources.get("requests");

        // Check if any resources are set
        if limits.is_some() || requests.is_some() {
            has_any_resources = true;
        }

        // For Guaranteed, all containers must have cpu AND memory in limits
        let has_cpu_limit = limits.and_then(|l| l.get("cpu")).is_some();
        let has_mem_limit = limits.and_then(|l| l.get("memory")).is_some();

        if !has_cpu_limit || !has_mem_limit {
            all_guaranteed = false;
            continue;
        }

        // If requests are not set, they default to limits (still Guaranteed)
        // If requests are set, they must equal limits
        if let Some(req) = requests {
            let cpu_req = req.get("cpu").and_then(|v| v.as_str());
            let mem_req = req.get("memory").and_then(|v| v.as_str());
            let cpu_lim = limits.and_then(|l| l.get("cpu")).and_then(|v| v.as_str());
            let mem_lim = limits
                .and_then(|l| l.get("memory"))
                .and_then(|v| v.as_str());

            if cpu_req != cpu_lim || mem_req != mem_lim {
                all_guaranteed = false;
            }
        }
    }

    if all_guaranteed {
        "Guaranteed"
    } else if has_any_resources {
        "Burstable"
    } else {
        "BestEffort"
    }
}

pub fn resolve_resource_name(body: &mut serde_json::Value) -> Result<String, AppError> {
    // Try metadata.name first
    if let Some(name) = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
    {
        return Ok(name.to_string());
    }

    // Try metadata.generateName
    if let Some(prefix) = body
        .get("metadata")
        .and_then(|m| m.get("generateName"))
        .and_then(|n| n.as_str())
    {
        let generated_name = crate::utils::generate_name(prefix);

        // Inject generated name into body's metadata.name
        if let Some(obj) = body.as_object_mut()
            && let Some(metadata) = obj.get_mut("metadata")
            && let Some(meta_obj) = metadata.as_object_mut()
        {
            meta_obj.insert(
                "name".to_string(),
                serde_json::Value::String(generated_name.clone()),
            );
        }

        return Ok(generated_name);
    }

    // Neither name nor generateName provided
    Err(AppError::BadRequest(
        "Missing metadata.name or metadata.generateName".to_string(),
    ))
}

/// Max 253 characters.
/// Cannot start/end with hyphen or dot.
pub fn validate_dns_subdomain(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 {
        return false;
    }

    // Must start and end with alphanumeric
    let first = name.chars().next().unwrap();
    let last = name.chars().last().unwrap();
    if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
        return false;
    }

    // All characters must be lowercase alphanumeric, hyphen, or dot.
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// Validate DNS label (RFC 1123).
/// Rules: lowercase alphanumeric, hyphens (no dots)
/// Max 63 characters
/// Cannot start/end with hyphen
pub fn validate_dns_label(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }

    let first = name.chars().next().unwrap();
    let last = name.chars().last().unwrap();
    if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
        return false;
    }

    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// RBAC metadata names use Kubernetes path-segment validation, which permits
/// colons but rejects path separators and the two relative-path names.
pub fn validate_path_segment_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('%')
}

pub fn metadata_name_uses_path_segment_validation(api_version: &str, kind: &str) -> bool {
    api_version == "rbac.authorization.k8s.io/v1"
        && matches!(
            kind,
            "Role" | "RoleBinding" | "ClusterRole" | "ClusterRoleBinding"
        )
}

pub fn validate_metadata_name_for_kind(api_version: &str, kind: &str, name: &str) -> bool {
    if kind == "IPAddress" {
        return validate_path_segment_name(name);
    }
    if api_version == "v1" && kind == "Namespace" {
        return validate_dns_label(name);
    }
    if metadata_name_uses_path_segment_validation(api_version, kind) {
        return validate_path_segment_name(name);
    }
    validate_dns_subdomain(name)
}

// Error handling

// Cluster-scoped CRD handlers (e.g., ClusterIssuer, ClusterRole via CRD)
// These mirror the namespaced CRD handlers but with namespace=None

reconcile_handlers!(
    statefulset,
    create_statefulset_base,
    update_statefulset_base,
    patch_statefulset_base
);
reconcile_handlers!(
    daemonset,
    create_daemonset_base,
    update_daemonset_base,
    patch_daemonset_base
);
// Job: trigger reconcile_job on every create/update/patch so pods are created and
// status.succeeded/failed is updated after pod phase transitions.
reconcile_handlers!(job, create_job_base, update_job_base, patch_job_base);

reconcile_handlers!(
    replicaset,
    create_replicaset_base,
    update_replicaset_base,
    patch_replicaset_base
);
reconcile_create_handler!(replicationcontroller, create_replicationcontroller_base);

#[allow(hidden_glob_reexports)]
async fn update_replicationcontroller(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let result = generated_handlers::update_replicationcontroller(
        State(state.clone()),
        Path((namespace, name)),
        Query(query),
        axum::Extension(identity),
        LenientJson(body),
    )
    .await?;

    state.controller_dispatcher.enqueue(&result.0).await;

    Ok(result)
}

#[allow(hidden_glob_reexports)]
async fn patch_replicationcontroller(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let result = generated_handlers::patch_replicationcontroller(
        State(state.clone()),
        Path((namespace, name)),
        Query(query),
        headers,
        axum::Extension(identity),
        body,
    )
    .await?;

    state.controller_dispatcher.enqueue(&result.0).await;

    Ok(result)
}
