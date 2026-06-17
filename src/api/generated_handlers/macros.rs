//! Wrapper macros + resource handler registrations.
//! Extracted from generated_handlers.rs (refactor).

use crate::api::*;
use std::sync::Arc;

use super::inners::*;

// ============================================================================
// Thin axum-extractor wrapper macros. Each invocation generates one set of
// per-resource handler functions that delegate to the shared `*_inner`
// functions above. The two macros differ only in the path-extractor shape
// and the namespace argument passed through.
// ============================================================================

macro_rules! namespaced_resource_handlers {
    ($kind:expr_2021, $list_kind:expr_2021, $api_version:expr_2021, $list_fn:ident, $get_fn:ident, $create_fn:ident, $update_fn:ident, $delete_fn:ident, $patch_fn:ident, $delete_collection_fn:ident) => {
        pub async fn $list_fn(
            State(state): State<Arc<AppState>>,
            Path(namespace): Path<String>,
            Query(query): Query<ListQuery>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<Response, AppError> {
            list_inner(
                state,
                &identity,
                GeneratedListInnerRequest {
                    api_version: $api_version,
                    kind: $kind,
                    list_kind: $list_kind,
                    namespace: Some(namespace),
                    query,
                    headers,
                },
            )
            .await
        }

        pub async fn $get_fn(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<K8sResponse, AppError> {
            get_inner(
                state,
                &identity,
                $api_version,
                $kind,
                Some(&namespace),
                &name,
                headers,
            )
            .await
        }

        pub async fn $create_fn(
            State(state): State<Arc<AppState>>,
            Path(namespace): Path<String>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<(StatusCode, Json<Value>), AppError> {
            create_inner(
                state,
                &identity,
                $api_version,
                $kind,
                Some(&namespace),
                query,
                body,
            )
            .await
        }

        pub async fn $update_fn(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<Json<Value>, AppError> {
            update_inner(
                state,
                &identity,
                GeneratedUpdateInnerRequest {
                    target: GeneratedNamedResource::new(
                        $api_version,
                        $kind,
                        Some(&namespace),
                        &name,
                    ),
                    query,
                    body,
                },
            )
            .await
        }

        pub async fn $delete_fn(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            body: Bytes,
        ) -> Result<(StatusCode, Json<Value>), AppError> {
            delete_inner(
                state,
                &identity,
                GeneratedDeleteInnerRequest {
                    target: GeneratedNamedResource::new(
                        $api_version,
                        $kind,
                        Some(&namespace),
                        &name,
                    ),
                    query,
                    body,
                },
            )
            .await
        }

        pub async fn $patch_fn(
            State(state): State<Arc<AppState>>,
            Path((namespace, name)): Path<(String, String)>,
            Query(query): Query<CreateUpdateQuery>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            body: Bytes,
        ) -> Result<Json<Value>, AppError> {
            patch_inner(
                state,
                &identity,
                GeneratedPatchInnerRequest {
                    target: GeneratedNamedResource::new(
                        $api_version,
                        $kind,
                        Some(&namespace),
                        &name,
                    ),
                    query,
                    headers,
                    body,
                },
            )
            .await
        }

        pub async fn $delete_collection_fn(
            State(state): State<Arc<AppState>>,
            Path(namespace): Path<String>,
            Query(query): Query<DeleteCollectionQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<Json<Value>, AppError> {
            delete_collection_inner(state, &identity, $api_version, $kind, &namespace, query).await
        }
    };
}

macro_rules! cluster_resource_handlers {
    ($kind:expr_2021, $list_kind:expr_2021, $api_version:expr_2021, $list_fn:ident, $get_fn:ident, $create_fn:ident, $update_fn:ident, $delete_fn:ident, $patch_fn:ident) => {
        pub async fn $list_fn(
            State(state): State<Arc<AppState>>,
            Query(query): Query<ListQuery>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<Response, AppError> {
            list_inner(
                state,
                &identity,
                GeneratedListInnerRequest {
                    api_version: $api_version,
                    kind: $kind,
                    list_kind: $list_kind,
                    namespace: None,
                    query,
                    headers,
                },
            )
            .await
        }

        pub async fn $get_fn(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
        ) -> Result<K8sResponse, AppError> {
            get_inner(state, &identity, $api_version, $kind, None, &name, headers).await
        }

        pub async fn $create_fn(
            State(state): State<Arc<AppState>>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<(StatusCode, Json<Value>), AppError> {
            create_inner(state, &identity, $api_version, $kind, None, query, body).await
        }

        pub async fn $update_fn(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            LenientJson(body): LenientJson<Value>,
        ) -> Result<Json<Value>, AppError> {
            update_inner(
                state,
                &identity,
                GeneratedUpdateInnerRequest {
                    target: GeneratedNamedResource::new($api_version, $kind, None, &name),
                    query,
                    body,
                },
            )
            .await
        }

        pub async fn $delete_fn(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            Query(query): Query<CreateUpdateQuery>,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            body: Bytes,
        ) -> Result<(StatusCode, Json<Value>), AppError> {
            delete_inner(
                state,
                &identity,
                GeneratedDeleteInnerRequest {
                    target: GeneratedNamedResource::new($api_version, $kind, None, &name),
                    query,
                    body,
                },
            )
            .await
        }

        pub async fn $patch_fn(
            State(state): State<Arc<AppState>>,
            Path(name): Path<String>,
            Query(query): Query<CreateUpdateQuery>,
            headers: HeaderMap,
            axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
            body: Bytes,
        ) -> Result<Json<Value>, AppError> {
            patch_inner(
                state,
                &identity,
                GeneratedPatchInnerRequest {
                    target: GeneratedNamedResource::new($api_version, $kind, None, &name),
                    query,
                    headers,
                    body,
                },
            )
            .await
        }
    };
}

// Generate handlers for all core v1 resources

// Namespaced resources
// Note: Pod handlers are extracted in `src/api/pod_handlers.rs`. The macro
// body still contains `if $kind == "Pod"`
// branches for any future caller, but Pod itself no longer goes through the
// macro expansion here.
namespaced_resource_handlers!(
    "Service",
    "ServiceList",
    "v1",
    list_services,
    get_service,
    create_service_base,
    update_service_base,
    delete_service_base,
    patch_service_base,
    delete_collection_services
);
namespaced_resource_handlers!(
    "Endpoints",
    "EndpointsList",
    "v1",
    list_endpoints,
    get_endpoints,
    create_endpoints,
    update_endpoints,
    delete_endpoints,
    patch_endpoints,
    delete_collection_endpoints
);
namespaced_resource_handlers!(
    "ConfigMap",
    "ConfigMapList",
    "v1",
    list_configmaps,
    get_configmap,
    create_configmap,
    update_configmap,
    delete_configmap,
    patch_configmap,
    delete_collection_configmaps
);
namespaced_resource_handlers!(
    "Secret",
    "SecretList",
    "v1",
    list_secrets,
    get_secret,
    create_secret,
    update_secret,
    delete_secret,
    patch_secret,
    delete_collection_secrets
);
namespaced_resource_handlers!(
    "PersistentVolumeClaim",
    "PersistentVolumeClaimList",
    "v1",
    list_persistent_volume_claims,
    get_persistent_volume_claim,
    create_persistent_volume_claim,
    update_persistent_volume_claim,
    delete_persistent_volume_claim,
    patch_persistent_volume_claim,
    delete_collection_persistent_volume_claims
);
namespaced_resource_handlers!(
    "ServiceAccount",
    "ServiceAccountList",
    "v1",
    list_service_accounts,
    get_service_account,
    create_service_account,
    update_service_account,
    delete_service_account,
    patch_service_account,
    delete_collection_service_accounts
);

// ServiceAccount token subresource
namespaced_resource_handlers!(
    "Event",
    "EventList",
    "v1",
    list_events,
    get_event,
    create_event,
    update_event,
    delete_event,
    patch_event,
    delete_collection_events
);
namespaced_resource_handlers!(
    "PodTemplate",
    "PodTemplateList",
    "v1",
    list_podtemplates,
    get_podtemplate,
    create_podtemplate,
    update_podtemplate,
    delete_podtemplate,
    patch_podtemplate,
    delete_collection_podtemplates
);

// Cluster-scoped resources
// Namespace handlers are now explicit (use dedicated namespaces table) - see lines ~826-1008
cluster_resource_handlers!(
    "Node",
    "NodeList",
    "v1",
    list_nodes,
    get_node,
    create_node,
    update_node,
    delete_node,
    patch_node
);
cluster_resource_handlers!(
    "PersistentVolume",
    "PersistentVolumeList",
    "v1",
    list_persistent_volumes,
    get_persistent_volume,
    create_persistent_volume,
    update_persistent_volume,
    delete_persistent_volume,
    patch_persistent_volume
);
namespaced_resource_handlers!(
    "ReplicationController",
    "ReplicationControllerList",
    "v1",
    list_replicationcontrollers,
    get_replicationcontroller,
    create_replicationcontroller_base,
    update_replicationcontroller,
    delete_replicationcontroller,
    patch_replicationcontroller,
    delete_collection_replicationcontrollers
);
namespaced_resource_handlers!(
    "LimitRange",
    "LimitRangeList",
    "v1",
    list_limitranges,
    get_limitrange,
    create_limitrange,
    update_limitrange,
    delete_limitrange,
    patch_limitrange,
    delete_collection_limitranges
);
namespaced_resource_handlers!(
    "ResourceQuota",
    "ResourceQuotaList",
    "v1",
    list_resourcequotas,
    get_resourcequota,
    create_resourcequota,
    update_resourcequota,
    delete_resourcequota,
    patch_resourcequota,
    delete_collection_resourcequotas
);

// apps/v1 resources (all namespaced)
namespaced_resource_handlers!(
    "Deployment",
    "DeploymentList",
    "apps/v1",
    list_deployments,
    get_deployment,
    create_deployment_base,
    update_deployment_base,
    delete_deployment,
    patch_deployment_base,
    delete_collection_deployments
);
namespaced_resource_handlers!(
    "ReplicaSet",
    "ReplicaSetList",
    "apps/v1",
    list_replicasets,
    get_replicaset,
    create_replicaset_base,
    update_replicaset_base,
    delete_replicaset,
    patch_replicaset_base,
    delete_collection_replicasets
);
namespaced_resource_handlers!(
    "StatefulSet",
    "StatefulSetList",
    "apps/v1",
    list_statefulsets,
    get_statefulset,
    create_statefulset_base,
    update_statefulset_base,
    delete_statefulset,
    patch_statefulset_base,
    delete_collection_statefulsets
);
namespaced_resource_handlers!(
    "DaemonSet",
    "DaemonSetList",
    "apps/v1",
    list_daemonsets,
    get_daemonset,
    create_daemonset_base,
    update_daemonset_base,
    delete_daemonset,
    patch_daemonset_base,
    delete_collection_daemonsets
);
namespaced_resource_handlers!(
    "ControllerRevision",
    "ControllerRevisionList",
    "apps/v1",
    list_controllerrevisions,
    get_controllerrevision,
    create_controllerrevision,
    update_controllerrevision,
    delete_controllerrevision,
    patch_controllerrevision,
    delete_collection_controllerrevisions
);

// batch/v1 resources (all namespaced)
// create/update/patch are _base variants; reconcile_handlers! wraps them (invoked below, after the macro
// definition) to trigger the Job controller on every mutation.
namespaced_resource_handlers!(
    "Job",
    "JobList",
    "batch/v1",
    list_jobs,
    get_job,
    create_job_base,
    update_job_base,
    delete_job,
    patch_job_base,
    delete_collection_jobs
);
namespaced_resource_handlers!(
    "CronJob",
    "CronJobList",
    "batch/v1",
    list_cronjobs,
    get_cronjob,
    create_cronjob,
    update_cronjob,
    delete_cronjob,
    patch_cronjob,
    delete_collection_cronjobs
);

// autoscaling/v1 resources
namespaced_resource_handlers!(
    "HorizontalPodAutoscaler",
    "HorizontalPodAutoscalerList",
    "autoscaling/v1",
    list_hpas_v1,
    get_hpa_v1,
    create_hpa_v1,
    update_hpa_v1,
    delete_hpa_v1,
    patch_hpa_v1,
    delete_collection_hpas_v1
);

// autoscaling/v2 resources
namespaced_resource_handlers!(
    "HorizontalPodAutoscaler",
    "HorizontalPodAutoscalerList",
    "autoscaling/v2",
    list_hpas_v2,
    get_hpa_v2,
    create_hpa_v2,
    update_hpa_v2,
    delete_hpa_v2,
    patch_hpa_v2,
    delete_collection_hpas_v2
);

// core v1 leases (namespaced) - separate from coordination.k8s.io/v1 leases
namespaced_resource_handlers!(
    "Lease",
    "LeaseList",
    "v1",
    list_leases_v1,
    get_lease_v1,
    create_lease_v1,
    update_lease_v1,
    delete_lease_v1,
    patch_lease_v1,
    delete_collection_leases_v1
);

// coordination.k8s.io/v1 resources (all namespaced)
namespaced_resource_handlers!(
    "Lease",
    "LeaseList",
    "coordination.k8s.io/v1",
    list_leases_coordination,
    get_lease_coordination,
    create_lease_coordination,
    update_lease_coordination,
    delete_lease_coordination,
    patch_lease_coordination,
    delete_collection_leases_coordination
);

// discovery.k8s.io/v1 resources (all namespaced)
namespaced_resource_handlers!(
    "EndpointSlice",
    "EndpointSliceList",
    "discovery.k8s.io/v1",
    list_endpointslices,
    get_endpointslice,
    create_endpointslice,
    update_endpointslice,
    delete_endpointslice,
    patch_endpointslice,
    delete_collection_endpointslices
);

// networking.k8s.io/v1 resources
namespaced_resource_handlers!(
    "Ingress",
    "IngressList",
    "networking.k8s.io/v1",
    list_ingresses,
    get_ingress,
    create_ingress,
    update_ingress,
    delete_ingress,
    patch_ingress,
    delete_collection_ingresses
);

// F1-01: NetworkPolicy is namespaced and follows the Ingress CRUD pattern.
namespaced_resource_handlers!(
    "NetworkPolicy",
    "NetworkPolicyList",
    "networking.k8s.io/v1",
    list_networkpolicies,
    get_networkpolicy,
    create_networkpolicy,
    update_networkpolicy,
    delete_networkpolicy,
    patch_networkpolicy,
    delete_collection_networkpolicies
);

cluster_resource_handlers!(
    "IngressClass",
    "IngressClassList",
    "networking.k8s.io/v1",
    list_ingressclasses,
    get_ingressclass,
    create_ingressclass,
    update_ingressclass,
    delete_ingressclass,
    patch_ingressclass
);

cluster_resource_handlers!(
    "ServiceCIDR",
    "ServiceCIDRList",
    "networking.k8s.io/v1",
    list_servicecidrs,
    get_servicecidr,
    create_servicecidr,
    update_servicecidr,
    delete_servicecidr,
    patch_servicecidr
);

cluster_resource_handlers!(
    "IPAddress",
    "IPAddressList",
    "networking.k8s.io/v1",
    list_ipaddresses,
    get_ipaddress,
    create_ipaddress,
    update_ipaddress,
    delete_ipaddress,
    patch_ipaddress
);

// storage.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "StorageClass",
    "StorageClassList",
    "storage.k8s.io/v1",
    list_storageclasses,
    get_storageclass,
    create_storageclass,
    update_storageclass,
    delete_storageclass,
    patch_storageclass
);

cluster_resource_handlers!(
    "VolumeAttachment",
    "VolumeAttachmentList",
    "storage.k8s.io/v1",
    list_volumeattachments,
    get_volumeattachment,
    create_volumeattachment,
    update_volumeattachment,
    delete_volumeattachment,
    patch_volumeattachment
);

cluster_resource_handlers!(
    "CSINode",
    "CSINodeList",
    "storage.k8s.io/v1",
    list_csinodes,
    get_csinode,
    create_csinode,
    update_csinode,
    delete_csinode,
    patch_csinode
);

cluster_resource_handlers!(
    "CSIDriver",
    "CSIDriverList",
    "storage.k8s.io/v1",
    list_csidrivers,
    get_csidriver,
    create_csidriver,
    update_csidriver,
    delete_csidriver,
    patch_csidriver
);

namespaced_resource_handlers!(
    "CSIStorageCapacity",
    "CSIStorageCapacityList",
    "storage.k8s.io/v1",
    list_csistoragecapacities,
    get_csistoragecapacity,
    create_csistoragecapacity,
    update_csistoragecapacity,
    delete_csistoragecapacity,
    patch_csistoragecapacity,
    delete_collection_csistoragecapacities
);

// node.k8s.io/v1 resources
cluster_resource_handlers!(
    "RuntimeClass",
    "RuntimeClassList",
    "node.k8s.io/v1",
    list_runtimeclasses,
    get_runtimeclass,
    create_runtimeclass,
    update_runtimeclass,
    delete_runtimeclass,
    patch_runtimeclass
);

// scheduling.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "PriorityClass",
    "PriorityClassList",
    "scheduling.k8s.io/v1",
    list_priorityclasses,
    get_priorityclass,
    create_priorityclass,
    update_priorityclass,
    delete_priorityclass,
    patch_priorityclass
);

// policy/v1 resources
namespaced_resource_handlers!(
    "PodDisruptionBudget",
    "PodDisruptionBudgetList",
    "policy/v1",
    list_poddisruptionbudgets,
    get_poddisruptionbudget,
    create_poddisruptionbudget_base,
    update_poddisruptionbudget_base,
    delete_poddisruptionbudget,
    patch_poddisruptionbudget_base,
    delete_collection_poddisruptionbudgets
);

// Custom PDB handlers with controller reconciliation
// rbac.authorization.k8s.io/v1 resources
namespaced_resource_handlers!(
    "Role",
    "RoleList",
    "rbac.authorization.k8s.io/v1",
    list_roles,
    get_role,
    create_role,
    update_role,
    delete_role,
    patch_role,
    delete_collection_roles
);
namespaced_resource_handlers!(
    "RoleBinding",
    "RoleBindingList",
    "rbac.authorization.k8s.io/v1",
    list_rolebindings,
    get_rolebinding,
    create_rolebinding,
    update_rolebinding,
    delete_rolebinding,
    patch_rolebinding,
    delete_collection_rolebindings
);
cluster_resource_handlers!(
    "ClusterRole",
    "ClusterRoleList",
    "rbac.authorization.k8s.io/v1",
    list_clusterroles,
    get_clusterrole,
    create_clusterrole,
    update_clusterrole,
    delete_clusterrole,
    patch_clusterrole
);
cluster_resource_handlers!(
    "ClusterRoleBinding",
    "ClusterRoleBindingList",
    "rbac.authorization.k8s.io/v1",
    list_clusterrolebindings,
    get_clusterrolebinding,
    create_clusterrolebinding,
    update_clusterrolebinding,
    delete_clusterrolebinding,
    patch_clusterrolebinding
);

// certificates.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "CertificateSigningRequest",
    "CertificateSigningRequestList",
    "certificates.k8s.io/v1",
    list_certificatesigningrequests,
    get_certificatesigningrequest,
    create_certificatesigningrequest,
    update_certificatesigningrequest,
    delete_certificatesigningrequest,
    patch_certificatesigningrequest
);

// apiregistration.k8s.io/v1 resources (cluster-scoped)
cluster_resource_handlers!(
    "APIService",
    "APIServiceList",
    "apiregistration.k8s.io/v1",
    list_apiservices,
    get_apiservice,
    create_apiservice,
    update_apiservice,
    delete_apiservice,
    patch_apiservice
);

// apiextensions.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "CustomResourceDefinition",
    "CustomResourceDefinitionList",
    "apiextensions.k8s.io/v1",
    list_customresourcedefinitions,
    get_customresourcedefinition,
    create_customresourcedefinition,
    update_customresourcedefinition,
    delete_customresourcedefinition,
    patch_customresourcedefinition
);

// Helper to add Established condition to CRD status
pub fn add_crd_established_condition(mut body: Value) -> Value {
    let now = crate::utils::k8s_timestamp();

    let established_condition = serde_json::json!({
        "type": "Established",
        "status": "True",
        "reason": "InitialNamesAccepted",
        "message": "the initial names have been accepted",
        "lastTransitionTime": now
    });

    let names_accepted_condition = serde_json::json!({
        "type": "NamesAccepted",
        "status": "True",
        "reason": "NoConflicts",
        "message": "no conflicts found",
        "lastTransitionTime": now
    });

    // Extract spec values before mutable borrow on status
    let accepted_names = body.pointer("/spec/names").cloned();
    let stored_versions: Vec<Value> = body
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .map(|versions| {
            versions
                .iter()
                .filter(|v| v.get("storage").and_then(|s| s.as_bool()).unwrap_or(false))
                .filter_map(|v| v.get("name").cloned())
                .collect()
        })
        .unwrap_or_default();

    // Ensure status.conditions exists
    ensure_object(&mut body, "status");
    let conditions = ensure_array(&mut body["status"], "conditions");
    conditions.push(established_condition);
    conditions.push(names_accepted_condition);
    // Get the status object for inserting other fields
    let status = body["status"]
        .as_object_mut()
        .expect("just ensured as object");

    // Set acceptedNames from spec.names
    if let Some(names) = accepted_names {
        status.insert("acceptedNames".to_string(), names);
    }

    // Set storedVersions
    if !stored_versions.is_empty() {
        status.insert("storedVersions".to_string(), Value::Array(stored_versions));
    }

    body
}

// Custom CRD create handler that registers the CRD in the registry
// admissionregistration.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "MutatingWebhookConfiguration",
    "MutatingWebhookConfigurationList",
    "admissionregistration.k8s.io/v1",
    list_mutatingwebhookconfigurations,
    get_mutatingwebhookconfiguration,
    create_mutatingwebhookconfiguration,
    update_mutatingwebhookconfiguration,
    delete_mutatingwebhookconfiguration,
    patch_mutatingwebhookconfiguration
);
cluster_resource_handlers!(
    "ValidatingWebhookConfiguration",
    "ValidatingWebhookConfigurationList",
    "admissionregistration.k8s.io/v1",
    list_validatingwebhookconfigurations,
    get_validatingwebhookconfiguration,
    create_validatingwebhookconfiguration,
    update_validatingwebhookconfiguration,
    delete_validatingwebhookconfiguration,
    patch_validatingwebhookconfiguration
);
cluster_resource_handlers!(
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyList",
    "admissionregistration.k8s.io/v1",
    list_validatingadmissionpolicies,
    get_validatingadmissionpolicy,
    create_validatingadmissionpolicy,
    update_validatingadmissionpolicy,
    delete_validatingadmissionpolicy,
    patch_validatingadmissionpolicy
);
cluster_resource_handlers!(
    "ValidatingAdmissionPolicyBinding",
    "ValidatingAdmissionPolicyBindingList",
    "admissionregistration.k8s.io/v1",
    list_validatingadmissionpolicybindings,
    get_validatingadmissionpolicybinding,
    create_validatingadmissionpolicybinding,
    update_validatingadmissionpolicybinding,
    delete_validatingadmissionpolicybinding,
    patch_validatingadmissionpolicybinding
);

// flowcontrol.apiserver.k8s.io/v1 resources (all cluster-scoped)
cluster_resource_handlers!(
    "FlowSchema",
    "FlowSchemaList",
    "flowcontrol.apiserver.k8s.io/v1",
    list_flowschemas,
    get_flowschema,
    create_flowschema,
    update_flowschema,
    delete_flowschema,
    patch_flowschema
);
cluster_resource_handlers!(
    "PriorityLevelConfiguration",
    "PriorityLevelConfigurationList",
    "flowcontrol.apiserver.k8s.io/v1",
    list_prioritylevelconfigurations,
    get_prioritylevelconfiguration,
    create_prioritylevelconfiguration,
    update_prioritylevelconfiguration,
    delete_prioritylevelconfiguration,
    patch_prioritylevelconfiguration
);

// storage.k8s.io/v1 cluster-scoped delete_collection (P0-E2E-20260424b-07)
cluster_delete_collection_handler!(
    delete_collection_persistent_volumes,
    "v1",
    "PersistentVolume"
);
cluster_delete_collection_handler!(delete_collection_csinodes, "storage.k8s.io/v1", "CSINode");
cluster_delete_collection_handler!(
    delete_collection_csidrivers,
    "storage.k8s.io/v1",
    "CSIDriver"
);
cluster_delete_collection_handler!(
    delete_collection_storageclasses,
    "storage.k8s.io/v1",
    "StorageClass"
);
cluster_delete_collection_handler!(
    delete_collection_volumeattachments,
    "storage.k8s.io/v1",
    "VolumeAttachment"
);
// scheduling.k8s.io/v1 cluster-scoped delete_collection
cluster_delete_collection_handler!(
    delete_collection_priorityclasses,
    "scheduling.k8s.io/v1",
    "PriorityClass"
);

// rbac.authorization.k8s.io/v1 cluster-scoped delete_collection
cluster_delete_collection_handler!(
    delete_collection_clusterroles,
    "rbac.authorization.k8s.io/v1",
    "ClusterRole"
);
cluster_delete_collection_handler!(
    delete_collection_clusterrolebindings,
    "rbac.authorization.k8s.io/v1",
    "ClusterRoleBinding"
);
cluster_delete_collection_handler!(
    delete_collection_ipaddresses,
    "networking.k8s.io/v1",
    "IPAddress"
);
cluster_delete_collection_handler!(
    delete_collection_certificatesigningrequests,
    "certificates.k8s.io/v1",
    "CertificateSigningRequest"
);

// Cluster-scoped delete_collection handlers (not in macro)
// Macro to generate cluster-wide list handlers (GET /api/v1/pods, etc.)
// These list resources across ALL namespaces (namespace=None in DB query).
macro_rules! cluster_wide_list_handler {
    ($kind:expr_2021, $list_kind:expr_2021, $api_version:expr_2021, $fn_name:ident) => {
        pub async fn $fn_name(
            State(state): State<Arc<AppState>>,
            Query(query): Query<ListQuery>,
            headers: HeaderMap,
        ) -> Result<Response, AppError> {
            validate_builtin_field_selector(
                $api_version,
                $kind,
                query.label_selector.as_deref(),
                query.field_selector.as_deref(),
                true,
            )?;
            // Watch streaming for cluster-wide list (all namespaces)
            if query.watch == Some("true".to_string()) {
                query.validate_send_initial_events_watch()?;
                let kind = $kind.to_string();
                let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
                let table_format = wants_table_format(&headers)?;
                let label_selector = query.label_selector.clone();
                let field_selector = query.field_selector.clone();

                let mut requested_rv: i64 = query.resource_version
                    .as_ref()
                    .and_then(|rv| rv.parse::<i64>().ok())
                    .unwrap_or(0);
                let explicit_resource_version_zero = query
                    .resource_version
                    .as_deref()
                    .is_some_and(|rv| rv.trim() == "0");
                let send_initial_events = query.send_initial_events.as_deref() == Some("true");
                let has_selector = label_selector
                    .as_deref()
                    .is_some_and(|s| !s.trim().is_empty())
                    || field_selector
                        .as_deref()
                        .is_some_and(|s| !s.trim().is_empty());

                // Selector-less rv-less watches pin requested_rv to the
                // pre-subscribe global rv so the stream starts "from now";
                // selector rv-less watches keep the floor at 0 and dedup the
                // baseline by exact rv in the stream builder.
                if requested_rv <= 0
                    && !send_initial_events
                    && !has_selector
                    && !explicit_resource_version_zero
                    && let Ok(floor) = state.db.get_current_resource_version().await
                    && floor > 0
                {
                    requested_rv = floor;
                }

                let signal_rx = state
                    .db
                    .subscribe_watch_signals(crate::watch::WatchTopic::new($api_version, &kind));
                let db = state.db.clone();

                let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
                    db,
                    signal_rx,
                    task_supervisor: state.task_supervisor.clone(),
                    api_version: $api_version,
                    kind,
                    watch_namespace: None,
                    requested_rv,
                    send_initial_events,
                    send_bookmarks,
                    label_selector,
                    field_selector,
                    table_format,
                    catch_up_mode: WatchCatchUpMode::NamespacedScoped,
                    timeout_seconds: query.timeout_seconds,
                    emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
                });
                return Ok(Response::builder()
                    .header("Content-Type", "application/json")
                    .header("Transfer-Encoding", "chunked")
                    .body(body)
                    .unwrap());
            }

            let normalized_limit = query.normalized_limit()?;

            let has_continue = query
                .continue_token
                .as_deref()
                .is_some_and(|t| !t.is_empty());
            let rv_match = query.resolve_resource_version_match(has_continue)?;

            // Decode continue token: check TTL and extract name for DB filter.
            let (db_continue_name, continue_resource_version) =
                process_continue_token(query.continue_token)?;

            let list_query = crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                query.field_selector.as_deref(),
                normalized_limit,
                db_continue_name.as_deref(),
            );

            // Cluster-wide (all-namespaces) collection: same consistent-snapshot
            // path as the namespaced handler, with no namespace scope. Pages 2+
            // are served from the pinned session snapshot, not current state.
            // See `query::resolve_list_page`.
            let db_for_snapshot = state.db.clone();
            let db_for_live = state.db.clone();
            let crate::api::query::ResolvedListPage {
                list,
                response_rv,
                continue_resource_version,
            } = crate::api::query::resolve_list_page(
                state.db.as_ref(),
                rv_match,
                continue_resource_version,
                |srv| async move {
                    db_for_snapshot
                        .snapshot_resources_at_rv($api_version, $kind, None, list_query, srv)
                        .await
                        .map_err(AppError::from)
                },
                || async move {
                    db_for_live
                        .list_resources($api_version, $kind, None, list_query)
                        .await
                        .map_err(AppError::from)
                },
            )
            .await?;

            let items: Vec<Value> = list
                .items
                .into_iter()
                .map(|r| inject_resource_version(r.data, r.resource_version))
                .collect();
            let resource_version = response_rv.to_string();

            // Return Table format if requested by kubectl
            if wants_table_format(&headers)? {
                let table = match $kind {
                    "Pod" => pod_list_to_table(items, resource_version),
                    "Node" => node_list_to_table(items, resource_version),
                    "ReplicaSet" => replicaset_list_to_table(items, resource_version),
                    "Deployment" => deployment_list_to_table(items, resource_version),
                    "StatefulSet" => statefulset_list_to_table(items, resource_version),
                    // Resources without a dedicated converter use kubectl's
                    // per-kind columns, falling back to the upstream default
                    // (NAME + CREATED AT) for kinds with no custom printer.
                    _ => generic_list_to_table($kind, items, resource_version),
                };
                return Ok(Json(table).into_response());
            }

            // Return normal List format
            // Omit "continue" when None; include "remainingItemCount" only when paginating.
            let mut metadata = serde_json::json!({
                "resourceVersion": resource_version,
            });
            if let Some(ref name) = list.continue_token {
                let token = crate::api::query::encode_response_continue_token(
                    name,
                    response_rv,
                    continue_resource_version,
                );
                metadata["continue"] = serde_json::json!(token);
            }
            if let Some(remaining) = list.remaining_item_count {
                metadata["remainingItemCount"] = serde_json::json!(remaining);
            }
            let response = serde_json::json!({
                "apiVersion": $api_version,
                "kind": $list_kind,
                "metadata": metadata,
                "items": items,
            });

            Ok(K8sResponse::new(response, &headers).into_response())
        }
    };
}

// Cluster-wide list handlers for namespaced core v1 resources
// Note: list_all_pods is extracted in src/api/pod_handlers.rs (Task 11 Step A).
cluster_wide_list_handler!("Service", "ServiceList", "v1", list_all_services);
cluster_wide_list_handler!("Endpoints", "EndpointsList", "v1", list_all_endpoints);
cluster_wide_list_handler!("ConfigMap", "ConfigMapList", "v1", list_all_configmaps);
cluster_wide_list_handler!("Secret", "SecretList", "v1", list_all_secrets);
cluster_wide_list_handler!("Event", "EventList", "v1", list_all_events);

// events.k8s.io/v1 resources (namespaced)
namespaced_resource_handlers!(
    "Event",
    "EventList",
    "events.k8s.io/v1",
    list_events_k8s_io,
    get_event_k8s_io,
    create_event_k8s_io,
    update_event_k8s_io,
    delete_event_k8s_io,
    patch_event_k8s_io,
    delete_collection_events_k8s_io
);
cluster_wide_list_handler!(
    "Event",
    "EventList",
    "events.k8s.io/v1",
    list_all_events_k8s_io
);

cluster_wide_list_handler!(
    "ServiceAccount",
    "ServiceAccountList",
    "v1",
    list_all_serviceaccounts
);
cluster_wide_list_handler!(
    "PersistentVolumeClaim",
    "PersistentVolumeClaimList",
    "v1",
    list_all_pvcs
);
cluster_wide_list_handler!("Lease", "LeaseList", "v1", list_all_leases_v1);
cluster_wide_list_handler!(
    "ReplicationController",
    "ReplicationControllerList",
    "v1",
    list_all_replicationcontrollers
);
cluster_wide_list_handler!(
    "PodTemplate",
    "PodTemplateList",
    "v1",
    list_all_podtemplates
);
cluster_wide_list_handler!("LimitRange", "LimitRangeList", "v1", list_all_limitranges);
cluster_wide_list_handler!(
    "ResourceQuota",
    "ResourceQuotaList",
    "v1",
    list_all_resourcequotas
);

// Cluster-wide list handlers for namespaced apps/v1 resources
cluster_wide_list_handler!(
    "Deployment",
    "DeploymentList",
    "apps/v1",
    list_all_deployments
);
cluster_wide_list_handler!(
    "ReplicaSet",
    "ReplicaSetList",
    "apps/v1",
    list_all_replicasets
);
cluster_wide_list_handler!(
    "StatefulSet",
    "StatefulSetList",
    "apps/v1",
    list_all_statefulsets
);
cluster_wide_list_handler!("DaemonSet", "DaemonSetList", "apps/v1", list_all_daemonsets);
cluster_wide_list_handler!(
    "ControllerRevision",
    "ControllerRevisionList",
    "apps/v1",
    list_all_controllerrevisions
);

// Cluster-wide list handlers for other namespaced resources
cluster_wide_list_handler!(
    "HorizontalPodAutoscaler",
    "HorizontalPodAutoscalerList",
    "autoscaling/v1",
    list_all_hpas_v1
);
cluster_wide_list_handler!(
    "HorizontalPodAutoscaler",
    "HorizontalPodAutoscalerList",
    "autoscaling/v2",
    list_all_hpas_v2
);
cluster_wide_list_handler!("Job", "JobList", "batch/v1", list_all_jobs);
cluster_wide_list_handler!("CronJob", "CronJobList", "batch/v1", list_all_cronjobs);
cluster_wide_list_handler!(
    "Lease",
    "LeaseList",
    "coordination.k8s.io/v1",
    list_all_leases_coordination
);
cluster_wide_list_handler!(
    "EndpointSlice",
    "EndpointSliceList",
    "discovery.k8s.io/v1",
    list_all_endpointslices
);
cluster_wide_list_handler!(
    "Role",
    "RoleList",
    "rbac.authorization.k8s.io/v1",
    list_all_roles
);
cluster_wide_list_handler!(
    "RoleBinding",
    "RoleBindingList",
    "rbac.authorization.k8s.io/v1",
    list_all_rolebindings
);
cluster_wide_list_handler!(
    "Ingress",
    "IngressList",
    "networking.k8s.io/v1",
    list_all_ingresses
);
cluster_wide_list_handler!(
    "NetworkPolicy",
    "NetworkPolicyList",
    "networking.k8s.io/v1",
    list_all_networkpolicies
);
cluster_wide_list_handler!(
    "ServiceCIDR",
    "ServiceCIDRList",
    "networking.k8s.io/v1",
    list_all_servicecidrs
);
cluster_wide_list_handler!(
    "IPAddress",
    "IPAddressList",
    "networking.k8s.io/v1",
    list_all_ipaddresses
);
cluster_wide_list_handler!(
    "PodDisruptionBudget",
    "PodDisruptionBudgetList",
    "policy/v1",
    list_all_poddisruptionbudgets
);
cluster_wide_list_handler!(
    "CSIStorageCapacity",
    "CSIStorageCapacityList",
    "storage.k8s.io/v1",
    list_all_csistoragecapacities
);
