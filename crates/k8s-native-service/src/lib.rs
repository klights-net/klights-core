//! Sole owner of the disposable Kubernetes-native HTTP service.
//!
//! The permanent API-server shell composes this crate through focused ports and
//! consumes its opaque router handoff; the private `current` implementation is
//! not a compatibility surface.

pub mod admission;
pub mod audit;
pub mod auth_http;
mod current;
pub mod discovery;
mod error;
mod extractor;
pub mod generic_command;
pub mod generic_read;
mod identity;
pub mod policy_inputs;
pub mod priority_fairness;
pub mod request_info;
mod resource_projection;
pub mod response;
mod router;
mod state;
pub mod status;
pub mod streaming;
pub mod subresources;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod watch;

pub use current::{
    AdmissionContextRequest, AdmissionResourceStore, ApiRuntimeInputs, ApiRuntimePaths,
    DeleteOptions, DeletePreconditions, NamespaceCreateEligibility, NamespaceTerminationOutcome,
    NativeApiOuterLayers, NativeApiRemoteNodeServices, PodApiService, PodApiServiceDependencies,
    PodNativeOrchestration, PodNativeOrchestrationDependencies, PodSubresourceService,
    apply_default_storage_class_admission, apply_limitrange_defaults_to_pod, apply_patch,
    apply_pod_runtimeclass_admission, apply_pod_service_account_defaults,
    apply_pod_spec_create_defaults, build_admission_context, build_current_router,
    check_resource_quota_for_creation, check_resource_quota_for_pod_update, classify_namespace,
    compute_qos_class, enforce_limitrange_constraints_for_pod,
    enforce_limitrange_constraints_for_pvc, enforce_pod_security_admission,
    normalize_resource_for_storage, parse_delete_options_body, parse_delete_options_protobuf,
    reconcile_namespace_termination_at, reconcile_namespace_termination_for_uid_with_outcome_at,
    resolve_resource_name, run_admission_for_request, set_namespace_terminating_status_at,
    validate_builtin_resource_spec, validate_dns_subdomain,
    validate_pod_resource_requirements_immutable, validate_pod_sysctls,
};
pub use discovery::{
    api_group_by_name, api_groups, custom_resource_discovery, get_openapi_v2,
    get_openapi_v3_api_v1, get_openapi_v3_discovery, get_openapi_v3_group_version,
};
pub use error::{
    AppError, StatusCause, map_mutating_admission_error, map_validating_admission_error,
};
pub use extractor::LenientJson;
pub use identity::ApiIdentityGenerator;
pub use resource_projection::inject_resource_version;
pub use response::K8sResponse;
pub use router::CurrentRouter;
pub use state::ApiState;
pub use streaming::StreamingDependencies;

/// Encoding surface used only by the base-owned native API composition suite.
/// Keeping this with the native-service owner avoids giving the root package a
/// direct public-protobuf development edge.
#[cfg(any(test, feature = "test-support"))]
pub mod test_protobuf {
    pub use prost::Message;

    pub type Unknown = klights_kube_protobuf::Unknown;
    pub type TypeMeta = klights_kube_protobuf::TypeMeta;

    pub fn encode_protobuf(value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        klights_kube_protobuf::encode_protobuf(value).map_err(anyhow::Error::new)
    }

    pub fn decode_protobuf(data: &[u8]) -> anyhow::Result<serde_json::Value> {
        klights_kube_protobuf::decode_protobuf(data).map_err(anyhow::Error::new)
    }

    pub mod api {
        pub mod admissionregistration {
            pub mod v1 {
                pub type ValidatingAdmissionPolicyBinding = klights_kube_protobuf::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding;
                pub type ValidatingAdmissionPolicyBindingSpec = klights_kube_protobuf::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingSpec;
            }
        }
        pub mod core {
            pub mod v1 {
                pub type ConfigMap = klights_kube_protobuf::api::core::v1::ConfigMap;
                pub type Container = klights_kube_protobuf::api::core::v1::Container;
                pub type Endpoints = klights_kube_protobuf::api::core::v1::Endpoints;
                pub type ObjectReference = klights_kube_protobuf::api::core::v1::ObjectReference;
                pub type PersistentVolume = klights_kube_protobuf::api::core::v1::PersistentVolume;
                pub type PersistentVolumeClaim =
                    klights_kube_protobuf::api::core::v1::PersistentVolumeClaim;
                pub type PersistentVolumeClaimSpec =
                    klights_kube_protobuf::api::core::v1::PersistentVolumeClaimSpec;
                pub type PersistentVolumeSpec =
                    klights_kube_protobuf::api::core::v1::PersistentVolumeSpec;
                pub type PersistentVolumeStatus =
                    klights_kube_protobuf::api::core::v1::PersistentVolumeStatus;
                pub type Pod = klights_kube_protobuf::api::core::v1::Pod;
                pub type PodSpec = klights_kube_protobuf::api::core::v1::PodSpec;
                pub type VolumeResourceRequirements =
                    klights_kube_protobuf::api::core::v1::VolumeResourceRequirements;
            }
        }
        pub mod events {
            pub mod v1 {
                pub type Event = klights_kube_protobuf::api::events::v1::Event;
            }
        }
        pub mod networking {
            pub mod v1 {
                pub type Ingress = klights_kube_protobuf::api::networking::v1::Ingress;
            }
        }
        pub mod storage {
            pub mod v1 {
                pub type CSIStorageCapacityList =
                    klights_kube_protobuf::api::storage::v1::CSIStorageCapacityList;
            }
        }
    }

    pub mod apimachinery {
        pub mod pkg {
            pub mod api {
                pub mod resource {
                    pub type Quantity =
                        klights_kube_protobuf::apimachinery::pkg::api::resource::Quantity;
                }
            }
            pub mod apis {
                pub mod meta {
                    pub mod v1 {
                        pub type MicroTime =
                            klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::MicroTime;
                        pub type ObjectMeta =
                            klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::ObjectMeta;
                        pub type Status =
                            klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::Status;
                        pub type WatchEvent =
                            klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::WatchEvent;
                    }
                }
            }
        }
    }
}

/// Focused root-adapter contracts used to compose the current native service.
pub mod ports {
    pub use crate::current::custom_resource_ports::{
        CustomResourceListSnapshot, CustomResourceProjection, CustomResourceReadFuture,
        CustomResourceReadPort, CustomResourceSnapshotRequest, CustomResourceWaitFuture,
        CustomResourceWatchTarget, added_watch_event, resource_event_to_watch_event,
    };
    pub use crate::current::generated_handler_ports::{GeneratedWatchPort, GeneratedWatchRequest};
    pub use crate::current::helpers::AdmissionResourceStore;
    pub use crate::current::state_ports::{
        ApiFailureEntry, ApiFailureMetrics, ApiNodeLeaseObservations, ApiNodeLeaseObservedFuture,
        ApiPodRepository,
    };
}
