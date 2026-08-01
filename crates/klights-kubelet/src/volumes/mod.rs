mod basics;
mod blocking;
mod configmap_secret;
mod downward_api;
mod projected;
pub(crate) mod shared;

pub fn pod_volume_dir_id(namespace: &str, name: &str, uid: &str) -> String {
    format!("{namespace}_{name}_{uid}")
}

pub use basics::unmount_volume_mounts_under;
pub use basics::{
    create_empty_dir_under_root, empty_dir_volume_path_under_root, resolve_host_path,
};
pub use basics::{parse_k8s_quantity, validate_volume_projection_paths, validate_volume_subpaths};
pub use blocking::run_blocking_fs_keyed;
pub use configmap_secret::{create_config_map_volume, create_secret_volume};
pub use configmap_secret::{
    refresh_secret_configmap_volumes_after_delete, refresh_secret_configmap_volumes_from_event,
};
pub use downward_api::refresh_downward_api_volumes;
pub use downward_api::{DownwardApiVolumeNsRequest, create_downward_api_volume_ns};
pub use projected::{ProjectedVolumeNsRequest, create_projected_volume_ns};
pub(crate) use projected::{ProjectedVolumeRootRequest, create_projected_volume_under_root};

#[cfg(any(test, feature = "test-support"))]
pub use basics::parse_mountinfo_entry;
#[cfg(any(test, feature = "test-support"))]
pub use basics::{
    create_empty_dir, create_empty_dir_for_namespace, empty_dir_volume_path,
    empty_dir_volume_path_for_namespace, volumes_root,
};
#[cfg(any(test, feature = "test-support"))]
pub use blocking::blocking_fs_keyed_call_count;
#[cfg(any(test, feature = "test-support"))]
pub use blocking::blocking_fs_keyed_call_count_for;
#[cfg(any(test, feature = "test-support"))]
pub use configmap_secret::{
    ConfigMapVolumeAtRequest, SecretVolumeAtRequest, create_config_map_volume_at,
    create_secret_volume_at,
};
#[cfg(any(test, feature = "test-support"))]
pub use downward_api::{
    DownwardApiVolumeWithDbNameRequest, create_downward_api_volume_at,
    create_downward_api_volume_at_with_db_name, extract_field_ref, extract_resource_field_ref,
};
#[cfg(any(test, feature = "test-support"))]
pub use projected::{ProjectedVolumeAtRequest, create_projected_volume_at};
