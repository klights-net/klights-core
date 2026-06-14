mod basics;
mod blocking;
mod configmap_secret;
mod downward_api;
mod projected;
pub(crate) mod shared;

#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_downward;
#[cfg(test)]
mod tests_projected_a;
#[cfg(test)]
mod tests_projected_b;
#[cfg(test)]
mod tests_refresh_subpath;

pub use basics::unmount_volume_mounts_under;
pub use basics::{
    create_empty_dir, create_empty_dir_for_namespace, empty_dir_volume_path,
    empty_dir_volume_path_for_namespace, resolve_host_path,
};
pub use basics::{
    parse_k8s_quantity, validate_volume_projection_paths, validate_volume_subpaths, volumes_root,
};
pub use blocking::run_blocking_fs_keyed;
pub use configmap_secret::{create_config_map_volume, create_secret_volume};
pub use configmap_secret::{
    refresh_secret_configmap_volumes_after_delete, refresh_secret_configmap_volumes_from_event,
};
pub use downward_api::refresh_downward_api_volumes;
pub use downward_api::{DownwardApiVolumeNsRequest, create_downward_api_volume_ns};
pub use projected::{ProjectedVolumeNsRequest, create_projected_volume_ns};

#[cfg(test)]
pub use basics::parse_mountinfo_entry;
#[cfg(test)]
pub use blocking::blocking_fs_keyed_call_count;
#[cfg(test)]
pub use blocking::blocking_fs_keyed_call_count_for;
#[cfg(test)]
pub use configmap_secret::{
    ConfigMapVolumeAtRequest, SecretVolumeAtRequest, create_config_map_volume_at,
    create_secret_volume_at,
};
#[cfg(test)]
pub use downward_api::{
    DownwardApiVolumeWithDbNameRequest, create_downward_api_volume_at,
    create_downward_api_volume_at_with_db_name, extract_field_ref, extract_resource_field_ref,
};
#[cfg(test)]
pub use projected::{ProjectedVolumeAtRequest, create_projected_volume_at};
