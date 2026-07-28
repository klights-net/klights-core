use klights_leader_api::{
    ControlplaneJoinAdmission, ControlplaneJoinAuthority, ControlplaneJoinHandler,
    ControlplaneJoinMetadata, ControlplaneJoinRegistration, ControlplaneMemberQuery,
    RemoteNodeHostFacts,
};

fn assert_handler_object_safe(_: Option<&dyn ControlplaneJoinHandler>) {}
fn assert_authority_object_safe(_: Option<&dyn ControlplaneJoinAuthority>) {}
fn assert_admission_object_safe(_: Option<&dyn ControlplaneJoinAdmission>) {}
fn assert_member_query_object_safe(_: Option<&dyn ControlplaneMemberQuery>) {}
fn assert_registration_object_safe(_: Option<&dyn ControlplaneJoinRegistration>) {}
fn assert_metadata_object_safe(_: Option<&dyn ControlplaneJoinMetadata>) {}

#[test]
fn focused_join_capabilities_are_independently_object_safe() {
    assert_handler_object_safe(None);
    assert_authority_object_safe(None);
    assert_admission_object_safe(None);
    assert_member_query_object_safe(None);
    assert_registration_object_safe(None);
    assert_metadata_object_safe(None);
}

#[test]
fn remote_host_validation_preserves_join_boundary_rules() {
    let mut host = RemoteNodeHostFacts {
        cpu_count: 2,
        memory_ki: 1024,
        architecture: "x86_64".to_string(),
        operating_system: "linux".to_string(),
        os_image: "Test Linux".to_string(),
        kernel_version: "6.1.0".to_string(),
        container_runtime_version: "containerd://1.7.0".to_string(),
        kubelet_version: "v1.34.0".to_string(),
        git_commit: "abc123".to_string(),
    };
    host.validate().expect("valid host facts");

    host.cpu_count = 0;
    assert_eq!(
        host.validate().unwrap_err().to_string(),
        "node registration cpu_count must be positive"
    );
}
