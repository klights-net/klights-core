use super::*;

#[test]
fn test_build_container_config_fieldref_spec_node_name() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "spec": {
            "nodeName": "worker-node-1"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "NODE_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "spec.nodeName"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let node_name_env = config
        .envs
        .iter()
        .find(|e| e.key == "NODE_NAME")
        .expect("NODE_NAME env should exist");
    assert_eq!(node_name_env.value, "worker-node-1");
}

#[test]
fn test_build_container_config_fieldref_spec_service_account_name() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "spec": {
            "serviceAccountName": "my-service-account"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "SERVICE_ACCOUNT",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "spec.serviceAccountName"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let sa_env = config
        .envs
        .iter()
        .find(|e| e.key == "SERVICE_ACCOUNT")
        .expect("SERVICE_ACCOUNT env should exist");
    assert_eq!(sa_env.value, "my-service-account");
}

#[test]
fn test_build_container_config_fieldref_status_pod_ip() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "status": {
            "podIP": "10.43.0.5"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "POD_IP",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "status.podIP"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let pod_ip_env = config
        .envs
        .iter()
        .find(|e| e.key == "POD_IP")
        .expect("POD_IP env should exist");
    assert_eq!(pod_ip_env.value, "10.43.0.5");
}

#[test]
fn test_build_container_config_fieldref_labels() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123",
            "labels": {
                "app": "myapp",
                "version": "v1.0"
            }
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "APP_LABEL",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.labels['app']"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let app_label_env = config
        .envs
        .iter()
        .find(|e| e.key == "APP_LABEL")
        .expect("APP_LABEL env should exist");
    assert_eq!(app_label_env.value, "myapp");
}

#[test]
fn test_build_container_config_fieldref_label_with_slash() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123",
            "labels": {
                "app.kubernetes.io/name": "myapp"
            }
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "K8S_APP_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.labels['app.kubernetes.io/name']"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let env = config
        .envs
        .iter()
        .find(|e| e.key == "K8S_APP_NAME")
        .expect("K8S_APP_NAME env should exist");
    assert_eq!(env.value, "myapp");
}

#[test]
fn test_build_container_config_resource_field_ref_limits_cpu() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "resources": {
            "limits": {
                "cpu": "2",
                "memory": "1Gi"
            }
        },
        "env": [
            {
                "name": "CPU_LIMIT",
                "valueFrom": {
                    "resourceFieldRef": {
                        "resource": "limits.cpu"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let cpu_limit_env = config
        .envs
        .iter()
        .find(|e| e.key == "CPU_LIMIT")
        .expect("CPU_LIMIT env should exist");
    assert_eq!(cpu_limit_env.value, "2");
}

#[test]
fn test_build_container_config_security_context_run_as_user() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "securityContext": {
            "runAsUser": 1000,
            "runAsGroup": 2000
        }
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    // Verify security context is set
    assert!(config.linux.is_some());
    let linux = config.linux.unwrap();
    assert!(linux.security_context.is_some());
    let sec_ctx = linux.security_context.unwrap();

    assert_eq!(sec_ctx.run_as_user.map(|v| v.value), Some(1000));
    assert_eq!(sec_ctx.run_as_group.map(|v| v.value), Some(2000));
}

#[test]
fn test_build_container_config_security_context_container_overrides_pod() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "spec": {
            "securityContext": {
                "runAsUser": 500,
                "runAsGroup": 600
            }
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "securityContext": {
            "runAsUser": 1000
        }
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    // Container-level runAsUser should override pod-level
    let sec_ctx = config.linux.unwrap().security_context.unwrap();
    assert_eq!(sec_ctx.run_as_user.map(|v| v.value), Some(1000));
    // Pod-level runAsGroup should be used since container didn't specify
    assert_eq!(sec_ctx.run_as_group.map(|v| v.value), Some(600));
}

#[test]
fn test_build_container_config_security_context_no_new_privs() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "privileged": false
        }
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let sec_ctx = config.linux.unwrap().security_context.unwrap();
    // allowPrivilegeEscalation=false should set no_new_privs=true
    assert!(sec_ctx.no_new_privs);
    assert!(sec_ctx.readonly_rootfs);
    assert!(!(sec_ctx.privileged));
}

#[test]
fn test_build_container_config_unset_allow_priv_esc_defaults_to_true() {
    // K8s conformance: allowPrivilegeEscalation not explicitly set → default true
    // (no_new_privs = false), allowing setuid executables to gain root.
    let pod_data = serde_json::json!({
        "metadata": {"name": "p", "namespace": "default", "uid": "u1"},
        "spec": {"securityContext": {"runAsUser": 1000}}
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "alpine",
        "securityContext": {}  // no allowPrivilegeEscalation key
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    let sec_ctx = config.linux.unwrap().security_context.unwrap();
    assert!(
        !(sec_ctx.no_new_privs),
        "unset allowPrivilegeEscalation must NOT set no_new_privs"
    );
}

#[test]
fn test_build_container_config_working_dir_from_spec() {
    let pod_data = serde_json::json!({
        "metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}
    });
    let container_spec = serde_json::json!({
        "image": "app",
        "workingDir": "/app"
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert_eq!(config.working_dir, "/app");
}

#[test]
fn test_build_container_config_working_dir_absent_is_empty() {
    let pod_data = serde_json::json!({
        "metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}
    });
    let container_spec = serde_json::json!({"image": "app"});
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert_eq!(config.working_dir, "");
}

#[test]
fn test_build_container_config_tty_from_spec() {
    let pod_data = serde_json::json!({
        "metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}
    });
    let container_spec = serde_json::json!({
        "image": "app",
        "tty": true
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert!(config.tty);
}

#[test]
fn test_build_container_config_stdin_from_spec() {
    let pod_data = serde_json::json!({
        "metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}
    });
    let container_spec = serde_json::json!({
        "image": "app",
        "stdin": true
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert!(config.stdin);
}

#[test]
fn test_run_as_non_root_rejects_uid_zero() {
    // runAsNonRoot:true with no runAsUser → should reject (defaults to root)
    let pod_data = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "securityContext": {"runAsNonRoot": true}
        }
    });
    let container_spec = serde_json::json!({"name": "app", "image": "nginx"});
    let result = check_run_as_non_root(&pod_data, &container_spec, "app");
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("runAsNonRoot"),
        "Error should mention runAsNonRoot: {}",
        err_msg
    );

    // runAsNonRoot:true with explicit runAsUser:0 → should reject
    let pod_data2 = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "securityContext": {"runAsNonRoot": true, "runAsUser": 0}
        }
    });
    let result2 = check_run_as_non_root(&pod_data2, &container_spec, "app");
    assert!(result2.is_err());

    // Container-level runAsNonRoot:true overrides pod-level
    let pod_data3 = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {}
    });
    let container_spec3 = serde_json::json!({
        "name": "app", "image": "nginx",
        "securityContext": {"runAsNonRoot": true}
    });
    let result3 = check_run_as_non_root(&pod_data3, &container_spec3, "app");
    assert!(result3.is_err());
}

#[test]
fn test_run_as_non_root_allows_non_zero_uid() {
    // runAsNonRoot:true with runAsUser:1000 → should allow
    let pod_data = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "securityContext": {"runAsNonRoot": true, "runAsUser": 1000}
        }
    });
    let container_spec = serde_json::json!({"name": "app", "image": "nginx"});
    let result = check_run_as_non_root(&pod_data, &container_spec, "app");
    assert!(result.is_ok());

    // Container-level runAsUser overrides pod-level
    let pod_data2 = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "securityContext": {"runAsNonRoot": true, "runAsUser": 0}
        }
    });
    let container_spec2 = serde_json::json!({
        "name": "app", "image": "nginx",
        "securityContext": {"runAsUser": 1000}
    });
    let result2 = check_run_as_non_root(&pod_data2, &container_spec2, "app");
    assert!(result2.is_ok());

    // runAsNonRoot:false or absent → always allow
    let pod_data3 = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {}
    });
    let result3 = check_run_as_non_root(&pod_data3, &container_spec, "app");
    assert!(result3.is_ok());
}

#[test]
fn test_build_container_config_pod_level_run_as_user_no_container_secctx() {
    // Pod has securityContext.runAsUser:1001, container has NO securityContext at all
    // The container must still run as uid=1001, not root.
    let pod_data = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default", "uid": "abc-123"},
        "spec": {
            "securityContext": {
                "runAsUser": 1001,
                "runAsGroup": 1001
            }
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "busybox:latest"
        // NO securityContext field at all
    });

    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let linux = config.linux.expect("linux config must be set");
    let sec_ctx = linux
        .security_context
        .expect("security_context must be set when pod has securityContext");
    assert_eq!(
        sec_ctx.run_as_user.map(|v| v.value),
        Some(1001),
        "pod-level runAsUser=1001 must be passed to container (got uid=0 instead)"
    );
    assert_eq!(
        sec_ctx.run_as_group.map(|v| v.value),
        Some(1001),
        "pod-level runAsGroup=1001 must be passed to container"
    );
}

#[test]
fn test_build_container_config_container_level_run_as_user_overrides_pod_level() {
    // Pod has securityContext.runAsUser:1001, container has securityContext.runAsUser:1002.
    // Container-level MUST override pod-level. Sonobuoy test: expected uid=1002 got uid=0.
    let pod_data = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default", "uid": "abc-123"},
        "spec": {
            "securityContext": {
                "runAsUser": 1001,
                "runAsGroup": 1001
            }
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "busybox:latest",
        "securityContext": {
            "runAsUser": 1002
        }
    });

    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let linux = config.linux.expect("linux config must be set");
    let sec_ctx = linux
        .security_context
        .expect("security_context must be set");
    assert_eq!(
        sec_ctx.run_as_user.map(|v| v.value),
        Some(1002),
        "container-level runAsUser=1002 must override pod-level runAsUser=1001"
    );
    // Pod-level runAsGroup=1001 should be used since container doesn't specify runAsGroup
    assert_eq!(
        sec_ctx.run_as_group.map(|v| v.value),
        Some(1001),
        "pod-level runAsGroup=1001 should be inherited when container doesn't specify"
    );
}

#[test]
fn test_build_container_config_container_level_run_as_user_no_pod_secctx() {
    // Container has securityContext.runAsUser:1002, pod has NO pod-level securityContext.
    // The container must run as uid=1002, not uid=0.
    let pod_data = serde_json::json!({
        "metadata": {"name": "test-pod", "namespace": "default", "uid": "abc-123"},
        "spec": {
            "containers": [{"name": "app", "image": "busybox:latest"}]
            // NO spec.securityContext
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "busybox:latest",
        "securityContext": {
            "runAsUser": 1002
        }
    });

    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let linux = config.linux.expect("linux config must be set");
    let sec_ctx = linux
        .security_context
        .expect("security_context must be set when container has securityContext");
    assert_eq!(
        sec_ctx.run_as_user.map(|v| v.value),
        Some(1002),
        "container-level runAsUser=1002 must be passed to CRI (expected uid=1002 got uid=0)"
    );
}

#[test]
fn test_build_container_config_args_expand_env_vars() {
    let spec = serde_json::json!({
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo $(MESSAGE)"],
        "env": [
            {"name": "GREETING", "value": "hello"},
            {"name": "MESSAGE", "value": "$(GREETING) world"}
        ]
    });
    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(
        &spec,
        &pod_data,
        "busybox",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        config.args,
        vec!["echo hello world"],
        "$(MESSAGE) in args should expand using resolved env vars"
    );
}

#[test]
fn test_build_container_config_command_expand_env_vars() {
    let spec = serde_json::json!({
        "image": "busybox",
        "command": ["$(CMD_PATH)"],
        "args": [],
        "env": [
            {"name": "CMD_PATH", "value": "/usr/bin/myapp"}
        ]
    });
    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(
        &spec,
        &pod_data,
        "busybox",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        config.command,
        vec!["/usr/bin/myapp"],
        "$(CMD_PATH) in command should expand using resolved env vars"
    );
}
