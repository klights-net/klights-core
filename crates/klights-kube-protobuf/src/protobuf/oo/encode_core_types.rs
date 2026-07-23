/// Convert k8s-openapi Pod to k8s-pb Pod
/// Convert k8s-openapi PodSpec to k8s-pb PodSpec.
/// Shared by json_pod_to_pb and json_pod_template_spec_to_pb.
use crate::protobuf::*;
pub fn json_pod_spec_to_pb(
    spec: &k8s_openapi::api::core::v1::PodSpec,
) -> k8s_pb::api::core::v1::PodSpec {
    k8s_pb::api::core::v1::PodSpec {
        containers: spec.containers.iter().map(json_container_to_pb).collect(),
        init_containers: spec
            .init_containers
            .as_ref()
            .map(|containers| containers.iter().map(json_container_to_pb).collect())
            .unwrap_or_default(),
        ephemeral_containers: spec
            .ephemeral_containers
            .as_ref()
            .map(|containers| {
                containers
                    .iter()
                    .map(json_ephemeral_container_to_pb)
                    .collect()
            })
            .unwrap_or_default(),
        restart_policy: spec.restart_policy.clone(),
        node_name: spec.node_name.clone(),
        hostname: spec.hostname.clone(),
        subdomain: spec.subdomain.clone(),
        service_account_name: spec.service_account_name.clone(),
        service_account: spec.service_account_name.clone(),
        automount_service_account_token: spec.automount_service_account_token,
        termination_grace_period_seconds: spec.termination_grace_period_seconds,
        active_deadline_seconds: spec.active_deadline_seconds,
        tolerations: spec
            .tolerations
            .as_ref()
            .map(|tolerations| tolerations.iter().map(json_toleration_to_pb).collect())
            .unwrap_or_default(),
        dns_policy: spec.dns_policy.clone(),
        node_selector: spec
            .node_selector
            .clone()
            .map(|selector| selector.into_iter().collect())
            .unwrap_or_default(),
        affinity: spec.affinity.as_ref().map(json_affinity_to_pb),
        host_network: spec.host_network,
        priority_class_name: spec.priority_class_name.clone(),
        priority: spec.priority,
        preemption_policy: spec.preemption_policy.clone(),
        dns_config: spec
            .dns_config
            .as_ref()
            .map(|dns| k8s_pb::api::core::v1::PodDnsConfig {
                nameservers: dns.nameservers.clone().unwrap_or_default(),
                searches: dns.searches.clone().unwrap_or_default(),
                options: dns
                    .options
                    .as_ref()
                    .map(|options| {
                        options
                            .iter()
                            .map(|opt| k8s_pb::api::core::v1::PodDnsConfigOption {
                                name: opt.name.clone(),
                                value: opt.value.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
        scheduler_name: spec.scheduler_name.clone(),
        runtime_class_name: spec.runtime_class_name.clone(),
        overhead: spec
            .overhead
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
        volumes: spec
            .volumes
            .as_ref()
            .map(|vols| vols.iter().map(json_volume_to_pb).collect())
            .unwrap_or_default(),
        security_context: spec.security_context.as_ref().map(|sc| {
            k8s_pb::api::core::v1::PodSecurityContext {
                run_as_user: sc.run_as_user,
                run_as_group: sc.run_as_group,
                run_as_non_root: sc.run_as_non_root,
                fs_group: sc.fs_group,
                supplemental_groups: sc.supplemental_groups.clone().unwrap_or_default(),
                sysctls: sc
                    .sysctls
                    .as_ref()
                    .map(|sysctls| {
                        sysctls
                            .iter()
                            .map(|s| k8s_pb::api::core::v1::Sysctl {
                                name: Some(s.name.clone()),
                                value: Some(s.value.clone()),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                seccomp_profile: sc.seccomp_profile.as_ref().map(json_seccomp_profile_to_pb),
                ..Default::default()
            }
        }),
        ..Default::default()
    }
}

fn json_affinity_to_pb(
    affinity: &k8s_openapi::api::core::v1::Affinity,
) -> k8s_pb::api::core::v1::Affinity {
    k8s_pb::api::core::v1::Affinity {
        node_affinity: affinity
            .node_affinity
            .as_ref()
            .map(json_node_affinity_to_pb),
        pod_affinity: None,
        pod_anti_affinity: None,
    }
}

fn json_node_affinity_to_pb(
    node_affinity: &k8s_openapi::api::core::v1::NodeAffinity,
) -> k8s_pb::api::core::v1::NodeAffinity {
    k8s_pb::api::core::v1::NodeAffinity {
        required_during_scheduling_ignored_during_execution: node_affinity
            .required_during_scheduling_ignored_during_execution
            .as_ref()
            .map(json_node_selector_to_pb),
        preferred_during_scheduling_ignored_during_execution: node_affinity
            .preferred_during_scheduling_ignored_during_execution
            .as_ref()
            .map(|terms| {
                terms
                    .iter()
                    .map(json_preferred_scheduling_term_to_pb)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn json_node_selector_to_pb(
    selector: &k8s_openapi::api::core::v1::NodeSelector,
) -> k8s_pb::api::core::v1::NodeSelector {
    k8s_pb::api::core::v1::NodeSelector {
        node_selector_terms: selector
            .node_selector_terms
            .iter()
            .map(json_node_selector_term_to_pb)
            .collect(),
    }
}

fn json_preferred_scheduling_term_to_pb(
    term: &k8s_openapi::api::core::v1::PreferredSchedulingTerm,
) -> k8s_pb::api::core::v1::PreferredSchedulingTerm {
    k8s_pb::api::core::v1::PreferredSchedulingTerm {
        weight: Some(term.weight),
        preference: Some(json_node_selector_term_to_pb(&term.preference)),
    }
}

fn json_node_selector_term_to_pb(
    term: &k8s_openapi::api::core::v1::NodeSelectorTerm,
) -> k8s_pb::api::core::v1::NodeSelectorTerm {
    k8s_pb::api::core::v1::NodeSelectorTerm {
        match_expressions: term
            .match_expressions
            .as_ref()
            .map(|requirements| {
                requirements
                    .iter()
                    .map(json_node_selector_requirement_to_pb)
                    .collect()
            })
            .unwrap_or_default(),
        match_fields: term
            .match_fields
            .as_ref()
            .map(|requirements| {
                requirements
                    .iter()
                    .map(json_node_selector_requirement_to_pb)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn json_node_selector_requirement_to_pb(
    req: &k8s_openapi::api::core::v1::NodeSelectorRequirement,
) -> k8s_pb::api::core::v1::NodeSelectorRequirement {
    k8s_pb::api::core::v1::NodeSelectorRequirement {
        key: Some(req.key.clone()),
        operator: Some(req.operator.clone()),
        values: req.values.clone().unwrap_or_default(),
    }
}

fn json_toleration_to_pb(
    toleration: &k8s_openapi::api::core::v1::Toleration,
) -> k8s_pb::api::core::v1::Toleration {
    k8s_pb::api::core::v1::Toleration {
        key: toleration.key.clone(),
        operator: toleration.operator.clone(),
        value: toleration.value.clone(),
        effect: toleration.effect.clone(),
        toleration_seconds: toleration.toleration_seconds,
    }
}

pub fn json_ephemeral_container_to_pb(
    c: &k8s_openapi::api::core::v1::EphemeralContainer,
) -> k8s_pb::api::core::v1::EphemeralContainer {
    let common_container = k8s_openapi::api::core::v1::Container {
        args: c.args.clone(),
        command: c.command.clone(),
        env: c.env.clone(),
        env_from: c.env_from.clone(),
        image: c.image.clone(),
        image_pull_policy: c.image_pull_policy.clone(),
        lifecycle: c.lifecycle.clone(),
        liveness_probe: c.liveness_probe.clone(),
        name: c.name.clone(),
        ports: c.ports.clone(),
        readiness_probe: c.readiness_probe.clone(),
        resize_policy: c.resize_policy.clone(),
        resources: c.resources.clone(),
        restart_policy: c.restart_policy.clone(),
        security_context: c.security_context.clone(),
        startup_probe: c.startup_probe.clone(),
        stdin: c.stdin,
        stdin_once: c.stdin_once,
        termination_message_path: c.termination_message_path.clone(),
        termination_message_policy: c.termination_message_policy.clone(),
        tty: c.tty,
        volume_devices: c.volume_devices.clone(),
        volume_mounts: c.volume_mounts.clone(),
        working_dir: c.working_dir.clone(),
    };
    let pb = json_container_to_pb(&common_container);

    k8s_pb::api::core::v1::EphemeralContainer {
        ephemeral_container_common: Some(k8s_pb::api::core::v1::EphemeralContainerCommon {
            name: pb.name,
            image: pb.image,
            command: pb.command,
            args: pb.args,
            working_dir: pb.working_dir,
            ports: pb.ports,
            env_from: pb.env_from,
            env: pb.env,
            resources: pb.resources,
            resize_policy: pb.resize_policy,
            restart_policy: pb.restart_policy,
            volume_mounts: pb.volume_mounts,
            volume_devices: pb.volume_devices,
            liveness_probe: pb.liveness_probe,
            readiness_probe: pb.readiness_probe,
            startup_probe: pb.startup_probe,
            lifecycle: pb.lifecycle,
            termination_message_path: pb.termination_message_path,
            termination_message_policy: pb.termination_message_policy,
            image_pull_policy: pb.image_pull_policy,
            security_context: pb.security_context,
            stdin: pb.stdin,
            stdin_once: pb.stdin_once,
            tty: pb.tty,
            restart_policy_rules: vec![],
        }),
        target_container_name: c.target_container_name.clone(),
    }
}

/// Convert k8s-openapi PodTemplateSpec to k8s-pb PodTemplateSpec.
/// Used by Deployment, ReplicaSet, StatefulSet, DaemonSet, and Job encoders.
pub fn json_pod_template_spec_to_pb_encode(
    template: &k8s_openapi::api::core::v1::PodTemplateSpec,
) -> k8s_pb::api::core::v1::PodTemplateSpec {
    k8s_pb::api::core::v1::PodTemplateSpec {
        metadata: template.metadata.as_ref().map(json_meta_to_pb),
        spec: template.spec.as_ref().map(json_pod_spec_to_pb),
    }
}

pub fn json_pod_to_pb(
    pod: &k8s_openapi::api::core::v1::Pod,
    raw_json: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::Pod> {
    Ok(k8s_pb::api::core::v1::Pod {
        metadata: Some(json_meta_to_pb(&pod.metadata)),
        spec: pod.spec.as_ref().map(json_pod_spec_to_pb),
        status: pod.status.as_ref().map(|status| {
            // Read containerStatuses and initContainerStatuses directly from raw JSON
            // to bypass k8s_openapi deserialization which can silently lose state fields.
            let raw_container_statuses = raw_json
                .pointer("/status/containerStatuses")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().map(raw_container_status_to_pb).collect())
                .unwrap_or_default();

            let raw_init_container_statuses = raw_json
                .pointer("/status/initContainerStatuses")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().map(raw_container_status_to_pb).collect())
                .unwrap_or_default();

            let raw_ephemeral_container_statuses = raw_json
                .pointer("/status/ephemeralContainerStatuses")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().map(raw_container_status_to_pb).collect())
                .unwrap_or_default();
            let raw_pod_ips = raw_json
                .pointer("/status/podIPs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|entry| {
                            entry.get("ip").and_then(|v| v.as_str()).map(|ip| {
                                k8s_pb::api::core::v1::PodIp {
                                    ip: Some(ip.to_string()),
                                }
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let raw_host_ips = raw_json
                .pointer("/status/hostIPs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|entry| {
                            entry.get("ip").and_then(|v| v.as_str()).map(|ip| {
                                k8s_pb::api::core::v1::HostIp {
                                    ip: Some(ip.to_string()),
                                }
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            k8s_pb::api::core::v1::PodStatus {
                phase: status.phase.clone(),
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| conds.iter().map(json_pod_condition_to_pb).collect())
                    .unwrap_or_default(),
                message: status.message.clone(),
                reason: status.reason.clone(),
                pod_ip: status.pod_ip.clone(),
                pod_ips: raw_pod_ips,
                host_ip: status.host_ip.clone(),
                host_ips: raw_host_ips,
                start_time: status.start_time.as_ref().map(json_time_to_pb),
                init_container_statuses: raw_init_container_statuses,
                container_statuses: raw_container_statuses,
                ephemeral_container_statuses: raw_ephemeral_container_statuses,
                qos_class: status.qos_class.clone(),
                ..Default::default()
            }
        }),
    })
}

/// Convert k8s-openapi Container to k8s-pb Container
/// Convert k8s-openapi IntOrString to k8s-pb IntOrString
pub fn json_intorstring_to_pb(
    ios: &k8s_openapi::apimachinery::pkg::util::intstr::IntOrString,
) -> k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
    match ios {
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(i) => {
            k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(0),
                int_val: Some(*i),
                str_val: None,
            }
        }
        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => {
            k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(1),
                int_val: None,
                str_val: Some(s.clone()),
            }
        }
    }
}

/// Convert k8s-openapi Probe to k8s-pb Probe
pub fn json_probe_to_pb(probe: &k8s_openapi::api::core::v1::Probe) -> k8s_pb::api::core::v1::Probe {
    let handler = {
        let exec = probe
            .exec
            .as_ref()
            .map(|e| k8s_pb::api::core::v1::ExecAction {
                command: e.command.clone().unwrap_or_default(),
            });
        let http_get = probe
            .http_get
            .as_ref()
            .map(|h| k8s_pb::api::core::v1::HttpGetAction {
                path: h.path.clone(),
                port: Some(json_intorstring_to_pb(&h.port)),
                host: h.host.clone(),
                scheme: h.scheme.clone(),
                http_headers: h
                    .http_headers
                    .as_ref()
                    .map(|hdrs| {
                        hdrs.iter()
                            .map(|hdr| k8s_pb::api::core::v1::HttpHeader {
                                name: Some(hdr.name.clone()),
                                value: Some(hdr.value.clone()),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            });
        let tcp_socket =
            probe
                .tcp_socket
                .as_ref()
                .map(|t| k8s_pb::api::core::v1::TcpSocketAction {
                    port: Some(json_intorstring_to_pb(&t.port)),
                    host: t.host.clone(),
                });
        let grpc = probe
            .grpc
            .as_ref()
            .map(|g| k8s_pb::api::core::v1::GrpcAction {
                port: Some(g.port),
                service: g.service.clone(),
            });
        if exec.is_some() || http_get.is_some() || tcp_socket.is_some() || grpc.is_some() {
            Some(k8s_pb::api::core::v1::ProbeHandler {
                exec,
                http_get,
                tcp_socket,
                grpc,
            })
        } else {
            None
        }
    };
    k8s_pb::api::core::v1::Probe {
        handler,
        initial_delay_seconds: probe.initial_delay_seconds,
        timeout_seconds: probe.timeout_seconds,
        period_seconds: probe.period_seconds,
        success_threshold: probe.success_threshold,
        failure_threshold: probe.failure_threshold,
        ..Default::default()
    }
}

pub fn json_container_to_pb(
    c: &k8s_openapi::api::core::v1::Container,
) -> k8s_pb::api::core::v1::Container {
    use k8s_pb::api::core::v1 as pbv1;

    k8s_pb::api::core::v1::Container {
        name: Some(c.name.clone()),
        image: c.image.clone(),
        command: c.command.clone().unwrap_or_default(),
        args: c.args.clone().unwrap_or_default(),
        working_dir: c.working_dir.clone(),
        image_pull_policy: c.image_pull_policy.clone(),
        termination_message_path: c.termination_message_path.clone(),
        termination_message_policy: c.termination_message_policy.clone(),
        stdin: c.stdin,
        tty: c.tty,
        ports: c
            .ports
            .as_ref()
            .map(|ports| {
                ports
                    .iter()
                    .map(|p| k8s_pb::api::core::v1::ContainerPort {
                        name: p.name.clone(),
                        container_port: Some(p.container_port),
                        protocol: p.protocol.clone(),
                        host_port: p.host_port,
                        host_ip: p.host_ip.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        env: c
            .env
            .as_ref()
            .map(|env_vars| {
                env_vars
                    .iter()
                    .map(|e| pbv1::EnvVar {
                        name: Some(e.name.clone()),
                        value: e.value.clone(),
                        value_from: e.value_from.as_ref().map(json_env_var_source_to_pb),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        volume_mounts: c
            .volume_mounts
            .as_ref()
            .map(|mounts| {
                mounts
                    .iter()
                    .map(|m| k8s_pb::api::core::v1::VolumeMount {
                        name: Some(m.name.clone()),
                        mount_path: Some(m.mount_path.clone()),
                        read_only: m.read_only,
                        sub_path: m.sub_path.clone(),
                        mount_propagation: m.mount_propagation.clone(),
                        sub_path_expr: m.sub_path_expr.clone(),
                        recursive_read_only: None,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        liveness_probe: c.liveness_probe.as_ref().map(json_probe_to_pb),
        readiness_probe: c.readiness_probe.as_ref().map(json_probe_to_pb),
        startup_probe: c.startup_probe.as_ref().map(json_probe_to_pb),
        resources: c
            .resources
            .as_ref()
            .map(|r| k8s_pb::api::core::v1::ResourceRequirements {
                requests: r
                    .requests
                    .as_ref()
                    .map(|reqs| {
                        reqs.iter()
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
                limits: r
                    .limits
                    .as_ref()
                    .map(|lims| {
                        lims.iter()
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
                ..Default::default()
            }),
        security_context: c.security_context.as_ref().map(|sc| {
            k8s_pb::api::core::v1::SecurityContext {
                run_as_user: sc.run_as_user,
                run_as_group: sc.run_as_group,
                run_as_non_root: sc.run_as_non_root,
                privileged: sc.privileged,
                read_only_root_filesystem: sc.read_only_root_filesystem,
                allow_privilege_escalation: sc.allow_privilege_escalation,
                capabilities: sc.capabilities.as_ref().map(|caps| {
                    k8s_pb::api::core::v1::Capabilities {
                        add: caps.add.clone().unwrap_or_default(),
                        drop: caps.drop.clone().unwrap_or_default(),
                    }
                }),
                proc_mount: sc.proc_mount.clone(),
                seccomp_profile: sc.seccomp_profile.as_ref().map(json_seccomp_profile_to_pb),
                ..Default::default()
            }
        }),
        lifecycle: c
            .lifecycle
            .as_ref()
            .map(|lc| k8s_pb::api::core::v1::Lifecycle {
                post_start: lc.post_start.as_ref().map(json_lifecycle_handler_to_pb),
                pre_stop: lc.pre_stop.as_ref().map(json_lifecycle_handler_to_pb),
                stop_signal: None,
            }),
        ..Default::default()
    }
}

fn json_seccomp_profile_to_pb(
    profile: &k8s_openapi::api::core::v1::SeccompProfile,
) -> k8s_pb::api::core::v1::SeccompProfile {
    k8s_pb::api::core::v1::SeccompProfile {
        r#type: Some(profile.type_.clone()),
        localhost_profile: profile.localhost_profile.clone(),
    }
}

/// Convert k8s-openapi LifecycleHandler to k8s-pb LifecycleHandler
pub fn json_lifecycle_handler_to_pb(
    h: &k8s_openapi::api::core::v1::LifecycleHandler,
) -> k8s_pb::api::core::v1::LifecycleHandler {
    k8s_pb::api::core::v1::LifecycleHandler {
        exec: h.exec.as_ref().map(|e| k8s_pb::api::core::v1::ExecAction {
            command: e.command.clone().unwrap_or_default(),
        }),
        http_get: h
            .http_get
            .as_ref()
            .map(|hg| k8s_pb::api::core::v1::HttpGetAction {
                path: hg.path.clone(),
                port: Some(json_intorstring_to_pb(&hg.port)),
                host: hg.host.clone(),
                scheme: hg.scheme.clone(),
                http_headers: vec![],
            }),
        tcp_socket: h
            .tcp_socket
            .as_ref()
            .map(|t| k8s_pb::api::core::v1::TcpSocketAction {
                port: Some(json_intorstring_to_pb(&t.port)),
                host: t.host.clone(),
            }),
        sleep: None,
    }
}

fn json_env_var_source_to_pb(
    source: &k8s_openapi::api::core::v1::EnvVarSource,
) -> k8s_pb::api::core::v1::EnvVarSource {
    use k8s_pb::api::core::v1 as pbv1;

    pbv1::EnvVarSource {
        field_ref: source
            .field_ref
            .as_ref()
            .map(|field_ref| pbv1::ObjectFieldSelector {
                api_version: field_ref.api_version.clone(),
                field_path: Some(field_ref.field_path.clone()),
            }),
        resource_field_ref: source
            .resource_field_ref
            .as_ref()
            .map(|resource_field_ref| pbv1::ResourceFieldSelector {
                container_name: resource_field_ref.container_name.clone(),
                resource: Some(resource_field_ref.resource.clone()),
                divisor: resource_field_ref.divisor.as_ref().map(|divisor| {
                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                        string: Some(divisor.0.clone()),
                    }
                }),
            }),
        config_map_key_ref: source
            .config_map_key_ref
            .as_ref()
            .map(|config_map_key_ref| pbv1::ConfigMapKeySelector {
                local_object_reference: Some(pbv1::LocalObjectReference {
                    name: Some(config_map_key_ref.name.clone()),
                }),
                key: Some(config_map_key_ref.key.clone()),
                optional: config_map_key_ref.optional,
            }),
        secret_key_ref: source.secret_key_ref.as_ref().map(|secret_key_ref| {
            pbv1::SecretKeySelector {
                local_object_reference: Some(pbv1::LocalObjectReference {
                    name: Some(secret_key_ref.name.clone()),
                }),
                key: Some(secret_key_ref.key.clone()),
                optional: secret_key_ref.optional,
            }
        }),
        file_key_ref: None,
    }
}

/// Convert k8s-openapi Volume to k8s-pb Volume
pub fn json_volume_to_pb(v: &k8s_openapi::api::core::v1::Volume) -> k8s_pb::api::core::v1::Volume {
    use k8s_pb::api::core::v1 as pbv1;

    let volume_source = if let Some(ref ed) = v.empty_dir {
        Some(pbv1::VolumeSource {
            empty_dir: Some(pbv1::EmptyDirVolumeSource {
                medium: ed.medium.clone(),
                size_limit: ed.size_limit.as_ref().map(|q| {
                    k8s_pb::apimachinery::pkg::api::resource::Quantity {
                        string: Some(q.0.clone()),
                    }
                }),
            }),
            ..Default::default()
        })
    } else if let Some(ref hp) = v.host_path {
        Some(pbv1::VolumeSource {
            host_path: Some(pbv1::HostPathVolumeSource {
                path: Some(hp.path.clone()),
                r#type: hp.type_.clone(),
            }),
            ..Default::default()
        })
    } else if let Some(ref cm) = v.config_map {
        Some(pbv1::VolumeSource {
            config_map: Some(pbv1::ConfigMapVolumeSource {
                local_object_reference: Some(pbv1::LocalObjectReference {
                    name: Some(cm.name.clone()),
                }),
                items: cm
                    .items
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|kp| pbv1::KeyToPath {
                                key: Some(kp.key.clone()),
                                path: Some(kp.path.clone()),
                                mode: kp.mode,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                default_mode: cm.default_mode,
                optional: cm.optional,
            }),
            ..Default::default()
        })
    } else if let Some(ref s) = v.secret {
        Some(pbv1::VolumeSource {
            secret: Some(pbv1::SecretVolumeSource {
                secret_name: s.secret_name.clone(),
                items: s
                    .items
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|kp| pbv1::KeyToPath {
                                key: Some(kp.key.clone()),
                                path: Some(kp.path.clone()),
                                mode: kp.mode,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                default_mode: s.default_mode,
                optional: s.optional,
            }),
            ..Default::default()
        })
    } else if let Some(ref da) = v.downward_api {
        Some(pbv1::VolumeSource {
            downward_api: Some(pbv1::DownwardAPIVolumeSource {
                items: da
                    .items
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|item| pbv1::DownwardAPIVolumeFile {
                                path: Some(item.path.clone()),
                                field_ref: item.field_ref.as_ref().map(|fr| {
                                    pbv1::ObjectFieldSelector {
                                        api_version: fr.api_version.clone(),
                                        field_path: Some(fr.field_path.clone()),
                                    }
                                }),
                                resource_field_ref: item.resource_field_ref.as_ref().map(|rfr| {
                                    pbv1::ResourceFieldSelector {
                                        container_name: rfr.container_name.clone(),
                                        resource: Some(rfr.resource.clone()),
                                        divisor: rfr.divisor.as_ref().map(|d| {
                                            k8s_pb::apimachinery::pkg::api::resource::Quantity {
                                                string: Some(d.0.clone()),
                                            }
                                        }),
                                    }
                                }),
                                mode: item.mode,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                default_mode: da.default_mode,
            }),
            ..Default::default()
        })
    } else if let Some(ref proj) = v.projected {
        Some(pbv1::VolumeSource {
            projected: Some(pbv1::ProjectedVolumeSource {
                sources: proj.sources.as_ref().map(|sources| {
                    sources.iter().map(|vp| pbv1::VolumeProjection {
                        pod_certificate: None,
                        secret: vp.secret.as_ref().map(|s| pbv1::SecretProjection {
                            local_object_reference: Some(pbv1::LocalObjectReference {
                                name: Some(s.name.clone()),
                            }),
                            items: s.items.as_ref().map(|items| {
                                items.iter().map(|kp| pbv1::KeyToPath {
                                    key: Some(kp.key.clone()),
                                    path: Some(kp.path.clone()),
                                    mode: kp.mode,
                                }).collect()
                            }).unwrap_or_default(),
                            optional: s.optional,
                        }),
                        config_map: vp.config_map.as_ref().map(|cm| pbv1::ConfigMapProjection {
                            local_object_reference: Some(pbv1::LocalObjectReference {
                                name: Some(cm.name.clone()),
                            }),
                            items: cm.items.as_ref().map(|items| {
                                items.iter().map(|kp| pbv1::KeyToPath {
                                    key: Some(kp.key.clone()),
                                    path: Some(kp.path.clone()),
                                    mode: kp.mode,
                                }).collect()
                            }).unwrap_or_default(),
                            optional: cm.optional,
                        }),
                        downward_api: vp.downward_api.as_ref().map(|da| pbv1::DownwardAPIProjection {
                            items: da.items.as_ref().map(|items| {
                                items.iter().map(|item| pbv1::DownwardAPIVolumeFile {
                                    path: Some(item.path.clone()),
                                    field_ref: item.field_ref.as_ref().map(|fr| pbv1::ObjectFieldSelector {
                                        api_version: fr.api_version.clone(),
                                        field_path: Some(fr.field_path.clone()),
                                    }),
                                    resource_field_ref: item.resource_field_ref.as_ref().map(|rfr| pbv1::ResourceFieldSelector {
                                        container_name: rfr.container_name.clone(),
                                        resource: Some(rfr.resource.clone()),
                                        divisor: rfr.divisor.as_ref().map(|d| {
                                            k8s_pb::apimachinery::pkg::api::resource::Quantity { string: Some(d.0.clone()) }
                                        }),
                                    }),
                                    mode: item.mode,
                                }).collect()
                            }).unwrap_or_default(),
                        }),
                        service_account_token: vp.service_account_token.as_ref().map(|sat| {
                            pbv1::ServiceAccountTokenProjection {
                                audience: sat.audience.clone(),
                                expiration_seconds: sat.expiration_seconds,
                                path: Some(sat.path.clone()),
                            }
                        }),
                        cluster_trust_bundle: None,
                    }).collect()
                }).unwrap_or_default(),
                default_mode: proj.default_mode,
            }),
            ..Default::default()
        })
    } else if let Some(ref csi) = v.csi {
        Some(pbv1::VolumeSource {
            csi: Some(pbv1::CSIVolumeSource {
                driver: Some(csi.driver.clone()),
                read_only: csi.read_only,
                fs_type: csi.fs_type.clone(),
                volume_attributes: csi.volume_attributes.clone().unwrap_or_default(),
                node_publish_secret_ref: csi.node_publish_secret_ref.as_ref().map(|r| {
                    pbv1::LocalObjectReference {
                        name: Some(r.name.clone()),
                    }
                }),
            }),
            ..Default::default()
        })
    } else {
        v.persistent_volume_claim
            .as_ref()
            .map(|pvc| pbv1::VolumeSource {
                persistent_volume_claim: Some(pbv1::PersistentVolumeClaimVolumeSource {
                    claim_name: Some(pvc.claim_name.clone()),
                    read_only: pvc.read_only,
                }),
                ..Default::default()
            })
    };

    k8s_pb::api::core::v1::Volume {
        name: Some(v.name.clone()),
        volume_source,
    }
}

/// Convert k8s-openapi PodCondition to k8s-pb PodCondition
pub fn json_pod_condition_to_pb(
    cond: &k8s_openapi::api::core::v1::PodCondition,
) -> k8s_pb::api::core::v1::PodCondition {
    k8s_pb::api::core::v1::PodCondition {
        r#type: Some(cond.type_.clone()),
        status: Some(cond.status.clone()),
        last_probe_time: cond.last_probe_time.as_ref().map(json_time_to_pb),
        last_transition_time: cond.last_transition_time.as_ref().map(json_time_to_pb),
        reason: cond.reason.clone(),
        message: cond.message.clone(),
        observed_generation: None,
    }
}

/// Parse a K8s timestamp string (RFC 3339) to k8s-pb Time.
/// Returns None if the string is empty or unparseable.
pub fn raw_time_str_to_pb(s: &str) -> Option<k8s_pb::apimachinery::pkg::apis::meta::v1::Time> {
    if s.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s).ok().map(|dt| {
        k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
            seconds: Some(dt.timestamp()),
            nanos: Some(dt.timestamp_subsec_nanos() as i32),
        }
    })
}

/// Convert raw JSON ContainerState to k8s-pb ContainerState.
/// Reads directly from serde_json::Value, bypassing k8s_openapi deserialization.
pub fn raw_container_state_to_pb(state: &Value) -> k8s_pb::api::core::v1::ContainerState {
    k8s_pb::api::core::v1::ContainerState {
        running: state.get("running").and_then(|r| r.as_object()).map(|r| {
            k8s_pb::api::core::v1::ContainerStateRunning {
                started_at: r
                    .get("startedAt")
                    .and_then(|v| v.as_str())
                    .and_then(raw_time_str_to_pb),
            }
        }),
        terminated: state
            .get("terminated")
            .and_then(|t| t.as_object())
            .map(|t| k8s_pb::api::core::v1::ContainerStateTerminated {
                exit_code: t.get("exitCode").and_then(|v| v.as_i64()).map(|v| v as i32),
                signal: t.get("signal").and_then(|v| v.as_i64()).map(|v| v as i32),
                reason: t.get("reason").and_then(|v| v.as_str()).map(String::from),
                message: t.get("message").and_then(|v| v.as_str()).map(String::from),
                started_at: t
                    .get("startedAt")
                    .and_then(|v| v.as_str())
                    .and_then(raw_time_str_to_pb),
                finished_at: t
                    .get("finishedAt")
                    .and_then(|v| v.as_str())
                    .and_then(raw_time_str_to_pb),
                container_id: t
                    .get("containerID")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            }),
        waiting: state.get("waiting").and_then(|w| w.as_object()).map(|w| {
            k8s_pb::api::core::v1::ContainerStateWaiting {
                reason: w.get("reason").and_then(|v| v.as_str()).map(String::from),
                message: w.get("message").and_then(|v| v.as_str()).map(String::from),
            }
        }),
    }
}

/// Convert raw JSON ContainerStatus to k8s-pb ContainerStatus.
/// Reads directly from serde_json::Value, bypassing k8s_openapi deserialization.
/// This ensures container state is never silently lost during intermediate deserialization.
pub fn raw_container_status_to_pb(cs: &Value) -> k8s_pb::api::core::v1::ContainerStatus {
    k8s_pb::api::core::v1::ContainerStatus {
        name: cs.get("name").and_then(|v| v.as_str()).map(String::from),
        ready: cs.get("ready").and_then(|v| v.as_bool()),
        restart_count: cs
            .get("restartCount")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32),
        image: cs.get("image").and_then(|v| v.as_str()).map(String::from),
        image_id: cs.get("imageID").and_then(|v| v.as_str()).map(String::from),
        container_id: cs
            .get("containerID")
            .and_then(|v| v.as_str())
            .map(String::from),
        started: cs.get("started").and_then(|v| v.as_bool()),
        state: cs
            .get("state")
            .filter(|v| v.is_object())
            .map(raw_container_state_to_pb),
        last_state: cs
            .get("lastState")
            .filter(|v| v.is_object())
            .map(raw_container_state_to_pb),
        ..Default::default()
    }
}

/// Convert k8s-openapi ConfigMap to k8s-pb ConfigMap
pub fn json_configmap_to_pb(
    cm: &k8s_openapi::api::core::v1::ConfigMap,
) -> anyhow::Result<k8s_pb::api::core::v1::ConfigMap> {
    Ok(k8s_pb::api::core::v1::ConfigMap {
        metadata: Some(json_meta_to_pb(&cm.metadata)),
        data: cm
            .data
            .clone()
            .map(|btree| btree.into_iter().collect())
            .unwrap_or_default(),
        binary_data: cm
            .binary_data
            .as_ref()
            .map(|bd| {
                bd.iter()
                    .map(|(k, v)| (k.clone(), v.0.clone()))
                    .collect::<std::collections::BTreeMap<_, _>>()
            })
            .unwrap_or_default(),
        immutable: cm.immutable,
    })
}

/// Convert k8s-openapi Secret to k8s-pb Secret
pub fn json_secret_to_pb(
    s: &k8s_openapi::api::core::v1::Secret,
) -> anyhow::Result<k8s_pb::api::core::v1::Secret> {
    Ok(k8s_pb::api::core::v1::Secret {
        metadata: Some(json_meta_to_pb(&s.metadata)),
        data: s
            .data
            .as_ref()
            .map(|d| {
                d.iter()
                    .map(|(k, v)| (k.clone(), v.0.clone()))
                    .collect::<std::collections::BTreeMap<_, _>>()
            })
            .unwrap_or_default(),
        string_data: s
            .string_data
            .clone()
            .map(|btree| btree.into_iter().collect())
            .unwrap_or_default(),
        r#type: s.type_.clone(),
        immutable: s.immutable,
    })
}

/// Convert k8s-openapi Service to k8s-pb Service
pub fn json_service_to_pb(
    svc: &k8s_openapi::api::core::v1::Service,
    raw: &Value,
) -> anyhow::Result<k8s_pb::api::core::v1::Service> {
    Ok(k8s_pb::api::core::v1::Service {
        metadata: Some(json_meta_to_pb(&svc.metadata)),
        spec: svc.spec.as_ref().map(|spec| {
            let raw_session_affinity = raw
                .pointer("/spec/sessionAffinity")
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .map(|v| v.to_string());
            let session_affinity = spec
                .session_affinity
                .clone()
                .filter(|v| !v.is_empty())
                .or(raw_session_affinity)
                .or_else(|| Some("None".to_string()));

            k8s_pb::api::core::v1::ServiceSpec {
                ports: spec
                    .ports
                    .as_ref()
                    .map(|ports| {
                        ports
                            .iter()
                            .map(|p| k8s_pb::api::core::v1::ServicePort {
                                name: p.name.clone(),
                                protocol: p.protocol.clone(),
                                port: Some(p.port),
                                target_port: p.target_port.as_ref().map(json_intorstring_to_pb),
                                node_port: p.node_port,
                                app_protocol: p.app_protocol.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                selector: spec
                    .selector
                    .clone()
                    .map(|btree| btree.into_iter().collect())
                    .unwrap_or_default(),
                session_affinity,
                cluster_ip: spec.cluster_ip.clone(),
                cluster_ips: spec.cluster_ips.clone().unwrap_or_default(),
                external_name: spec.external_name.clone(),
                r#type: spec.type_.clone(),
                ..Default::default()
            }
        }),
        status: svc
            .status
            .as_ref()
            .map(|status| k8s_pb::api::core::v1::ServiceStatus {
                load_balancer: status.load_balancer.as_ref().map(|lb| {
                    k8s_pb::api::core::v1::LoadBalancerStatus {
                        ingress: lb
                            .ingress
                            .as_ref()
                            .map(|entries| {
                                entries
                                    .iter()
                                    .map(|entry| k8s_pb::api::core::v1::LoadBalancerIngress {
                                        ip: entry.ip.clone(),
                                        hostname: entry.hostname.clone(),
                                        ip_mode: entry.ip_mode.clone(),
                                        ports: entry
                                            .ports
                                            .as_ref()
                                            .map(|ports| {
                                                ports
                                                    .iter()
                                                    .map(|p| k8s_pb::api::core::v1::PortStatus {
                                                        port: Some(p.port),
                                                        protocol: Some(p.protocol.clone()),
                                                        error: p.error.clone(),
                                                    })
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }
                }),
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::apimachinery::pkg::apis::meta::v1::Condition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                observed_generation: c.observed_generation,
                                last_transition_time: Some(json_time_to_pb(
                                    &c.last_transition_time,
                                )),
                                reason: Some(c.reason.clone()),
                                message: Some(c.message.clone()),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    })
}
