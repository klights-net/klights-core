/// Convert protobuf PodSpec to JSON
use crate::protobuf::*;
pub fn pb_pod_spec_to_json(spec: &k8s_pb::api::core::v1::PodSpec) -> Value {
    use serde_json::json;
    let mut obj = json!({});

    if !spec.containers.is_empty() {
        obj["containers"] = json!(
            spec.containers
                .iter()
                .map(pb_container_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !spec.init_containers.is_empty() {
        obj["initContainers"] = json!(
            spec.init_containers
                .iter()
                .map(pb_container_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !spec.ephemeral_containers.is_empty() {
        obj["ephemeralContainers"] = json!(
            spec.ephemeral_containers
                .iter()
                .map(pb_ephemeral_container_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(restart_policy) = &spec.restart_policy {
        obj["restartPolicy"] = json!(restart_policy);
    }
    if let Some(node_name) = &spec.node_name {
        obj["nodeName"] = json!(node_name);
    }
    if let Some(service_account_name) = &spec.service_account_name {
        obj["serviceAccountName"] = json!(service_account_name);
    }
    if let Some(automount) = spec.automount_service_account_token {
        obj["automountServiceAccountToken"] = json!(automount);
    }
    if let Some(termination_grace_period) = spec.termination_grace_period_seconds {
        obj["terminationGracePeriodSeconds"] = json!(termination_grace_period);
    }
    if let Some(active_deadline_seconds) = spec.active_deadline_seconds {
        obj["activeDeadlineSeconds"] = json!(active_deadline_seconds);
    }
    if !spec.tolerations.is_empty() {
        obj["tolerations"] = json!(
            spec.tolerations
                .iter()
                .map(pb_toleration_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(dns_policy) = &spec.dns_policy {
        obj["dnsPolicy"] = json!(dns_policy);
    }
    if !spec.node_selector.is_empty() {
        obj["nodeSelector"] = json!(spec.node_selector);
    }
    if let Some(host_network) = spec.host_network {
        obj["hostNetwork"] = json!(host_network);
    }
    if let Some(priority_class_name) = &spec.priority_class_name
        && !priority_class_name.is_empty()
    {
        obj["priorityClassName"] = json!(priority_class_name);
    }
    if let Some(priority) = spec.priority {
        obj["priority"] = json!(priority);
    }
    if let Some(preemption_policy) = &spec.preemption_policy
        && !preemption_policy.is_empty()
    {
        obj["preemptionPolicy"] = json!(preemption_policy);
    }
    if let Some(affinity) = &spec.affinity {
        let affinity_obj = pb_affinity_to_json(affinity);
        if affinity_obj.as_object().is_some_and(|obj| !obj.is_empty()) {
            obj["affinity"] = affinity_obj;
        }
    }
    if let Some(dns_config) = &spec.dns_config {
        let mut dns_obj = json!({});
        if !dns_config.nameservers.is_empty() {
            dns_obj["nameservers"] = json!(dns_config.nameservers);
        }
        if !dns_config.searches.is_empty() {
            dns_obj["searches"] = json!(dns_config.searches);
        }
        if !dns_config.options.is_empty() {
            dns_obj["options"] = json!(
                dns_config
                    .options
                    .iter()
                    .map(|opt| {
                        let mut item = json!({});
                        if let Some(name) = &opt.name {
                            item["name"] = json!(name);
                        }
                        if let Some(value) = &opt.value {
                            item["value"] = json!(value);
                        }
                        item
                    })
                    .collect::<Vec<_>>()
            );
        }
        obj["dnsConfig"] = dns_obj;
    }
    if let Some(hostname) = &spec.hostname
        && !hostname.is_empty()
    {
        obj["hostname"] = json!(hostname);
    }
    if let Some(subdomain) = &spec.subdomain
        && !subdomain.is_empty()
    {
        obj["subdomain"] = json!(subdomain);
    }
    if let Some(scheduler_name) = &spec.scheduler_name {
        obj["schedulerName"] = json!(scheduler_name);
    }
    if let Some(runtime_class_name) = &spec.runtime_class_name {
        obj["runtimeClassName"] = json!(runtime_class_name);
    }
    if !spec.overhead.is_empty() {
        let mut overhead_obj = serde_json::Map::new();
        for (k, v) in &spec.overhead {
            if let Some(quantity) = &v.string {
                overhead_obj.insert(k.clone(), json!(quantity));
            }
        }
        if !overhead_obj.is_empty() {
            obj["overhead"] = Value::Object(overhead_obj);
        }
    }
    if !spec.volumes.is_empty() {
        obj["volumes"] = json!(
            spec.volumes
                .iter()
                .map(pb_volume_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(sc) = &spec.security_context {
        let mut sc_obj = json!({});
        if let Some(run_as_user) = sc.run_as_user {
            sc_obj["runAsUser"] = json!(run_as_user);
        }
        if let Some(run_as_group) = sc.run_as_group {
            sc_obj["runAsGroup"] = json!(run_as_group);
        }
        if let Some(run_as_non_root) = sc.run_as_non_root {
            sc_obj["runAsNonRoot"] = json!(run_as_non_root);
        }
        if let Some(fs_group) = sc.fs_group {
            sc_obj["fsGroup"] = json!(fs_group);
        }
        if !sc.supplemental_groups.is_empty() {
            sc_obj["supplementalGroups"] = json!(sc.supplemental_groups);
        }
        if !sc.sysctls.is_empty() {
            sc_obj["sysctls"] = json!(
                sc.sysctls
                    .iter()
                    .map(|sysctl| {
                        let mut item = json!({});
                        if let Some(name) = &sysctl.name {
                            item["name"] = json!(name);
                        }
                        if let Some(value) = &sysctl.value {
                            item["value"] = json!(value);
                        }
                        item
                    })
                    .collect::<Vec<_>>()
            );
        }
        if let Some(seccomp_profile) = &sc.seccomp_profile {
            sc_obj["seccompProfile"] = pb_seccomp_profile_to_json(seccomp_profile);
        }
        obj["securityContext"] = sc_obj;
    }
    if !spec.host_aliases.is_empty() {
        obj["hostAliases"] = json!(
            spec.host_aliases
                .iter()
                .map(|ha| {
                    let mut alias = json!({});
                    if let Some(ip) = &ha.ip {
                        alias["ip"] = json!(ip);
                    }
                    if !ha.hostnames.is_empty() {
                        alias["hostnames"] = json!(ha.hostnames);
                    }
                    alias
                })
                .collect::<Vec<_>>()
        );
    }

    obj
}

fn pb_affinity_to_json(affinity: &k8s_pb::api::core::v1::Affinity) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(node_affinity) = &affinity.node_affinity {
        let node_obj = pb_node_affinity_to_json(node_affinity);
        if node_obj.as_object().is_some_and(|obj| !obj.is_empty()) {
            obj["nodeAffinity"] = node_obj;
        }
    }
    obj
}

fn pb_node_affinity_to_json(node_affinity: &k8s_pb::api::core::v1::NodeAffinity) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(required) = &node_affinity.required_during_scheduling_ignored_during_execution {
        obj["requiredDuringSchedulingIgnoredDuringExecution"] = pb_node_selector_to_json(required);
    }
    if !node_affinity
        .preferred_during_scheduling_ignored_during_execution
        .is_empty()
    {
        obj["preferredDuringSchedulingIgnoredDuringExecution"] = json!(
            node_affinity
                .preferred_during_scheduling_ignored_during_execution
                .iter()
                .map(pb_preferred_scheduling_term_to_json)
                .collect::<Vec<_>>()
        );
    }
    obj
}

fn pb_node_selector_to_json(selector: &k8s_pb::api::core::v1::NodeSelector) -> Value {
    use serde_json::json;

    json!({
        "nodeSelectorTerms": selector
            .node_selector_terms
            .iter()
            .map(pb_node_selector_term_to_json)
            .collect::<Vec<_>>()
    })
}

fn pb_preferred_scheduling_term_to_json(
    term: &k8s_pb::api::core::v1::PreferredSchedulingTerm,
) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(weight) = term.weight {
        obj["weight"] = json!(weight);
    }
    if let Some(preference) = &term.preference {
        obj["preference"] = pb_node_selector_term_to_json(preference);
    }
    obj
}

fn pb_node_selector_term_to_json(term: &k8s_pb::api::core::v1::NodeSelectorTerm) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if !term.match_expressions.is_empty() {
        obj["matchExpressions"] = json!(
            term.match_expressions
                .iter()
                .map(pb_node_selector_requirement_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !term.match_fields.is_empty() {
        obj["matchFields"] = json!(
            term.match_fields
                .iter()
                .map(pb_node_selector_requirement_to_json)
                .collect::<Vec<_>>()
        );
    }
    obj
}

fn pb_node_selector_requirement_to_json(
    req: &k8s_pb::api::core::v1::NodeSelectorRequirement,
) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(key) = &req.key {
        obj["key"] = json!(key);
    }
    if let Some(operator) = &req.operator {
        obj["operator"] = json!(operator);
    }
    if !req.values.is_empty() {
        obj["values"] = json!(req.values);
    }
    obj
}

fn pb_toleration_to_json(toleration: &k8s_pb::api::core::v1::Toleration) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(key) = &toleration.key {
        obj["key"] = json!(key);
    }
    if let Some(operator) = &toleration.operator {
        obj["operator"] = json!(operator);
    }
    if let Some(value) = &toleration.value {
        obj["value"] = json!(value);
    }
    if let Some(effect) = &toleration.effect {
        obj["effect"] = json!(effect);
    }
    if let Some(seconds) = toleration.toleration_seconds {
        obj["tolerationSeconds"] = json!(seconds);
    }
    obj
}

fn pb_ephemeral_container_to_json(c: &k8s_pb::api::core::v1::EphemeralContainer) -> Value {
    use serde_json::json;

    let mut obj = c
        .ephemeral_container_common
        .as_ref()
        .map(|common| {
            let container = k8s_pb::api::core::v1::Container {
                name: common.name.clone(),
                image: common.image.clone(),
                command: common.command.clone(),
                args: common.args.clone(),
                working_dir: common.working_dir.clone(),
                ports: common.ports.clone(),
                env_from: common.env_from.clone(),
                env: common.env.clone(),
                resources: common.resources.clone(),
                resize_policy: common.resize_policy.clone(),
                restart_policy: common.restart_policy.clone(),
                volume_mounts: common.volume_mounts.clone(),
                volume_devices: common.volume_devices.clone(),
                liveness_probe: common.liveness_probe.clone(),
                readiness_probe: common.readiness_probe.clone(),
                startup_probe: common.startup_probe.clone(),
                lifecycle: common.lifecycle.clone(),
                termination_message_path: common.termination_message_path.clone(),
                termination_message_policy: common.termination_message_policy.clone(),
                image_pull_policy: common.image_pull_policy.clone(),
                security_context: common.security_context.clone(),
                stdin: common.stdin,
                stdin_once: common.stdin_once,
                tty: common.tty,
                restart_policy_rules: vec![],
            };
            pb_container_to_json(&container)
        })
        .unwrap_or_else(|| json!({}));

    if let Some(target_container_name) = &c.target_container_name {
        obj["targetContainerName"] = json!(target_container_name);
    }
    obj
}

/// Convert protobuf KeyToPath to JSON
pub fn pb_key_to_path_to_json(kp: &k8s_pb::api::core::v1::KeyToPath) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(key) = &kp.key {
        obj["key"] = json!(key);
    }
    if let Some(path) = &kp.path {
        obj["path"] = json!(path);
    }
    if let Some(mode) = kp.mode {
        obj["mode"] = json!(mode);
    }
    obj
}

/// Convert protobuf Volume to JSON with all volume source types
pub fn pb_volume_to_json(v: &k8s_pb::api::core::v1::Volume) -> Value {
    use serde_json::json;
    let mut vol = json!({});
    if let Some(name) = &v.name {
        vol["name"] = json!(name);
    }
    if let Some(vs) = &v.volume_source {
        if let Some(ed) = &vs.empty_dir {
            let mut ed_obj = json!({});
            if let Some(medium) = &ed.medium
                && !medium.is_empty()
            {
                ed_obj["medium"] = json!(medium);
            }
            vol["emptyDir"] = ed_obj;
        } else if let Some(hp) = &vs.host_path {
            let mut hp_obj = json!({});
            if let Some(path) = &hp.path {
                hp_obj["path"] = json!(path);
            }
            if let Some(t) = &hp.r#type {
                hp_obj["type"] = json!(t);
            }
            vol["hostPath"] = hp_obj;
        } else if let Some(cm) = &vs.config_map {
            let mut cm_obj = json!({});
            if let Some(lor) = &cm.local_object_reference
                && let Some(name) = &lor.name
            {
                cm_obj["name"] = json!(name);
            }
            if !cm.items.is_empty() {
                cm_obj["items"] = json!(
                    cm.items
                        .iter()
                        .map(pb_key_to_path_to_json)
                        .collect::<Vec<_>>()
                );
            }
            if let Some(mode) = cm.default_mode {
                cm_obj["defaultMode"] = json!(mode);
            }
            if let Some(opt) = cm.optional {
                cm_obj["optional"] = json!(opt);
            }
            vol["configMap"] = cm_obj;
        } else if let Some(s) = &vs.secret {
            let mut s_obj = json!({});
            if let Some(name) = &s.secret_name {
                s_obj["secretName"] = json!(name);
            }
            if !s.items.is_empty() {
                s_obj["items"] = json!(
                    s.items
                        .iter()
                        .map(pb_key_to_path_to_json)
                        .collect::<Vec<_>>()
                );
            }
            if let Some(mode) = s.default_mode {
                s_obj["defaultMode"] = json!(mode);
            }
            if let Some(opt) = s.optional {
                s_obj["optional"] = json!(opt);
            }
            vol["secret"] = s_obj;
        } else if let Some(da) = &vs.downward_api {
            let mut da_obj = json!({});
            if !da.items.is_empty() {
                da_obj["items"] = json!(
                    da.items
                        .iter()
                        .map(|item| {
                            let mut i = json!({});
                            if let Some(path) = &item.path {
                                i["path"] = json!(path);
                            }
                            if let Some(fr) = &item.field_ref {
                                let mut fr_obj = json!({});
                                if let Some(fp) = &fr.field_path {
                                    fr_obj["fieldPath"] = json!(fp);
                                }
                                if let Some(av) = &fr.api_version {
                                    fr_obj["apiVersion"] = json!(av);
                                }
                                i["fieldRef"] = fr_obj;
                            }
                            if let Some(rfr) = &item.resource_field_ref {
                                let mut rfr_obj = json!({});
                                if let Some(r) = &rfr.resource {
                                    rfr_obj["resource"] = json!(r);
                                }
                                if let Some(cn) = &rfr.container_name {
                                    rfr_obj["containerName"] = json!(cn);
                                }
                                i["resourceFieldRef"] = rfr_obj;
                            }
                            if let Some(mode) = item.mode {
                                i["mode"] = json!(mode);
                            }
                            i
                        })
                        .collect::<Vec<_>>()
                );
            }
            if let Some(mode) = da.default_mode {
                da_obj["defaultMode"] = json!(mode);
            }
            vol["downwardAPI"] = da_obj;
        } else if let Some(proj) = &vs.projected {
            let mut proj_obj = json!({});
            if let Some(mode) = proj.default_mode {
                proj_obj["defaultMode"] = json!(mode);
            }
            if !proj.sources.is_empty() {
                proj_obj["sources"] = json!(
                    proj.sources
                        .iter()
                        .map(|vp| {
                            let mut src = json!({});
                            if let Some(sat) = &vp.service_account_token {
                                let mut sat_obj = json!({});
                                if let Some(path) = &sat.path {
                                    sat_obj["path"] = json!(path);
                                }
                                if let Some(exp) = sat.expiration_seconds {
                                    sat_obj["expirationSeconds"] = json!(exp);
                                }
                                if let Some(aud) = &sat.audience {
                                    sat_obj["audience"] = json!(aud);
                                }
                                src["serviceAccountToken"] = sat_obj;
                            }
                            if let Some(cm) = &vp.config_map {
                                let mut cm_obj = json!({});
                                if let Some(lor) = &cm.local_object_reference
                                    && let Some(name) = &lor.name
                                {
                                    cm_obj["name"] = json!(name);
                                }
                                if !cm.items.is_empty() {
                                    cm_obj["items"] = json!(
                                        cm.items
                                            .iter()
                                            .map(pb_key_to_path_to_json)
                                            .collect::<Vec<_>>()
                                    );
                                }
                                if let Some(opt) = cm.optional {
                                    cm_obj["optional"] = json!(opt);
                                }
                                src["configMap"] = cm_obj;
                            }
                            if let Some(s) = &vp.secret {
                                let mut s_obj = json!({});
                                if let Some(lor) = &s.local_object_reference
                                    && let Some(name) = &lor.name
                                {
                                    s_obj["name"] = json!(name);
                                }
                                if !s.items.is_empty() {
                                    s_obj["items"] = json!(
                                        s.items
                                            .iter()
                                            .map(pb_key_to_path_to_json)
                                            .collect::<Vec<_>>()
                                    );
                                }
                                if let Some(opt) = s.optional {
                                    s_obj["optional"] = json!(opt);
                                }
                                src["secret"] = s_obj;
                            }
                            if let Some(da) = &vp.downward_api {
                                let mut da_obj = json!({});
                                if !da.items.is_empty() {
                                    da_obj["items"] = json!(
                                        da.items
                                            .iter()
                                            .map(|item| {
                                                let mut i = json!({});
                                                if let Some(path) = &item.path {
                                                    i["path"] = json!(path);
                                                }
                                                if let Some(fr) = &item.field_ref {
                                                    let mut fr_obj = json!({});
                                                    if let Some(fp) = &fr.field_path {
                                                        fr_obj["fieldPath"] = json!(fp);
                                                    }
                                                    if let Some(av) = &fr.api_version {
                                                        fr_obj["apiVersion"] = json!(av);
                                                    }
                                                    i["fieldRef"] = fr_obj;
                                                }
                                                if let Some(rfr) = &item.resource_field_ref {
                                                    let mut rfr_obj = json!({});
                                                    if let Some(r) = &rfr.resource {
                                                        rfr_obj["resource"] = json!(r);
                                                    }
                                                    if let Some(cn) = &rfr.container_name {
                                                        rfr_obj["containerName"] = json!(cn);
                                                    }
                                                    i["resourceFieldRef"] = rfr_obj;
                                                }
                                                if let Some(mode) = item.mode {
                                                    i["mode"] = json!(mode);
                                                }
                                                i
                                            })
                                            .collect::<Vec<_>>()
                                    );
                                }
                                src["downwardAPI"] = da_obj;
                            }
                            src
                        })
                        .collect::<Vec<_>>()
                );
            }
            vol["projected"] = proj_obj;
        } else if let Some(csi) = &vs.csi {
            let mut csi_obj = json!({});
            if let Some(driver) = &csi.driver {
                csi_obj["driver"] = json!(driver);
            }
            if let Some(read_only) = csi.read_only {
                csi_obj["readOnly"] = json!(read_only);
            }
            if let Some(fs_type) = &csi.fs_type {
                csi_obj["fsType"] = json!(fs_type);
            }
            if !csi.volume_attributes.is_empty() {
                csi_obj["volumeAttributes"] = json!(csi.volume_attributes);
            }
            if let Some(secret_ref) = &csi.node_publish_secret_ref {
                let mut secret_obj = json!({});
                if let Some(name) = &secret_ref.name {
                    secret_obj["name"] = json!(name);
                }
                csi_obj["nodePublishSecretRef"] = secret_obj;
            }
            vol["csi"] = csi_obj;
        } else if let Some(pvc) = &vs.persistent_volume_claim {
            let mut pvc_obj = json!({});
            if let Some(name) = &pvc.claim_name {
                pvc_obj["claimName"] = json!(name);
            }
            if let Some(ro) = pvc.read_only {
                pvc_obj["readOnly"] = json!(ro);
            }
            vol["persistentVolumeClaim"] = pvc_obj;
        }
    }
    vol
}

/// Convert protobuf PodTemplateSpec to JSON
pub fn pb_pod_template_spec_to_json(template: &k8s_pb::api::core::v1::PodTemplateSpec) -> Value {
    use serde_json::json;
    let mut obj = json!({});

    if let Some(metadata) = &template.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }
    if let Some(spec) = &template.spec {
        obj["spec"] = pb_pod_spec_to_json(spec);
    }

    obj
}

// Convert protobuf Deployment to JSON
pb_decode!(
    pb_deployment_to_json,
    k8s_pb::api::apps::v1::Deployment,
    d,
    "apps/v1",
    "Deployment",
    obj,
    {
        if let Some(spec) = &d.spec {
            let mut spec_obj = json!({});
            if let Some(replicas) = spec.replicas {
                spec_obj["replicas"] = json!(replicas);
            }
            if let Some(selector) = &spec.selector {
                let mut sel = json!({});
                if !selector.match_labels.is_empty() {
                    sel["matchLabels"] = json!(selector.match_labels);
                }
                spec_obj["selector"] = sel;
            }
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            if let Some(strategy) = &spec.strategy {
                let mut strat = json!({});
                if let Some(t) = &strategy.r#type {
                    strat["type"] = json!(t);
                }
                if let Some(ru) = &strategy.rolling_update {
                    let mut ru_obj = json!({});
                    if let Some(mu) = &ru.max_unavailable {
                        ru_obj["maxUnavailable"] = intorstring_to_json(mu);
                    }
                    if let Some(ms) = &ru.max_surge {
                        ru_obj["maxSurge"] = intorstring_to_json(ms);
                    }
                    strat["rollingUpdate"] = ru_obj;
                }
                spec_obj["strategy"] = strat;
            }
            if let Some(revision_history_limit) = spec.revision_history_limit {
                spec_obj["revisionHistoryLimit"] = json!(revision_history_limit);
            }
            if let Some(progress_deadline_seconds) = spec.progress_deadline_seconds {
                spec_obj["progressDeadlineSeconds"] = json!(progress_deadline_seconds);
            }
            if let Some(min_ready_seconds) = spec.min_ready_seconds {
                spec_obj["minReadySeconds"] = json!(min_ready_seconds);
            }
            if let Some(paused) = spec.paused {
                spec_obj["paused"] = json!(paused);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &d.status {
            let mut status_obj = json!({});
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if let Some(v) = status.replicas {
                status_obj["replicas"] = json!(v);
            }
            if let Some(v) = status.updated_replicas {
                status_obj["updatedReplicas"] = json!(v);
            }
            if let Some(v) = status.ready_replicas {
                status_obj["readyReplicas"] = json!(v);
            }
            if let Some(v) = status.available_replicas {
                status_obj["availableReplicas"] = json!(v);
            }
            if let Some(v) = status.unavailable_replicas {
                status_obj["unavailableReplicas"] = json!(v);
            }
            if let Some(v) = status.collision_count {
                status_obj["collisionCount"] = json!(v);
            }
            if !status.conditions.is_empty() {
                status_obj["conditions"] = json!(
                    status
                        .conditions
                        .iter()
                        .map(|c| {
                            let mut cond = json!({
                                "type": c.r#type,
                                "status": c.status
                            });
                            if let Some(t) = &c.last_update_time {
                                cond["lastUpdateTime"] = pb_time_to_json(t);
                            }
                            if let Some(t) = &c.last_transition_time {
                                cond["lastTransitionTime"] = pb_time_to_json(t);
                            }
                            if let Some(reason) = &c.reason {
                                cond["reason"] = json!(reason);
                            }
                            if let Some(message) = &c.message {
                                cond["message"] = json!(message);
                            }
                            cond
                        })
                        .collect::<Vec<_>>()
                );
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

pb_decode!(
    pb_replicaset_to_json,
    k8s_pb::api::apps::v1::ReplicaSet,
    rs,
    "apps/v1",
    "ReplicaSet",
    obj,
    {
        if let Some(spec) = &rs.spec {
            let mut spec_obj = json!({});
            if let Some(replicas) = spec.replicas {
                spec_obj["replicas"] = json!(replicas);
            }
            if let Some(selector) = &spec.selector {
                let mut sel = json!({});
                if !selector.match_labels.is_empty() {
                    sel["matchLabels"] = json!(selector.match_labels);
                }
                spec_obj["selector"] = sel;
            }
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &rs.status {
            let mut status_obj = json!({});
            if let Some(replicas) = status.replicas {
                status_obj["replicas"] = json!(replicas);
            }
            if let Some(v) = status.fully_labeled_replicas {
                status_obj["fullyLabeledReplicas"] = json!(v);
            }
            if let Some(v) = status.ready_replicas {
                status_obj["readyReplicas"] = json!(v);
            }
            if let Some(v) = status.available_replicas {
                status_obj["availableReplicas"] = json!(v);
            }
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if !status.conditions.is_empty() {
                status_obj["conditions"] = json!(
                    status
                        .conditions
                        .iter()
                        .map(|c| {
                            let mut cond = json!({
                                "type": c.r#type,
                                "status": c.status
                            });
                            if let Some(t) = &c.last_transition_time {
                                cond["lastTransitionTime"] = pb_time_to_json(t);
                            }
                            if let Some(reason) = &c.reason {
                                cond["reason"] = json!(reason);
                            }
                            if let Some(message) = &c.message {
                                cond["message"] = json!(message);
                            }
                            cond
                        })
                        .collect::<Vec<_>>()
                );
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

// Convert protobuf Pod to JSON
pb_decode!(
    pb_pod_to_json,
    k8s_pb::api::core::v1::Pod,
    pod,
    "v1",
    "Pod",
    obj,
    {
        if let Some(spec) = &pod.spec {
            obj["spec"] = pb_pod_spec_to_json(spec);
        }
        if let Some(status) = &pod.status {
            obj["status"] = pb_pod_status_to_json(status);
        }
    }
);

/// Convert protobuf PodStatus to JSON
pub fn pb_pod_status_to_json(status: &k8s_pb::api::core::v1::PodStatus) -> Value {
    use serde_json::json;

    let mut obj = json!({});

    if let Some(phase) = &status.phase {
        obj["phase"] = json!(phase);
    }
    if let Some(message) = &status.message {
        obj["message"] = json!(message);
    }
    if let Some(reason) = &status.reason {
        obj["reason"] = json!(reason);
    }
    if let Some(pod_ip) = &status.pod_ip {
        obj["podIP"] = json!(pod_ip);
    }
    if !status.pod_ips.is_empty() {
        obj["podIPs"] = json!(
            status
                .pod_ips
                .iter()
                .map(|entry| json!({"ip": entry.ip.clone().unwrap_or_default()}))
                .collect::<Vec<_>>()
        );
    }
    if let Some(host_ip) = &status.host_ip {
        obj["hostIP"] = json!(host_ip);
    }
    if !status.host_ips.is_empty() {
        obj["hostIPs"] = json!(
            status
                .host_ips
                .iter()
                .map(|entry| json!({"ip": entry.ip.clone().unwrap_or_default()}))
                .collect::<Vec<_>>()
        );
    }
    if let Some(qos_class) = &status.qos_class {
        obj["qosClass"] = json!(qos_class);
    }
    if let Some(start_time) = &status.start_time {
        obj["startTime"] = pb_time_to_json(start_time);
    }

    if !status.conditions.is_empty() {
        obj["conditions"] = json!(
            status
                .conditions
                .iter()
                .map(pb_pod_condition_to_json)
                .collect::<Vec<_>>()
        );
    }

    if !status.container_statuses.is_empty() {
        obj["containerStatuses"] = json!(
            status
                .container_statuses
                .iter()
                .map(pb_container_status_to_json)
                .collect::<Vec<_>>()
        );
    }

    if !status.init_container_statuses.is_empty() {
        obj["initContainerStatuses"] = json!(
            status
                .init_container_statuses
                .iter()
                .map(pb_container_status_to_json)
                .collect::<Vec<_>>()
        );
    }

    if !status.ephemeral_container_statuses.is_empty() {
        obj["ephemeralContainerStatuses"] = json!(
            status
                .ephemeral_container_statuses
                .iter()
                .map(pb_container_status_to_json)
                .collect::<Vec<_>>()
        );
    }

    obj
}

/// Convert protobuf PodCondition to JSON
pub fn pb_pod_condition_to_json(cond: &k8s_pb::api::core::v1::PodCondition) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(t) = &cond.r#type {
        obj["type"] = json!(t);
    }
    if let Some(s) = &cond.status {
        obj["status"] = json!(s);
    }
    if let Some(t) = &cond.last_probe_time {
        obj["lastProbeTime"] = pb_time_to_json(t);
    }
    if let Some(t) = &cond.last_transition_time {
        obj["lastTransitionTime"] = pb_time_to_json(t);
    }
    if let Some(r) = &cond.reason {
        obj["reason"] = json!(r);
    }
    if let Some(m) = &cond.message {
        obj["message"] = json!(m);
    }
    obj
}

/// Convert protobuf Time to JSON RFC3339 string
pub fn pb_time_to_json(time: &k8s_pb::apimachinery::pkg::apis::meta::v1::Time) -> Value {
    if let Some(seconds) = time.seconds
        && let Ok(dt) = time::OffsetDateTime::from_unix_timestamp(seconds)
        && let Ok(formatted) = dt.format(&time::format_description::well_known::Rfc3339)
    {
        return serde_json::json!(formatted);
    }
    serde_json::json!(null)
}

/// Convert protobuf ContainerStatus to JSON
pub fn pb_container_status_to_json(cs: &k8s_pb::api::core::v1::ContainerStatus) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(name) = &cs.name {
        obj["name"] = json!(name);
    }
    if let Some(ready) = cs.ready {
        obj["ready"] = json!(ready);
    }
    if let Some(restart_count) = cs.restart_count {
        obj["restartCount"] = json!(restart_count);
    }
    if let Some(image) = &cs.image {
        obj["image"] = json!(image);
    }
    if let Some(image_id) = &cs.image_id {
        obj["imageID"] = json!(image_id);
    }
    if let Some(container_id) = &cs.container_id {
        obj["containerID"] = json!(container_id);
    }
    if let Some(started) = cs.started {
        obj["started"] = json!(started);
    }
    if let Some(state) = &cs.state {
        obj["state"] = pb_container_state_to_json(state);
    }
    if let Some(last_state) = &cs.last_state {
        let last = pb_container_state_to_json(last_state);
        if !last.as_object().map(|o| o.is_empty()).unwrap_or(true) {
            obj["lastState"] = last;
        }
    }
    obj
}

/// Convert protobuf ContainerState to JSON
pub fn pb_container_state_to_json(state: &k8s_pb::api::core::v1::ContainerState) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(running) = &state.running {
        let mut r = json!({});
        if let Some(started_at) = &running.started_at {
            r["startedAt"] = pb_time_to_json(started_at);
        }
        obj["running"] = r;
    }
    if let Some(terminated) = &state.terminated {
        let mut t = json!({});
        if let Some(exit_code) = terminated.exit_code {
            t["exitCode"] = json!(exit_code);
        }
        if let Some(reason) = &terminated.reason {
            t["reason"] = json!(reason);
        }
        if let Some(message) = &terminated.message {
            t["message"] = json!(message);
        }
        if let Some(started_at) = &terminated.started_at {
            t["startedAt"] = pb_time_to_json(started_at);
        }
        if let Some(finished_at) = &terminated.finished_at {
            t["finishedAt"] = pb_time_to_json(finished_at);
        }
        if let Some(container_id) = &terminated.container_id {
            t["containerID"] = json!(container_id);
        }
        obj["terminated"] = t;
    }
    if let Some(waiting) = &state.waiting {
        let mut w = json!({});
        if let Some(reason) = &waiting.reason {
            w["reason"] = json!(reason);
        }
        if let Some(message) = &waiting.message {
            w["message"] = json!(message);
        }
        obj["waiting"] = w;
    }
    obj
}

// Convert protobuf Service to JSON
pb_decode!(
    pb_service_to_json,
    k8s_pb::api::core::v1::Service,
    svc,
    "v1",
    "Service",
    obj,
    {
        if let Some(spec) = &svc.spec {
            let mut spec_obj = json!({});
            if !spec.ports.is_empty() {
                spec_obj["ports"] = json!(
                    spec.ports
                        .iter()
                        .map(|p| {
                            let mut port = json!({});
                            if let Some(name) = &p.name {
                                port["name"] = json!(name);
                            }
                            if let Some(protocol) = &p.protocol {
                                port["protocol"] = json!(protocol);
                            }
                            if let Some(port_num) = p.port {
                                port["port"] = json!(port_num);
                            }
                            if let Some(target_port) = &p.target_port {
                                port["targetPort"] = intorstring_to_json(target_port);
                            }
                            if let Some(node_port) = p.node_port {
                                port["nodePort"] = json!(node_port);
                            }
                            port
                        })
                        .collect::<Vec<_>>()
                );
            }
            if !spec.selector.is_empty() {
                spec_obj["selector"] = json!(spec.selector);
            }
            if let Some(svc_type) = &spec.r#type {
                spec_obj["type"] = json!(svc_type);
            }
            if let Some(session_affinity) = &spec.session_affinity
                && !session_affinity.is_empty()
            {
                spec_obj["sessionAffinity"] = json!(session_affinity);
            }
            if let Some(cluster_ip) = &spec.cluster_ip {
                spec_obj["clusterIP"] = json!(cluster_ip);
            }
            if let Some(external_name) = &spec.external_name {
                spec_obj["externalName"] = json!(external_name);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &svc.status {
            let mut status_obj = json!({});
            if let Some(lb) = &status.load_balancer {
                let ingress: Vec<Value> = lb
                    .ingress
                    .iter()
                    .map(|entry| {
                        let mut ingress_obj = json!({});
                        if let Some(ip) = &entry.ip {
                            ingress_obj["ip"] = json!(ip);
                        }
                        if let Some(hostname) = &entry.hostname {
                            ingress_obj["hostname"] = json!(hostname);
                        }
                        if let Some(ip_mode) = &entry.ip_mode {
                            ingress_obj["ipMode"] = json!(ip_mode);
                        }
                        if !entry.ports.is_empty() {
                            ingress_obj["ports"] = json!(
                                entry
                                    .ports
                                    .iter()
                                    .map(|p| {
                                        let mut port = json!({});
                                        if let Some(v) = p.port {
                                            port["port"] = json!(v);
                                        }
                                        if let Some(v) = &p.protocol {
                                            port["protocol"] = json!(v);
                                        }
                                        if let Some(v) = &p.error {
                                            port["error"] = json!(v);
                                        }
                                        port
                                    })
                                    .collect::<Vec<_>>()
                            );
                        }
                        ingress_obj
                    })
                    .collect();
                status_obj["loadBalancer"] = json!({ "ingress": ingress });
            }
            if !status.conditions.is_empty() {
                let conditions: Vec<Value> = status
                    .conditions
                    .iter()
                    .map(|c| {
                        let mut cond = json!({
                            "type": c.r#type.as_deref().unwrap_or(""),
                            "status": c.status.as_deref().unwrap_or(""),
                        });
                        if let Some(reason) = &c.reason {
                            cond["reason"] = json!(reason);
                        }
                        if let Some(message) = &c.message {
                            cond["message"] = json!(message);
                        }
                        if let Some(v) = c.observed_generation {
                            cond["observedGeneration"] = json!(v);
                        }
                        if let Some(t) = &c.last_transition_time {
                            cond["lastTransitionTime"] = pb_time_to_json(t);
                        }
                        cond
                    })
                    .collect();
                status_obj["conditions"] = json!(conditions);
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

// Convert protobuf Secret to JSON
pb_decode!(
    pb_secret_to_json,
    k8s_pb::api::core::v1::Secret,
    secret,
    "v1",
    "Secret",
    obj,
    {
        if !secret.data.is_empty() {
            let data_obj: std::collections::HashMap<String, String> = secret
                .data
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v),
                    )
                })
                .collect();
            obj["data"] = json!(data_obj);
        }
        if !secret.string_data.is_empty() {
            obj["stringData"] = json!(secret.string_data);
        }
        if let Some(t) = &secret.r#type {
            obj["type"] = json!(t);
        }
        // P0-E2E-20260424b-09: immutable must survive proto decode so the
        // immutable-enforcement check in the update handler fires correctly.
        if let Some(imm) = secret.immutable {
            obj["immutable"] = json!(imm);
        }
    }
);
