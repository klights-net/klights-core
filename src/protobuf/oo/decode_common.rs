use crate::protobuf::*;

/// Decode protobuf resource by apiVersion and kind using the OO codec registry.
pub fn decode_protobuf_resource(
    api_version: &str,
    kind: &str,
    data: &[u8],
) -> anyhow::Result<Value> {
    tracing::debug!(
        "decode_protobuf_resource: apiVersion={}, kind={}, data_len={}",
        api_version,
        kind,
        data.len()
    );

    if let Ok(json_value) = serde_json::from_slice::<Value>(data) {
        return Ok(json_value);
    }

    let registry = global_oo_registry();
    if registry.handles(api_version, kind) {
        registry.decode(api_version, kind, data)
    } else {
        decode_generic_protobuf(api_version, kind, data)
    }
}

/// A generic protobuf message that only decodes ObjectMeta (field 1).
/// Used as a fallback for unknown K8s resource types.
#[derive(prost::Message)]
struct GenericK8sResource {
    #[prost(message, optional, tag = "1")]
    metadata: Option<k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta>,
}

/// Generic fallback decoder: decodes the ObjectMeta from field 1 and returns a minimal JSON.
pub fn decode_generic_protobuf(
    api_version: &str,
    kind: &str,
    data: &[u8],
) -> anyhow::Result<Value> {
    use prost::Message;
    let generic = GenericK8sResource::decode(data).map_err(|e| {
        anyhow::anyhow!(
            "Failed to decode generic resource {}/{}: {}",
            api_version,
            kind,
            e
        )
    })?;

    let mut obj = serde_json::json!({
        "apiVersion": api_version,
        "kind": kind,
    });

    if let Some(metadata) = &generic.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }

    Ok(obj)
}

/// Macro for the boilerplate shared by all top-level pb_*_to_json functions:
/// - Creates the initial `{"apiVersion": ..., "kind": ...}` object
/// - Applies metadata if present
/// - Runs the caller-supplied body to fill in spec/status/other fields
/// - Returns Ok(obj)
///
/// Usage:
/// ```ignore
/// pb_decode!(fn_name, ResourceType, resource_var, "group/v1", "Kind", obj, {
///     // body using `resource_var` and `obj`
/// });
/// ```
macro_rules! pb_decode {
    ($fn_name:ident, $proto_ty:ty, $var:ident, $api_version:expr_2021, $kind:expr_2021, $obj:ident, $body:block) => {
        pub fn $fn_name($var: &$proto_ty) -> anyhow::Result<Value> {
            use serde_json::json;
            let mut $obj = json!({"apiVersion": $api_version, "kind": $kind});
            if let Some(metadata) = &$var.metadata {
                $obj["metadata"] = meta_to_json(metadata);
            }
            $body
            Ok($obj)
        }
    };
}

// Convert protobuf Namespace to JSON
pb_decode!(
    pb_namespace_to_json,
    k8s_pb::api::core::v1::Namespace,
    ns,
    "v1",
    "Namespace",
    obj,
    {
        if let Some(spec) = &ns.spec {
            let mut spec_obj = json!({});
            if !spec.finalizers.is_empty() {
                spec_obj["finalizers"] = json!(spec.finalizers);
            }
            if !spec_obj.is_null() && spec_obj.as_object().is_some_and(|o| !o.is_empty()) {
                obj["spec"] = spec_obj;
            }
        }

        if let Some(status) = &ns.status {
            let mut status_obj = json!({});
            if let Some(phase) = &status.phase {
                status_obj["phase"] = json!(phase);
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
            if !status_obj.is_null() && status_obj.as_object().is_some_and(|o| !o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

/// Convert protobuf ObjectMeta to JSON
pub fn meta_to_json(meta: &k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> Value {
    use serde_json::json;

    let mut obj = json!({});

    if let Some(name) = &meta.name {
        obj["name"] = json!(name);
    }
    if let Some(generate_name) = &meta.generate_name {
        obj["generateName"] = json!(generate_name);
    }
    if let Some(namespace) = &meta.namespace {
        obj["namespace"] = json!(namespace);
    }
    if let Some(uid) = &meta.uid {
        obj["uid"] = json!(uid);
    }
    if let Some(rv) = &meta.resource_version {
        obj["resourceVersion"] = json!(rv);
    }
    if let Some(r#gen) = meta.generation {
        obj["generation"] = json!(r#gen);
    }
    if !meta.labels.is_empty() {
        obj["labels"] = json!(meta.labels);
    }
    if !meta.annotations.is_empty() {
        obj["annotations"] = json!(meta.annotations);
    }
    if !meta.finalizers.is_empty() {
        obj["finalizers"] = json!(meta.finalizers);
    }
    if !meta.owner_references.is_empty() {
        let owner_refs: Vec<Value> = meta
            .owner_references
            .iter()
            .map(|owner_ref| {
                let mut ref_obj = json!({});
                if let Some(api_version) = &owner_ref.api_version {
                    ref_obj["apiVersion"] = json!(api_version);
                }
                if let Some(kind) = &owner_ref.kind {
                    ref_obj["kind"] = json!(kind);
                }
                if let Some(name) = &owner_ref.name {
                    ref_obj["name"] = json!(name);
                }
                if let Some(uid) = &owner_ref.uid {
                    ref_obj["uid"] = json!(uid);
                }
                if let Some(controller) = owner_ref.controller {
                    ref_obj["controller"] = json!(controller);
                }
                if let Some(block_owner_deletion) = owner_ref.block_owner_deletion {
                    ref_obj["blockOwnerDeletion"] = json!(block_owner_deletion);
                }
                ref_obj
            })
            .collect();
        obj["ownerReferences"] = json!(owner_refs);
    }
    if let Some(creation_timestamp) = &meta.creation_timestamp
        && let Some(seconds) = creation_timestamp.seconds
    {
        // Convert to RFC3339 format
        if let Ok(dt) = time::OffsetDateTime::from_unix_timestamp(seconds)
            && let Ok(formatted) = dt.format(&time::format_description::well_known::Rfc3339)
        {
            obj["creationTimestamp"] = json!(formatted);
        }
    }
    if let Some(deletion_timestamp) = &meta.deletion_timestamp
        && let Some(seconds) = deletion_timestamp.seconds
    {
        // Convert to RFC3339 format
        if let Ok(dt) = time::OffsetDateTime::from_unix_timestamp(seconds)
            && let Ok(formatted) = dt.format(&time::format_description::well_known::Rfc3339)
        {
            obj["deletionTimestamp"] = json!(formatted);
        }
    }

    obj
}

// Convert protobuf ConfigMap to JSON
pb_decode!(
    pb_configmap_to_json,
    k8s_pb::api::core::v1::ConfigMap,
    cm,
    "v1",
    "ConfigMap",
    obj,
    {
        if !cm.data.is_empty() {
            obj["data"] = json!(cm.data);
        }
        if !cm.binary_data.is_empty() {
            obj["binaryData"] = json!(
                cm.binary_data
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v),
                        )
                    })
                    .collect::<std::collections::HashMap<_, _>>()
            );
        }
        // P0-E2E-20260424b-09: immutable field must survive proto decode so that
        // the immutable-enforcement check in the update handler fires correctly.
        if let Some(imm) = cm.immutable {
            obj["immutable"] = json!(imm);
        }
    }
);

/// Convert a protobuf `map<string, Quantity>` (resource requests / limits /
/// pod-status `allocatedResources`, etc.) into a JSON object whose values
/// are the canonical Quantity strings. Builds a serde_json::Map directly
/// (skipping the HashMap intermediate), borrows the inner Quantity string
/// via `as_deref` (one fewer Option<String> clone per entry), and pre-sizes
/// the Map so the decode hot path stays allocation-cheap.
pub fn pb_quantity_map_to_value(
    m: &std::collections::BTreeMap<String, k8s_pb::apimachinery::pkg::api::resource::Quantity>,
) -> Value {
    let mut obj = serde_json::Map::with_capacity(m.len());
    for (k, v) in m {
        let s = v.string.as_deref().unwrap_or_default();
        obj.insert(k.clone(), Value::String(s.to_string()));
    }
    Value::Object(obj)
}

/// Convert protobuf Container to JSON
pub fn pb_container_to_json(c: &k8s_pb::api::core::v1::Container) -> Value {
    use serde_json::json;
    let mut obj = json!({});

    if let Some(name) = &c.name {
        obj["name"] = json!(name);
    }
    if let Some(image) = &c.image {
        obj["image"] = json!(image);
    }
    if !c.command.is_empty() {
        obj["command"] = json!(c.command);
    }
    if !c.args.is_empty() {
        obj["args"] = json!(c.args);
    }
    if !c.ports.is_empty() {
        obj["ports"] = json!(
            c.ports
                .iter()
                .map(|p| {
                    let mut port = json!({});
                    if let Some(name) = &p.name {
                        port["name"] = json!(name);
                    }
                    if let Some(container_port) = p.container_port {
                        port["containerPort"] = json!(container_port);
                    }
                    if let Some(protocol) = &p.protocol {
                        port["protocol"] = json!(protocol);
                    }
                    if let Some(host_port) = p.host_port {
                        port["hostPort"] = json!(host_port);
                    }
                    if let Some(host_ip) = &p.host_ip
                        && !host_ip.is_empty()
                    {
                        port["hostIP"] = json!(host_ip);
                    }
                    port
                })
                .collect::<Vec<_>>()
        );
    }
    if !c.env.is_empty() {
        obj["env"] = json!(
            c.env
                .iter()
                .map(|e| {
                    let mut env = json!({});
                    if let Some(name) = &e.name {
                        env["name"] = json!(name);
                    }
                    if let Some(value) = &e.value
                        && !value.is_empty()
                    {
                        env["value"] = json!(value);
                    }
                    if let Some(value_from) = &e.value_from {
                        let mut vf = json!({});
                        if let Some(field_ref) = &value_from.field_ref {
                            let mut fr = json!({});
                            if let Some(api_version) = &field_ref.api_version {
                                fr["apiVersion"] = json!(api_version);
                            }
                            if let Some(field_path) = &field_ref.field_path {
                                fr["fieldPath"] = json!(field_path);
                            }
                            vf["fieldRef"] = fr;
                        }
                        if let Some(resource_field_ref) = &value_from.resource_field_ref {
                            let mut rfr = json!({});
                            if let Some(container_name) = &resource_field_ref.container_name {
                                rfr["containerName"] = json!(container_name);
                            }
                            if let Some(resource) = &resource_field_ref.resource {
                                rfr["resource"] = json!(resource);
                            }
                            vf["resourceFieldRef"] = rfr;
                        }
                        if let Some(config_map_key_ref) = &value_from.config_map_key_ref {
                            let mut cmkr = json!({});
                            if let Some(lor) = &config_map_key_ref.local_object_reference
                                && let Some(name) = &lor.name
                            {
                                cmkr["name"] = json!(name);
                            }
                            if let Some(key) = &config_map_key_ref.key {
                                cmkr["key"] = json!(key);
                            }
                            if let Some(optional) = config_map_key_ref.optional {
                                cmkr["optional"] = json!(optional);
                            }
                            vf["configMapKeyRef"] = cmkr;
                        }
                        if let Some(secret_key_ref) = &value_from.secret_key_ref {
                            let mut skr = json!({});
                            if let Some(lor) = &secret_key_ref.local_object_reference
                                && let Some(name) = &lor.name
                            {
                                skr["name"] = json!(name);
                            }
                            if let Some(key) = &secret_key_ref.key {
                                skr["key"] = json!(key);
                            }
                            if let Some(optional) = secret_key_ref.optional {
                                skr["optional"] = json!(optional);
                            }
                            vf["secretKeyRef"] = skr;
                        }
                        env["valueFrom"] = vf;
                    }
                    env
                })
                .collect::<Vec<_>>()
        );
    }
    if !c.volume_mounts.is_empty() {
        obj["volumeMounts"] = json!(
            c.volume_mounts
                .iter()
                .map(|vm| {
                    let mut mount = json!({});
                    if let Some(name) = &vm.name {
                        mount["name"] = json!(name);
                    }
                    if let Some(mount_path) = &vm.mount_path {
                        mount["mountPath"] = json!(mount_path);
                    }
                    if let Some(read_only) = vm.read_only {
                        mount["readOnly"] = json!(read_only);
                    }
                    if let Some(sub_path) = &vm.sub_path {
                        mount["subPath"] = json!(sub_path);
                    }
                    if let Some(sub_path_expr) = &vm.sub_path_expr
                        && !sub_path_expr.is_empty()
                    {
                        mount["subPathExpr"] = json!(sub_path_expr);
                    }
                    mount
                })
                .collect::<Vec<_>>()
        );
    }
    if let Some(resources) = &c.resources {
        let mut resources_obj = json!({});
        if !resources.requests.is_empty() {
            resources_obj["requests"] = pb_quantity_map_to_value(&resources.requests);
        }
        if !resources.limits.is_empty() {
            resources_obj["limits"] = pb_quantity_map_to_value(&resources.limits);
        }
        obj["resources"] = resources_obj;
    }
    if let Some(sc) = &c.security_context {
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
        if let Some(privileged) = sc.privileged {
            sc_obj["privileged"] = json!(privileged);
        }
        if let Some(read_only_root_filesystem) = sc.read_only_root_filesystem {
            sc_obj["readOnlyRootFilesystem"] = json!(read_only_root_filesystem);
        }
        if let Some(allow_privilege_escalation) = sc.allow_privilege_escalation {
            sc_obj["allowPrivilegeEscalation"] = json!(allow_privilege_escalation);
        }
        if let Some(caps) = &sc.capabilities {
            let mut caps_obj = json!({});
            if !caps.add.is_empty() {
                caps_obj["add"] = json!(caps.add);
            }
            if !caps.drop.is_empty() {
                caps_obj["drop"] = json!(caps.drop);
            }
            sc_obj["capabilities"] = caps_obj;
        }
        if let Some(proc_mount) = &sc.proc_mount {
            sc_obj["procMount"] = json!(proc_mount);
        }
        obj["securityContext"] = sc_obj;
    }

    // Probes
    if let Some(probe) = &c.liveness_probe {
        obj["livenessProbe"] = pb_probe_to_json(probe);
    }
    if let Some(probe) = &c.readiness_probe {
        obj["readinessProbe"] = pb_probe_to_json(probe);
    }
    if let Some(probe) = &c.startup_probe {
        obj["startupProbe"] = pb_probe_to_json(probe);
    }

    // Lifecycle hooks
    if let Some(lifecycle) = &c.lifecycle {
        let mut lc = json!({});
        if let Some(post_start) = &lifecycle.post_start {
            lc["postStart"] = pb_lifecycle_handler_to_json(post_start);
        }
        if let Some(pre_stop) = &lifecycle.pre_stop {
            lc["preStop"] = pb_lifecycle_handler_to_json(pre_stop);
        }
        obj["lifecycle"] = lc;
    }

    // Additional container fields
    if let Some(working_dir) = &c.working_dir
        && !working_dir.is_empty()
    {
        obj["workingDir"] = json!(working_dir);
    }
    if let Some(image_pull_policy) = &c.image_pull_policy {
        obj["imagePullPolicy"] = json!(image_pull_policy);
    }
    if let Some(stdin) = c.stdin
        && stdin
    {
        obj["stdin"] = json!(true);
    }
    if let Some(tty) = c.tty
        && tty
    {
        obj["tty"] = json!(true);
    }
    if let Some(termination_message_path) = &c.termination_message_path {
        obj["terminationMessagePath"] = json!(termination_message_path);
    }
    if let Some(termination_message_policy) = &c.termination_message_policy {
        obj["terminationMessagePolicy"] = json!(termination_message_policy);
    }

    // envFrom: bulk injection of all keys from a Secret or ConfigMap as env vars.
    // Missing from the original decoder — without this, envFrom SecretRef/ConfigMapRef
    // pods sent via protobuf (the Go client default) have no envFrom env vars at all.
    if !c.env_from.is_empty() {
        let env_from_arr: Vec<Value> = c
            .env_from
            .iter()
            .map(|ef| {
                let mut entry = json!({});
                if let Some(prefix) = &ef.prefix
                    && !prefix.is_empty()
                {
                    entry["prefix"] = json!(prefix);
                }
                if let Some(secret_ref) = &ef.secret_ref {
                    let mut sr = json!({});
                    if let Some(lor) = &secret_ref.local_object_reference
                        && let Some(name) = &lor.name
                    {
                        sr["name"] = json!(name);
                    }
                    if let Some(optional) = secret_ref.optional {
                        sr["optional"] = json!(optional);
                    }
                    entry["secretRef"] = sr;
                }
                if let Some(cm_ref) = &ef.config_map_ref {
                    let mut cmr = json!({});
                    if let Some(lor) = &cm_ref.local_object_reference
                        && let Some(name) = &lor.name
                    {
                        cmr["name"] = json!(name);
                    }
                    if let Some(optional) = cm_ref.optional {
                        cmr["optional"] = json!(optional);
                    }
                    entry["configMapRef"] = cmr;
                }
                entry
            })
            .collect();
        obj["envFrom"] = json!(env_from_arr);
    }

    obj
}

/// Convert protobuf Probe to JSON
pub fn pb_probe_to_json(probe: &k8s_pb::api::core::v1::Probe) -> Value {
    use serde_json::json;
    let mut obj = json!({});

    if let Some(handler) = &probe.handler {
        if let Some(exec) = &handler.exec {
            obj["exec"] = json!({ "command": exec.command });
        }
        if let Some(http_get) = &handler.http_get {
            let mut hg = json!({});
            if let Some(path) = &http_get.path {
                hg["path"] = json!(path);
            }
            if let Some(port) = &http_get.port {
                hg["port"] = intorstring_to_json(port);
            }
            if let Some(host) = &http_get.host
                && !host.is_empty()
            {
                hg["host"] = json!(host);
            }
            if let Some(scheme) = &http_get.scheme {
                hg["scheme"] = json!(scheme);
            }
            if !http_get.http_headers.is_empty() {
                hg["httpHeaders"] = json!(
                    http_get
                        .http_headers
                        .iter()
                        .map(|h| {
                            let mut hdr = json!({});
                            if let Some(name) = &h.name {
                                hdr["name"] = json!(name);
                            }
                            if let Some(value) = &h.value {
                                hdr["value"] = json!(value);
                            }
                            hdr
                        })
                        .collect::<Vec<_>>()
                );
            }
            obj["httpGet"] = hg;
        }
        if let Some(tcp_socket) = &handler.tcp_socket {
            let mut ts = json!({});
            if let Some(port) = &tcp_socket.port {
                ts["port"] = intorstring_to_json(port);
            }
            obj["tcpSocket"] = ts;
        }
        if let Some(grpc) = &handler.grpc {
            let mut g = json!({});
            if let Some(port) = grpc.port {
                g["port"] = json!(port);
            }
            if let Some(service) = &grpc.service {
                g["service"] = json!(service);
            }
            obj["grpc"] = g;
        }
    }

    if let Some(v) = probe.initial_delay_seconds {
        obj["initialDelaySeconds"] = json!(v);
    }
    if let Some(v) = probe.timeout_seconds {
        obj["timeoutSeconds"] = json!(v);
    }
    if let Some(v) = probe.period_seconds {
        obj["periodSeconds"] = json!(v);
    }
    if let Some(v) = probe.success_threshold {
        obj["successThreshold"] = json!(v);
    }
    if let Some(v) = probe.failure_threshold {
        obj["failureThreshold"] = json!(v);
    }

    obj
}

/// Convert protobuf LifecycleHandler to JSON
pub fn pb_lifecycle_handler_to_json(handler: &k8s_pb::api::core::v1::LifecycleHandler) -> Value {
    use serde_json::json;
    let mut obj = json!({});

    if let Some(exec) = &handler.exec {
        obj["exec"] = json!({ "command": exec.command });
    }
    if let Some(http_get) = &handler.http_get {
        let mut hg = json!({});
        if let Some(host) = &http_get.host
            && !host.is_empty()
        {
            hg["host"] = json!(host);
        }
        if let Some(path) = &http_get.path {
            hg["path"] = json!(path);
        }
        if let Some(port) = &http_get.port {
            hg["port"] = intorstring_to_json(port);
        }
        if let Some(scheme) = &http_get.scheme
            && !scheme.is_empty()
        {
            hg["scheme"] = json!(scheme);
        }
        obj["httpGet"] = hg;
    }
    if let Some(tcp_socket) = &handler.tcp_socket {
        let mut ts = json!({});
        if let Some(port) = &tcp_socket.port {
            ts["port"] = intorstring_to_json(port);
        }
        obj["tcpSocket"] = ts;
    }

    obj
}
