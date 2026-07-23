use crate::protobuf::*;
pb_decode!(
    pb_persistentvolume_to_json,
    k8s_pb::api::core::v1::PersistentVolume,
    pv,
    "v1",
    "PersistentVolume",
    obj,
    {
        if let Some(spec) = &pv.spec {
            let mut spec_obj = json!({});
            if !spec.capacity.is_empty() {
                spec_obj["capacity"] = pb_quantity_map_to_value(&spec.capacity);
            }
            if !spec.access_modes.is_empty() {
                spec_obj["accessModes"] = json!(spec.access_modes);
            }
            if let Some(policy) = &spec.persistent_volume_reclaim_policy {
                spec_obj["persistentVolumeReclaimPolicy"] = json!(policy);
            }
            if let Some(storage_class) = &spec.storage_class_name {
                spec_obj["storageClassName"] = json!(storage_class);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &pv.status {
            let mut status_obj = json!({});
            if let Some(phase) = &status.phase {
                status_obj["phase"] = json!(phase);
            }
            if let Some(message) = &status.message {
                status_obj["message"] = json!(message);
            }
            if let Some(reason) = &status.reason {
                status_obj["reason"] = json!(reason);
            }
            if let Some(last_transition) = &status.last_phase_transition_time
                && let Some(seconds) = last_transition.seconds
                && let Some(dt) = chrono::DateTime::from_timestamp(
                    seconds,
                    last_transition.nanos.unwrap_or(0) as u32,
                )
            {
                status_obj["lastPhaseTransitionTime"] = json!(dt.to_rfc3339());
            }
            obj["status"] = status_obj;
        }
    }
);

pb_decode!(
    pb_persistentvolumeclaim_to_json,
    k8s_pb::api::core::v1::PersistentVolumeClaim,
    pvc,
    "v1",
    "PersistentVolumeClaim",
    obj,
    {
        if let Some(spec) = &pvc.spec {
            let mut spec_obj = json!({});
            if !spec.access_modes.is_empty() {
                spec_obj["accessModes"] = json!(spec.access_modes);
            }
            if let Some(resources) = &spec.resources {
                let mut resources_obj = json!({});
                if !resources.requests.is_empty() {
                    resources_obj["requests"] = pb_quantity_map_to_value(&resources.requests);
                }
                if !resources.limits.is_empty() {
                    resources_obj["limits"] = pb_quantity_map_to_value(&resources.limits);
                }
                spec_obj["resources"] = resources_obj;
            }
            if let Some(storage_class) = &spec.storage_class_name {
                spec_obj["storageClassName"] = json!(storage_class);
            }
            if let Some(volume_name) = &spec.volume_name {
                spec_obj["volumeName"] = json!(volume_name);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &pvc.status {
            let mut status_obj = json!({});
            if let Some(phase) = &status.phase {
                status_obj["phase"] = json!(phase);
            }
            if !status.access_modes.is_empty() {
                status_obj["accessModes"] = json!(status.access_modes);
            }
            if !status.capacity.is_empty() {
                status_obj["capacity"] = pb_quantity_map_to_value(&status.capacity);
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
                        if let Some(t) = &c.last_probe_time
                            && let Some(s) = t.seconds
                            && let Some(dt) =
                                chrono::DateTime::from_timestamp(s, t.nanos.unwrap_or(0) as u32)
                        {
                            cond["lastProbeTime"] = json!(dt.to_rfc3339());
                        }
                        if let Some(t) = &c.last_transition_time
                            && let Some(s) = t.seconds
                            && let Some(dt) =
                                chrono::DateTime::from_timestamp(s, t.nanos.unwrap_or(0) as u32)
                        {
                            cond["lastTransitionTime"] = json!(dt.to_rfc3339());
                        }
                        cond
                    })
                    .collect();
                status_obj["conditions"] = json!(conditions);
            }
            if !status.allocated_resources.is_empty() {
                status_obj["allocatedResources"] =
                    pb_quantity_map_to_value(&status.allocated_resources);
            }
            if !status.allocated_resource_statuses.is_empty() {
                status_obj["allocatedResourceStatuses"] = json!(status.allocated_resource_statuses);
            }
            if let Some(current) = &status.current_volume_attributes_class_name {
                status_obj["currentVolumeAttributesClassName"] = json!(current);
            }
            if let Some(mv) = &status.modify_volume_status {
                let mut mv_obj = json!({});
                if let Some(status) = &mv.status {
                    mv_obj["status"] = json!(status);
                }
                if let Some(target) = &mv.target_volume_attributes_class_name {
                    mv_obj["targetVolumeAttributesClassName"] = json!(target);
                }
                status_obj["modifyVolumeStatus"] = mv_obj;
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

pb_decode!(
    pb_event_to_json,
    k8s_pb::api::core::v1::Event,
    event,
    "v1",
    "Event",
    obj,
    {
        if let Some(involved_obj) = &event.involved_object {
            let mut inv_obj = json!({});
            if let Some(kind) = &involved_obj.kind {
                inv_obj["kind"] = json!(kind);
            }
            if let Some(name) = &involved_obj.name {
                inv_obj["name"] = json!(name);
            }
            if let Some(namespace) = &involved_obj.namespace {
                inv_obj["namespace"] = json!(namespace);
            }
            if let Some(uid) = &involved_obj.uid {
                inv_obj["uid"] = json!(uid);
            }
            if let Some(api_version) = &involved_obj.api_version {
                inv_obj["apiVersion"] = json!(api_version);
            }
            obj["involvedObject"] = inv_obj;
        }
        if let Some(reason) = &event.reason {
            obj["reason"] = json!(reason);
        }
        if let Some(message) = &event.message {
            obj["message"] = json!(message);
        }
        if let Some(source) = &event.source {
            let mut source_obj = json!({});
            if let Some(component) = &source.component {
                source_obj["component"] = json!(component);
            }
            if let Some(host) = &source.host {
                source_obj["host"] = json!(host);
            }
            obj["source"] = source_obj;
        }
        if let Some(typ) = &event.r#type {
            obj["type"] = json!(typ);
        }
        if let Some(count) = event.count {
            obj["count"] = json!(count);
        }
        if let Some(event_time) = &event.event_time
            && let Some(seconds) = event_time.seconds
        {
            let ts =
                chrono::DateTime::from_timestamp(seconds, event_time.nanos.unwrap_or(0) as u32)
                    .map(|dt| crate::utils::k8s_microtime_format(dt.with_timezone(&chrono::Utc)))
                    .unwrap_or_default();
            obj["eventTime"] = json!(ts);
        }
        if let Some(series) = &event.series {
            let mut series_obj = json!({});
            if let Some(count) = series.count {
                series_obj["count"] = json!(count);
            }
            if let Some(last_observed_time) = &series.last_observed_time
                && let Some(seconds) = last_observed_time.seconds
            {
                let ts = chrono::DateTime::from_timestamp(
                    seconds,
                    last_observed_time.nanos.unwrap_or(0) as u32,
                )
                .map(|dt| crate::utils::k8s_microtime_format(dt.with_timezone(&chrono::Utc)))
                .unwrap_or_default();
                series_obj["lastObservedTime"] = json!(ts);
            }
            obj["series"] = series_obj;
        }
        if let Some(action) = &event.action {
            obj["action"] = json!(action);
        }
        if let Some(related) = &event.related {
            let mut rel_obj = json!({});
            if let Some(kind) = &related.kind {
                rel_obj["kind"] = json!(kind);
            }
            if let Some(name) = &related.name {
                rel_obj["name"] = json!(name);
            }
            if let Some(namespace) = &related.namespace {
                rel_obj["namespace"] = json!(namespace);
            }
            if let Some(uid) = &related.uid {
                rel_obj["uid"] = json!(uid);
            }
            if let Some(api_version) = &related.api_version {
                rel_obj["apiVersion"] = json!(api_version);
            }
            obj["related"] = rel_obj;
        }
        if let Some(component) = &event.reporting_component {
            obj["reportingComponent"] = json!(component);
        }
        if let Some(instance) = &event.reporting_instance {
            obj["reportingInstance"] = json!(instance);
        }
        if let Some(first_timestamp) = &event.first_timestamp
            && let Some(seconds) = first_timestamp.seconds
        {
            obj["firstTimestamp"] = json!(seconds);
        }
        if let Some(last_timestamp) = &event.last_timestamp
            && let Some(seconds) = last_timestamp.seconds
        {
            obj["lastTimestamp"] = json!(seconds);
        }
    }
);

pb_decode!(
    pb_events_v1_event_to_json,
    k8s_pb::api::events::v1::Event,
    event,
    "events.k8s.io/v1",
    "Event",
    obj,
    {
        if let Some(event_time) = &event.event_time
            && let Some(seconds) = event_time.seconds
        {
            let ts =
                chrono::DateTime::from_timestamp(seconds, event_time.nanos.unwrap_or(0) as u32)
                    .map(|dt| crate::utils::k8s_microtime_format(dt.with_timezone(&chrono::Utc)))
                    .unwrap_or_default();
            obj["eventTime"] = json!(ts);
        }
        if let Some(series) = &event.series {
            let mut series_obj = json!({});
            if let Some(count) = series.count {
                series_obj["count"] = json!(count);
            }
            if let Some(last_observed_time) = &series.last_observed_time
                && let Some(seconds) = last_observed_time.seconds
            {
                let ts = chrono::DateTime::from_timestamp(
                    seconds,
                    last_observed_time.nanos.unwrap_or(0) as u32,
                )
                .map(|dt| crate::utils::k8s_microtime_format(dt.with_timezone(&chrono::Utc)))
                .unwrap_or_default();
                series_obj["lastObservedTime"] = json!(ts);
            }
            obj["series"] = series_obj;
        }
        if let Some(v) = &event.reporting_controller {
            obj["reportingController"] = json!(v);
        }
        if let Some(v) = &event.reporting_instance {
            obj["reportingInstance"] = json!(v);
        }
        if let Some(action) = &event.action {
            obj["action"] = json!(action);
        }
        if let Some(reason) = &event.reason {
            obj["reason"] = json!(reason);
        }
        if let Some(regarding) = &event.regarding {
            let mut reg_obj = json!({});
            if let Some(kind) = &regarding.kind {
                reg_obj["kind"] = json!(kind);
            }
            if let Some(name) = &regarding.name {
                reg_obj["name"] = json!(name);
            }
            if let Some(namespace) = &regarding.namespace {
                reg_obj["namespace"] = json!(namespace);
            }
            if let Some(uid) = &regarding.uid {
                reg_obj["uid"] = json!(uid);
            }
            if let Some(api_version) = &regarding.api_version {
                reg_obj["apiVersion"] = json!(api_version);
            }
            obj["regarding"] = reg_obj;
        }
        if let Some(related) = &event.related {
            let mut rel_obj = json!({});
            if let Some(kind) = &related.kind {
                rel_obj["kind"] = json!(kind);
            }
            if let Some(name) = &related.name {
                rel_obj["name"] = json!(name);
            }
            if let Some(namespace) = &related.namespace {
                rel_obj["namespace"] = json!(namespace);
            }
            if let Some(uid) = &related.uid {
                rel_obj["uid"] = json!(uid);
            }
            if let Some(api_version) = &related.api_version {
                rel_obj["apiVersion"] = json!(api_version);
            }
            obj["related"] = rel_obj;
        }
        if let Some(note) = &event.note {
            obj["note"] = json!(note);
        }
        if let Some(typ) = &event.r#type {
            obj["type"] = json!(typ);
        }
        if let Some(source) = &event.deprecated_source {
            let mut source_obj = json!({});
            if let Some(component) = &source.component {
                source_obj["component"] = json!(component);
            }
            if let Some(host) = &source.host {
                source_obj["host"] = json!(host);
            }
            obj["deprecatedSource"] = source_obj;
        }
        if let Some(first_timestamp) = &event.deprecated_first_timestamp
            && let Some(seconds) = first_timestamp.seconds
        {
            obj["deprecatedFirstTimestamp"] = json!(seconds);
        }
        if let Some(last_timestamp) = &event.deprecated_last_timestamp
            && let Some(seconds) = last_timestamp.seconds
        {
            obj["deprecatedLastTimestamp"] = json!(seconds);
        }
        if let Some(count) = event.deprecated_count {
            obj["deprecatedCount"] = json!(count);
        }
    }
);

pb_decode!(
    pb_node_to_json,
    k8s_pb::api::core::v1::Node,
    node,
    "v1",
    "Node",
    obj,
    {
        if let Some(spec) = &node.spec {
            let mut spec_obj = json!({});
            if let Some(pod_cidr) = &spec.pod_cidr {
                spec_obj["podCIDR"] = json!(pod_cidr);
            }
            if !spec.pod_cid_rs.is_empty() {
                spec_obj["podCIDRs"] = json!(spec.pod_cid_rs);
            }
            if spec.unschedulable == Some(true) {
                spec_obj["unschedulable"] = json!(true);
            }
            if !spec.taints.is_empty() {
                let taints: Vec<Value> = spec
                    .taints
                    .iter()
                    .map(|taint| {
                        let mut taint_obj = json!({});
                        if let Some(key) = &taint.key {
                            taint_obj["key"] = json!(key);
                        }
                        if let Some(value) = &taint.value {
                            taint_obj["value"] = json!(value);
                        }
                        if let Some(effect) = &taint.effect {
                            taint_obj["effect"] = json!(effect);
                        }
                        taint_obj
                    })
                    .collect();
                spec_obj["taints"] = json!(taints);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &node.status {
            let mut status_obj = json!({});
            if !status.conditions.is_empty() {
                let conditions: Vec<Value> = status
                    .conditions
                    .iter()
                    .map(|cond| {
                        let mut cond_obj = json!({});
                        if let Some(typ) = &cond.r#type {
                            cond_obj["type"] = json!(typ);
                        }
                        if let Some(status_val) = &cond.status {
                            cond_obj["status"] = json!(status_val);
                        }
                        if let Some(reason) = &cond.reason {
                            cond_obj["reason"] = json!(reason);
                        }
                        if let Some(message) = &cond.message {
                            cond_obj["message"] = json!(message);
                        }
                        cond_obj
                    })
                    .collect();
                status_obj["conditions"] = json!(conditions);
            }
            if !status.addresses.is_empty() {
                let addresses: Vec<Value> = status
                    .addresses
                    .iter()
                    .map(|addr| {
                        let mut addr_obj = json!({});
                        if let Some(typ) = &addr.r#type {
                            addr_obj["type"] = json!(typ);
                        }
                        if let Some(address) = &addr.address {
                            addr_obj["address"] = json!(address);
                        }
                        addr_obj
                    })
                    .collect();
                status_obj["addresses"] = json!(addresses);
            }
            if !status.capacity.is_empty() {
                status_obj["capacity"] = pb_quantity_map_to_value(&status.capacity);
            }
            if !status.allocatable.is_empty() {
                status_obj["allocatable"] = pb_quantity_map_to_value(&status.allocatable);
            }
            if let Some(node_info) = &status.node_info {
                let mut info_obj = json!({});
                if let Some(v) = &node_info.machine_id {
                    info_obj["machineID"] = json!(v);
                }
                if let Some(v) = &node_info.system_uuid {
                    info_obj["systemUUID"] = json!(v);
                }
                if let Some(v) = &node_info.kernel_version {
                    info_obj["kernelVersion"] = json!(v);
                }
                if let Some(v) = &node_info.os_image {
                    info_obj["osImage"] = json!(v);
                }
                if let Some(v) = &node_info.container_runtime_version {
                    info_obj["containerRuntimeVersion"] = json!(v);
                }
                if let Some(v) = &node_info.kubelet_version {
                    info_obj["kubeletVersion"] = json!(v);
                }
                status_obj["nodeInfo"] = info_obj;
            }
            obj["status"] = status_obj;
        }
    }
);

/// Convert protobuf JsonSchemaProps to serde_json::Value recursively.
pub fn pb_json_schema_to_json(
    schema: &k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::JsonSchemaProps,
) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(ref d) = schema.description {
        obj["description"] = json!(d);
    }
    if let Some(ref t) = schema.r#type {
        obj["type"] = json!(t);
    }
    if let Some(ref t) = schema.title {
        obj["title"] = json!(t);
    }
    if let Some(ref f) = schema.format {
        obj["format"] = json!(f);
    }
    if let Some(ref p) = schema.pattern {
        obj["pattern"] = json!(p);
    }
    if let Some(v) = schema.maximum {
        obj["maximum"] = json!(v);
    }
    if let Some(v) = schema.minimum {
        obj["minimum"] = json!(v);
    }
    if let Some(v) = schema.max_length {
        obj["maxLength"] = json!(v);
    }
    if let Some(v) = schema.min_length {
        obj["minLength"] = json!(v);
    }
    if let Some(v) = schema.max_items {
        obj["maxItems"] = json!(v);
    }
    if let Some(v) = schema.min_items {
        obj["minItems"] = json!(v);
    }
    if let Some(v) = schema.unique_items
        && v
    {
        obj["uniqueItems"] = json!(v);
    }
    if let Some(v) = schema.max_properties {
        obj["maxProperties"] = json!(v);
    }
    if let Some(v) = schema.min_properties {
        obj["minProperties"] = json!(v);
    }
    if !schema.required.is_empty() {
        obj["required"] = json!(schema.required);
    }
    if let Some(v) = schema.nullable
        && v
    {
        obj["nullable"] = json!(v);
    }
    if let Some(v) = schema.x_kubernetes_preserve_unknown_fields
        && v
    {
        obj["x-kubernetes-preserve-unknown-fields"] = json!(v);
    }
    if let Some(v) = schema.x_kubernetes_embedded_resource
        && v
    {
        obj["x-kubernetes-embedded-resource"] = json!(v);
    }
    if let Some(v) = schema.x_kubernetes_int_or_string
        && v
    {
        obj["x-kubernetes-int-or-string"] = json!(v);
    }
    if let Some(ref v) = schema.x_kubernetes_list_type {
        obj["x-kubernetes-list-type"] = json!(v);
    }
    if !schema.x_kubernetes_list_map_keys.is_empty() {
        obj["x-kubernetes-list-map-keys"] = json!(schema.x_kubernetes_list_map_keys);
    }
    if let Some(ref v) = schema.x_kubernetes_map_type {
        obj["x-kubernetes-map-type"] = json!(v);
    }
    // Properties (recursive)
    if !schema.properties.is_empty() {
        let props: serde_json::Map<String, Value> = schema
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), pb_json_schema_to_json(v)))
            .collect();
        obj["properties"] = Value::Object(props);
    }
    // Items
    if let Some(ref items) = schema.items {
        if let Some(ref s) = items.schema {
            obj["items"] = pb_json_schema_to_json(s);
        } else if !items.j_son_schemas.is_empty() {
            let arr: Vec<Value> = items
                .j_son_schemas
                .iter()
                .map(pb_json_schema_to_json)
                .collect();
            obj["items"] = json!(arr);
        }
    }
    // AdditionalProperties
    if let Some(ref ap) = schema.additional_properties {
        if let Some(allows) = ap.allows {
            obj["additionalProperties"] = json!(allows);
        } else if let Some(ref s) = ap.schema {
            obj["additionalProperties"] = pb_json_schema_to_json(s);
        }
    }
    // Enum
    if !schema.r#enum.is_empty() {
        let enums: Vec<Value> = schema
            .r#enum
            .iter()
            .filter_map(|e| {
                e.raw
                    .as_deref()
                    .and_then(|bytes| serde_json::from_slice(bytes).ok())
            })
            .collect();
        if !enums.is_empty() {
            obj["enum"] = json!(enums);
        }
    }
    // Default
    if let Some(ref d) = schema.default
        && let Some(ref raw) = d.raw
        && let Ok(v) = serde_json::from_slice::<Value>(raw)
    {
        obj["default"] = v;
    }
    // AllOf, OneOf, AnyOf
    if !schema.all_of.is_empty() {
        obj["allOf"] = json!(
            schema
                .all_of
                .iter()
                .map(pb_json_schema_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !schema.one_of.is_empty() {
        obj["oneOf"] = json!(
            schema
                .one_of
                .iter()
                .map(pb_json_schema_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !schema.any_of.is_empty() {
        obj["anyOf"] = json!(
            schema
                .any_of
                .iter()
                .map(pb_json_schema_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(ref not) = schema.not {
        obj["not"] = pb_json_schema_to_json(not);
    }
    obj
}

pb_decode!(
    pb_crd_to_json,
    k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    crd,
    "apiextensions.k8s.io/v1",
    "CustomResourceDefinition",
    obj,
    {
        if let Some(spec) = &crd.spec {
            let mut spec_obj = json!({});
            if let Some(group) = &spec.group {
                spec_obj["group"] = json!(group);
            }
            if let Some(names) = &spec.names {
                let mut names_obj = json!({});
                if let Some(plural) = &names.plural {
                    names_obj["plural"] = json!(plural);
                }
                if let Some(singular) = &names.singular {
                    names_obj["singular"] = json!(singular);
                }
                if let Some(kind) = &names.kind {
                    names_obj["kind"] = json!(kind);
                }
                if !names.short_names.is_empty() {
                    names_obj["shortNames"] = json!(names.short_names);
                }
                if let Some(list_kind) = &names.list_kind {
                    names_obj["listKind"] = json!(list_kind);
                }
                if !names.categories.is_empty() {
                    names_obj["categories"] = json!(names.categories);
                }
                spec_obj["names"] = names_obj;
            }
            if let Some(scope) = &spec.scope {
                spec_obj["scope"] = json!(scope);
            }
            if let Some(conversion) = &spec.conversion {
                let mut conv_obj = json!({});
                if let Some(strategy) = &conversion.strategy {
                    conv_obj["strategy"] = json!(strategy);
                }
                if let Some(webhook) = &conversion.webhook {
                    let mut webhook_obj = json!({});
                    if let Some(client_config) = &webhook.client_config {
                        let mut client_obj = json!({});
                        if let Some(url) = &client_config.url {
                            client_obj["url"] = json!(url);
                        }
                        if let Some(service) = &client_config.service {
                            let mut service_obj = json!({});
                            if let Some(name) = &service.name {
                                service_obj["name"] = json!(name);
                            }
                            if let Some(namespace) = &service.namespace {
                                service_obj["namespace"] = json!(namespace);
                            }
                            if let Some(path) = &service.path {
                                service_obj["path"] = json!(path);
                            }
                            if let Some(port) = service.port {
                                service_obj["port"] = json!(port);
                            }
                            client_obj["service"] = service_obj;
                        }
                        if let Some(ca_bundle) = &client_config.ca_bundle
                            && !ca_bundle.is_empty()
                        {
                            client_obj["caBundle"] = json!(base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                ca_bundle
                            ));
                        }
                        webhook_obj["clientConfig"] = client_obj;
                    }
                    if !webhook.conversion_review_versions.is_empty() {
                        webhook_obj["conversionReviewVersions"] =
                            json!(webhook.conversion_review_versions);
                    }
                    conv_obj["webhook"] = webhook_obj;
                }
                spec_obj["conversion"] = conv_obj;
            }
            if !spec.versions.is_empty() {
                let versions: Vec<Value> = spec
                    .versions
                    .iter()
                    .map(|ver| {
                        let mut ver_obj = json!({});
                        if let Some(name) = &ver.name {
                            ver_obj["name"] = json!(name);
                        }
                        if let Some(served) = ver.served {
                            ver_obj["served"] = json!(served);
                        }
                        if let Some(storage) = ver.storage {
                            ver_obj["storage"] = json!(storage);
                        }
                        if !ver.selectable_fields.is_empty() {
                            let selectable_fields: Vec<Value> = ver
                                .selectable_fields
                                .iter()
                                .filter_map(|field| {
                                    field
                                        .json_path
                                        .as_ref()
                                        .map(|path| json!({ "jsonPath": path }))
                                })
                                .collect();
                            if !selectable_fields.is_empty() {
                                ver_obj["selectableFields"] = json!(selectable_fields);
                            }
                        }
                        // Preserve schema.openAPIV3Schema — required for CRD OpenAPI publishing
                        if let Some(validation) = &ver.schema
                            && let Some(schema) = &validation.open_apiv3_schema
                        {
                            ver_obj["schema"] =
                                json!({"openAPIV3Schema": pb_json_schema_to_json(schema)});
                        }
                        // Preserve subresources
                        if let Some(sub) = &ver.subresources {
                            let mut sub_obj = json!({});
                            if sub.status.is_some() {
                                sub_obj["status"] = json!({});
                            }
                            if let Some(scale) = &sub.scale {
                                let mut scale_obj = json!({});
                                if let Some(sr) = &scale.spec_replicas_path {
                                    scale_obj["specReplicasPath"] = json!(sr);
                                }
                                if let Some(sr) = &scale.status_replicas_path {
                                    scale_obj["statusReplicasPath"] = json!(sr);
                                }
                                if let Some(ls) = &scale.label_selector_path {
                                    scale_obj["labelSelectorPath"] = json!(ls);
                                }
                                sub_obj["scale"] = scale_obj;
                            }
                            ver_obj["subresources"] = sub_obj;
                        }
                        ver_obj
                    })
                    .collect();
                spec_obj["versions"] = json!(versions);
            }
            if let Some(preserve) = spec.preserve_unknown_fields {
                spec_obj["preserveUnknownFields"] = json!(preserve);
            }
            obj["spec"] = spec_obj;
        }
        // Decode status (needed so PUT/UpdateStatus round-trips preserve status.conditions)
        if let Some(status) = &crd.status {
            let mut status_obj = json!({});
            if !status.conditions.is_empty() {
                let conds: Vec<Value> = status
                    .conditions
                    .iter()
                    .map(|c| {
                        let mut cond = json!({});
                        // Always include type/status fields (even empty) to preserve round-trip fidelity
                        cond["type"] = json!(c.r#type.as_deref().unwrap_or(""));
                        cond["status"] = json!(c.status.as_deref().unwrap_or(""));
                        if let Some(reason) = &c.reason
                            && !reason.is_empty()
                        {
                            cond["reason"] = json!(reason);
                        }
                        if let Some(message) = &c.message
                            && !message.is_empty()
                        {
                            cond["message"] = json!(message);
                        }
                        if let Some(t) = &c.last_transition_time
                            && let Some(s) = t.seconds
                            && let Some(dt) =
                                chrono::DateTime::from_timestamp(s, t.nanos.unwrap_or(0) as u32)
                        {
                            cond["lastTransitionTime"] = json!(dt.to_rfc3339());
                        }
                        cond
                    })
                    .collect();
                status_obj["conditions"] = json!(conds);
            }
            if let Some(accepted) = &status.accepted_names {
                let mut names_obj = json!({});
                if let Some(plural) = &accepted.plural {
                    names_obj["plural"] = json!(plural);
                }
                if let Some(kind) = &accepted.kind {
                    names_obj["kind"] = json!(kind);
                }
                if let Some(singular) = &accepted.singular {
                    names_obj["singular"] = json!(singular);
                }
                if !accepted.short_names.is_empty() {
                    names_obj["shortNames"] = json!(accepted.short_names);
                }
                if let Some(list_kind) = &accepted.list_kind {
                    names_obj["listKind"] = json!(list_kind);
                }
                if !accepted.categories.is_empty() {
                    names_obj["categories"] = json!(accepted.categories);
                }
                status_obj["acceptedNames"] = names_obj;
            }
            if !status.stored_versions.is_empty() {
                status_obj["storedVersions"] = json!(status.stored_versions);
            }
            if !status_obj.as_object().is_some_and(|o| o.is_empty()) {
                obj["status"] = status_obj;
            }
        }
    }
);

pb_decode!(
    pb_lease_to_json,
    k8s_pb::api::coordination::v1::Lease,
    lease,
    "coordination.k8s.io/v1",
    "Lease",
    obj,
    {
        if let Some(spec) = &lease.spec {
            let mut spec_obj = json!({});
            if let Some(v) = &spec.holder_identity {
                spec_obj["holderIdentity"] = json!(v);
            }
            if let Some(v) = spec.lease_duration_seconds {
                spec_obj["leaseDurationSeconds"] = json!(v);
            }
            // Lease acquireTime/renewTime are metav1.MicroTime — must serialize
            // as `YYYY-MM-DDTHH:MM:SS.ffffffZ` (6 microsecond digits + Z).
            // chrono::DateTime::to_rfc3339() emits `+00:00` instead of `Z`,
            // which trips conformance Lease parsers (P0-E2E-20260423-12).
            if let Some(acquire_time) = &spec.acquire_time
                && let Some(seconds) = acquire_time.seconds
            {
                let ts = chrono::DateTime::from_timestamp(
                    seconds,
                    acquire_time.nanos.unwrap_or(0) as u32,
                )
                .map(crate::utils::k8s_microtime_format)
                .unwrap_or_default();
                spec_obj["acquireTime"] = json!(ts);
            }
            if let Some(renew_time) = &spec.renew_time
                && let Some(seconds) = renew_time.seconds
            {
                let ts =
                    chrono::DateTime::from_timestamp(seconds, renew_time.nanos.unwrap_or(0) as u32)
                        .map(crate::utils::k8s_microtime_format)
                        .unwrap_or_default();
                spec_obj["renewTime"] = json!(ts);
            }
            if let Some(v) = spec.lease_transitions {
                spec_obj["leaseTransitions"] = json!(v);
            }
            if let Some(v) = &spec.preferred_holder {
                spec_obj["preferredHolder"] = json!(v);
            }
            if let Some(v) = &spec.strategy {
                spec_obj["strategy"] = json!(v);
            }
            obj["spec"] = spec_obj;
        }
    }
);

pb_decode!(
    pb_podtemplate_to_json,
    k8s_pb::api::core::v1::PodTemplate,
    pt,
    "v1",
    "PodTemplate",
    obj,
    {
        if let Some(template) = &pt.template {
            obj["template"] = pb_pod_template_spec_to_json(template);
        }
    }
);
