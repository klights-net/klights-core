/// Convert a serde_json::Value representing an OpenAPI v3 schema to a protobuf JsonSchemaProps.
/// Recursively converts all schema fields.
use crate::protobuf::*;
pub fn json_value_to_pb_schema(
    v: &Value,
) -> k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::JsonSchemaProps {
    use k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1 as pb_ext;
    let obj = match v.as_object() {
        Some(o) => o,
        None => return pb_ext::JsonSchemaProps::default(),
    };

    let mut props = pb_ext::JsonSchemaProps::default();

    if let Some(s) = obj.get("description").and_then(|v| v.as_str()) {
        props.description = Some(s.to_string());
    }
    if let Some(s) = obj.get("type").and_then(|v| v.as_str()) {
        props.r#type = Some(s.to_string());
    }
    if let Some(s) = obj.get("title").and_then(|v| v.as_str()) {
        props.title = Some(s.to_string());
    }
    if let Some(s) = obj.get("format").and_then(|v| v.as_str()) {
        props.format = Some(s.to_string());
    }
    if let Some(s) = obj.get("pattern").and_then(|v| v.as_str()) {
        props.pattern = Some(s.to_string());
    }
    if let Some(n) = obj.get("maximum").and_then(|v| v.as_f64()) {
        props.maximum = Some(n);
    }
    if let Some(n) = obj.get("minimum").and_then(|v| v.as_f64()) {
        props.minimum = Some(n);
    }
    if let Some(n) = obj.get("maxLength").and_then(|v| v.as_i64()) {
        props.max_length = Some(n);
    }
    if let Some(n) = obj.get("minLength").and_then(|v| v.as_i64()) {
        props.min_length = Some(n);
    }
    if let Some(n) = obj.get("maxItems").and_then(|v| v.as_i64()) {
        props.max_items = Some(n);
    }
    if let Some(n) = obj.get("minItems").and_then(|v| v.as_i64()) {
        props.min_items = Some(n);
    }
    if let Some(b) = obj.get("uniqueItems").and_then(|v| v.as_bool()) {
        props.unique_items = Some(b);
    }
    if let Some(n) = obj.get("maxProperties").and_then(|v| v.as_i64()) {
        props.max_properties = Some(n);
    }
    if let Some(n) = obj.get("minProperties").and_then(|v| v.as_i64()) {
        props.min_properties = Some(n);
    }
    if let Some(b) = obj.get("nullable").and_then(|v| v.as_bool()) {
        props.nullable = Some(b);
    }
    if let Some(b) = obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
    {
        props.x_kubernetes_preserve_unknown_fields = Some(b);
    }
    if let Some(b) = obj
        .get("x-kubernetes-embedded-resource")
        .and_then(|v| v.as_bool())
    {
        props.x_kubernetes_embedded_resource = Some(b);
    }
    if let Some(b) = obj
        .get("x-kubernetes-int-or-string")
        .and_then(|v| v.as_bool())
    {
        props.x_kubernetes_int_or_string = Some(b);
    }
    if let Some(s) = obj.get("x-kubernetes-list-type").and_then(|v| v.as_str()) {
        props.x_kubernetes_list_type = Some(s.to_string());
    }
    if let Some(keys) = obj
        .get("x-kubernetes-list-map-keys")
        .and_then(|v| v.as_array())
    {
        props.x_kubernetes_list_map_keys = keys
            .iter()
            .filter_map(|k| k.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    if let Some(s) = obj.get("x-kubernetes-map-type").and_then(|v| v.as_str()) {
        props.x_kubernetes_map_type = Some(s.to_string());
    }
    if let Some(req) = obj.get("required").and_then(|v| v.as_array()) {
        props.required = req
            .iter()
            .filter_map(|r| r.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    // properties
    if let Some(properties) = obj.get("properties").and_then(|v| v.as_object()) {
        props.properties = properties
            .iter()
            .map(|(k, v)| (k.clone(), json_value_to_pb_schema(v)))
            .collect();
    }
    // items — can be a single schema or array of schemas
    if let Some(items_val) = obj.get("items") {
        if items_val.is_array() {
            let schemas: Vec<pb_ext::JsonSchemaProps> = items_val
                .as_array()
                .unwrap()
                .iter()
                .map(json_value_to_pb_schema)
                .collect();
            props.items = Some(Box::new(pb_ext::JsonSchemaPropsOrArray {
                schema: None,
                j_son_schemas: schemas,
            }));
        } else {
            props.items = Some(Box::new(pb_ext::JsonSchemaPropsOrArray {
                schema: Some(Box::new(json_value_to_pb_schema(items_val))),
                j_son_schemas: vec![],
            }));
        }
    }
    // additionalProperties — can be bool or schema
    if let Some(ap) = obj.get("additionalProperties") {
        if let Some(b) = ap.as_bool() {
            props.additional_properties = Some(Box::new(pb_ext::JsonSchemaPropsOrBool {
                allows: Some(b),
                schema: None,
            }));
        } else {
            props.additional_properties = Some(Box::new(pb_ext::JsonSchemaPropsOrBool {
                allows: None,
                schema: Some(Box::new(json_value_to_pb_schema(ap))),
            }));
        }
    }
    // enum
    if let Some(enums) = obj.get("enum").and_then(|v| v.as_array()) {
        props.r#enum = enums
            .iter()
            .filter_map(|e| {
                serde_json::to_vec(e)
                    .ok()
                    .map(|raw| pb_ext::Json { raw: Some(raw) })
            })
            .collect();
    }
    // default
    if let Some(default_val) = obj.get("default")
        && let Ok(raw) = serde_json::to_vec(default_val)
    {
        props.default = Some(pb_ext::Json { raw: Some(raw) });
    }
    // allOf, oneOf, anyOf
    if let Some(all) = obj.get("allOf").and_then(|v| v.as_array()) {
        props.all_of = all.iter().map(json_value_to_pb_schema).collect();
    }
    if let Some(one) = obj.get("oneOf").and_then(|v| v.as_array()) {
        props.one_of = one.iter().map(json_value_to_pb_schema).collect();
    }
    if let Some(any) = obj.get("anyOf").and_then(|v| v.as_array()) {
        props.any_of = any.iter().map(json_value_to_pb_schema).collect();
    }
    if let Some(not) = obj.get("not") {
        props.not = Some(Box::new(json_value_to_pb_schema(not)));
    }

    props
}

/// Convert k8s-openapi CustomResourceDefinition to k8s-pb CustomResourceDefinition.
/// Takes the raw JSON value alongside the typed struct to preserve schema fields.
pub fn json_crd_to_pb(
    crd: &k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    raw: &Value,
) -> anyhow::Result<
    k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
> {
    use k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1 as pb_ext;

    // Extract raw versions array for schema access (k8s_openapi drops schema fields)
    let raw_versions: Vec<&Value> = raw
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().collect())
        .unwrap_or_default();

    Ok(
        pb_ext::CustomResourceDefinition {
            metadata: Some(json_meta_to_pb(&crd.metadata)),
            spec: Some(pb_ext::CustomResourceDefinitionSpec {
                    group: Some(crd.spec.group.clone()),
                    names: Some(pb_ext::CustomResourceDefinitionNames {
                        plural: Some(crd.spec.names.plural.clone()),
                        singular: crd.spec.names.singular.clone(),
                        short_names: crd.spec.names.short_names.clone().unwrap_or_default(),
                        kind: Some(crd.spec.names.kind.clone()),
                        list_kind: crd.spec.names.list_kind.clone(),
                        categories: crd.spec.names.categories.clone().unwrap_or_default(),
                    }),
                    scope: Some(crd.spec.scope.clone()),
                    versions: crd.spec
                        .versions
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            // Get schema from raw JSON to avoid k8s_openapi dropping it
                            let schema = raw_versions
                                .get(i)
                                .and_then(|rv| rv.pointer("/schema/openAPIV3Schema"))
                                .map(|s| pb_ext::CustomResourceValidation {
                                    open_apiv3_schema: Some(json_value_to_pb_schema(s)),
                                });
                            let selectable_fields = if let Some(fields) = &v.selectable_fields {
                                fields
                                    .iter()
                                    .map(|field| pb_ext::SelectableField {
                                        json_path: Some(field.json_path.clone()),
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                raw_versions
                                    .get(i)
                                    .and_then(|rv| rv.get("selectableFields"))
                                    .and_then(|sf| sf.as_array())
                                    .map(|fields| {
                                        fields
                                            .iter()
                                            .filter_map(|field| {
                                                field
                                                    .get("jsonPath")
                                                    .and_then(|jp| jp.as_str())
                                                    .map(|path| pb_ext::SelectableField {
                                                        json_path: Some(path.to_string()),
                                                    })
                                            })
                                            .collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default()
                            };
                            pb_ext::CustomResourceDefinitionVersion {
                                name: Some(v.name.clone()),
                                served: Some(v.served),
                                storage: Some(v.storage),
                                deprecated: v.deprecated,
                                deprecation_warning: v.deprecation_warning.clone(),
                                schema,
                                selectable_fields,
                                ..Default::default()
                            }
                        })
                        .collect(),
                    conversion: crd.spec.conversion.as_ref().map(|conv| {
                        k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceConversion {
                            strategy: Some(conv.strategy.clone()),
                            webhook: conv.webhook.as_ref().map(|wh| {
                                k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::WebhookConversion {
                                    client_config: wh.client_config.as_ref().map(|cc| {
                                        k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::WebhookClientConfig {
                                            url: cc.url.clone(),
                                            service: cc.service.as_ref().map(|svc| {
                                                k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::ServiceReference {
                                                    name: Some(svc.name.clone()),
                                                    namespace: Some(svc.namespace.clone()),
                                                    path: svc.path.clone(),
                                                    port: svc.port,
                                                }
                                            }),
                                            ca_bundle: cc.ca_bundle.as_ref().map(|b| b.0.clone()),
                                        }
                                    }),
                                    conversion_review_versions: wh.conversion_review_versions.clone(),
                                }
                            }),
                        }
                    }),
                    preserve_unknown_fields: crd.spec.preserve_unknown_fields,
                }),
            status: crd.status.as_ref().map(|status| {
                k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionStatus {
                    conditions: status
                        .conditions
                        .as_ref()
                        .map(|conds| {
                            conds
                                .iter()
                                .map(|c| {
                                    k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionCondition {
                                        r#type: Some(c.type_.clone()),
                                        status: Some(c.status.clone()),
                                        last_transition_time: c.last_transition_time.as_ref().map(json_time_to_pb),
                                        reason: c.reason.clone(),
                                        message: c.message.clone(),
                                        observed_generation: None,
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    accepted_names: status.accepted_names.as_ref().map(|names| {
                        k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionNames {
                            plural: Some(names.plural.clone()),
                            singular: names.singular.clone(),
                            short_names: names.short_names.clone().unwrap_or_default(),
                            kind: Some(names.kind.clone()),
                            list_kind: names.list_kind.clone(),
                            categories: names.categories.clone().unwrap_or_default(),
                        }
                    }),
                    stored_versions: status.stored_versions.clone().unwrap_or_default(),
                    observed_generation: None,
                }
            }),
        },
    )
}

/// Encode CustomResourceDefinitionList from JSON value to protobuf
pub fn json_crdlist_to_pb(
    value: &Value,
) -> anyhow::Result<
    k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionList,
> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CustomResourceDefinitionList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition::deserialize(item)?;
            json_crd_to_pb(&openapi, item)
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionList {
        metadata,
        items: pb_items,
    })
}
