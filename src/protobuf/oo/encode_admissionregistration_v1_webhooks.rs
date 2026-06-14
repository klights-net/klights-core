/// Encode ValidatingAdmissionPolicy to protobuf (minimal implementation)
use crate::protobuf::*;
pub fn json_validating_admission_policy_to_pb(
    vap: &k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicy,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicy> {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    Ok(admissionv1::ValidatingAdmissionPolicy {
        metadata: Some(json_meta_to_pb(&vap.metadata)),
        spec: vap
            .spec
            .as_ref()
            .map(|_| admissionv1::ValidatingAdmissionPolicySpec {
                ..Default::default()
            }),
        status: None,
    })
}

/// Encode ValidatingAdmissionPolicyBinding to protobuf (minimal implementation)
pub fn json_validating_admission_policy_binding_to_pb(
    vapb: &k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding> {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    Ok(admissionv1::ValidatingAdmissionPolicyBinding {
        metadata: Some(json_meta_to_pb(&vapb.metadata)),
        spec: vapb
            .spec
            .as_ref()
            .map(|spec| admissionv1::ValidatingAdmissionPolicyBindingSpec {
                policy_name: spec.policy_name.clone(),
                ..Default::default()
            }),
    })
}

pub fn json_admission_rule_with_operations_to_pb(
    r: &k8s_openapi::api::admissionregistration::v1::RuleWithOperations,
) -> k8s_pb::api::admissionregistration::v1::RuleWithOperations {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    admissionv1::RuleWithOperations {
        operations: r
            .operations
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|op| normalize_admission_operation(&op))
            .collect(),
        rule: Some(admissionv1::Rule {
            api_groups: r.api_groups.clone().unwrap_or_default(),
            api_versions: r.api_versions.clone().unwrap_or_default(),
            resources: r.resources.clone().unwrap_or_default(),
            scope: r.scope.clone(),
        }),
    }
}

pub fn json_admission_webhook_client_config_to_pb(
    cc: &k8s_openapi::api::admissionregistration::v1::WebhookClientConfig,
) -> k8s_pb::api::admissionregistration::v1::WebhookClientConfig {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    admissionv1::WebhookClientConfig {
        url: cc.url.clone(),
        service: cc
            .service
            .as_ref()
            .map(|svc| admissionv1::ServiceReference {
                namespace: Some(svc.namespace.clone()),
                name: Some(svc.name.clone()),
                path: svc.path.clone(),
                port: svc.port,
            }),
        ca_bundle: cc.ca_bundle.as_ref().map(|b| b.0.clone()),
    }
}

/// Encode MutatingWebhookConfiguration to protobuf.
pub fn json_mutating_webhook_configuration_to_pb(
    mwc: &k8s_openapi::api::admissionregistration::v1::MutatingWebhookConfiguration,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::MutatingWebhookConfiguration> {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    Ok(admissionv1::MutatingWebhookConfiguration {
        metadata: Some(json_meta_to_pb(&mwc.metadata)),
        webhooks: mwc
            .webhooks
            .as_ref()
            .map(|webhooks| {
                webhooks
                    .iter()
                    .map(|wh| admissionv1::MutatingWebhook {
                        name: Some(wh.name.clone()),
                        client_config: Some(json_admission_webhook_client_config_to_pb(
                            &wh.client_config,
                        )),
                        rules: wh
                            .rules
                            .as_ref()
                            .map(|rules| {
                                rules
                                    .iter()
                                    .map(json_admission_rule_with_operations_to_pb)
                                    .collect()
                            })
                            .unwrap_or_default(),
                        failure_policy: wh.failure_policy.clone(),
                        match_policy: wh.match_policy.clone(),
                        namespace_selector: wh
                            .namespace_selector
                            .as_ref()
                            .map(json_label_selector_to_pb),
                        object_selector: wh.object_selector.as_ref().map(json_label_selector_to_pb),
                        side_effects: Some(wh.side_effects.clone()),
                        timeout_seconds: wh.timeout_seconds,
                        admission_review_versions: wh.admission_review_versions.clone(),
                        reinvocation_policy: wh.reinvocation_policy.clone(),
                        match_conditions: wh
                            .match_conditions
                            .as_ref()
                            .map(|conds| {
                                conds
                                    .iter()
                                    .map(|c| admissionv1::MatchCondition {
                                        name: Some(c.name.clone()),
                                        expression: Some(c.expression.clone()),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// Encode ValidatingWebhookConfiguration to protobuf.
pub fn json_validating_webhook_configuration_to_pb(
    vwc: &k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfiguration> {
    use k8s_pb::api::admissionregistration::v1 as admissionv1;
    Ok(admissionv1::ValidatingWebhookConfiguration {
        metadata: Some(json_meta_to_pb(&vwc.metadata)),
        webhooks: vwc
            .webhooks
            .as_ref()
            .map(|webhooks| {
                webhooks
                    .iter()
                    .map(|wh| admissionv1::ValidatingWebhook {
                        name: Some(wh.name.clone()),
                        client_config: Some(json_admission_webhook_client_config_to_pb(
                            &wh.client_config,
                        )),
                        rules: wh
                            .rules
                            .as_ref()
                            .map(|rules| {
                                rules
                                    .iter()
                                    .map(json_admission_rule_with_operations_to_pb)
                                    .collect()
                            })
                            .unwrap_or_default(),
                        failure_policy: wh.failure_policy.clone(),
                        match_policy: wh.match_policy.clone(),
                        namespace_selector: wh
                            .namespace_selector
                            .as_ref()
                            .map(json_label_selector_to_pb),
                        object_selector: wh.object_selector.as_ref().map(json_label_selector_to_pb),
                        side_effects: Some(wh.side_effects.clone()),
                        timeout_seconds: wh.timeout_seconds,
                        admission_review_versions: wh.admission_review_versions.clone(),
                        match_conditions: wh
                            .match_conditions
                            .as_ref()
                            .map(|conds| {
                                conds
                                    .iter()
                                    .map(|c| admissionv1::MatchCondition {
                                        name: Some(c.name.clone()),
                                        expression: Some(c.expression.clone()),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}
