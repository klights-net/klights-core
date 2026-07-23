/// Encode ValidatingAdmissionPolicyList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_validatingadmissionpolicylist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ValidatingAdmissionPolicyList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicy::deserialize(item)?;
            json_validating_admission_policy_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(
        k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyList {
            metadata,
            items: pb_items,
        },
    )
}

/// Encode ValidatingAdmissionPolicyBindingList from JSON value to protobuf
pub fn json_validatingadmissionpolicybindinglist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            anyhow::anyhow!("ValidatingAdmissionPolicyBindingList missing items array")
        })?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding::deserialize(item)?;
            json_validating_admission_policy_binding_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(
        k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingList {
            metadata,
            items: pb_items,
        },
    )
}

/// Encode MutatingWebhookConfigurationList from JSON value to protobuf
pub fn json_mutatingwebhookconfigurationlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::MutatingWebhookConfigurationList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("MutatingWebhookConfigurationList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::admissionregistration::v1::MutatingWebhookConfiguration::deserialize(item)?;
            json_mutating_webhook_configuration_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(
        k8s_pb::api::admissionregistration::v1::MutatingWebhookConfigurationList {
            metadata,
            items: pb_items,
        },
    )
}

/// Encode ValidatingWebhookConfigurationList from JSON value to protobuf
pub fn json_validatingwebhookconfigurationlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfigurationList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ValidatingWebhookConfigurationList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration::deserialize(item)?;
            json_validating_webhook_configuration_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(
        k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfigurationList {
            metadata,
            items: pb_items,
        },
    )
}
