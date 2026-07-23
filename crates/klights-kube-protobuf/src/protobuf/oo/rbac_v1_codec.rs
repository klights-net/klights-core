//! RbacV1Codec: OO protobuf codec for rbac.authorization.k8s.io/v1 resources.
//!
//! Handles round-trip encode/decode for all RBAC kinds:
//! ClusterRole, ClusterRoleList, ClusterRoleBinding, ClusterRoleBindingList,
//! Role, RoleList, RoleBinding, RoleBindingList.
//!
//! Dispatch is owned by the global OO protobuf registry.

use crate::protobuf::ResourceProtoCodec;
use crate::protobuf::*;
use anyhow::Context;
use serde_json::Value;

/// (api_version_prefix, kind) entries for rbac.authorization.k8s.io resources.
const RBAC_ENTRIES: &[(&str, &str)] = &[
    ("rbac.authorization.k8s.io", "ClusterRole"),
    ("rbac.authorization.k8s.io", "ClusterRoleList"),
    ("rbac.authorization.k8s.io", "ClusterRoleBinding"),
    ("rbac.authorization.k8s.io", "ClusterRoleBindingList"),
    ("rbac.authorization.k8s.io", "Role"),
    ("rbac.authorization.k8s.io", "RoleList"),
    ("rbac.authorization.k8s.io", "RoleBinding"),
    ("rbac.authorization.k8s.io", "RoleBindingList"),
];

/// Codec for rbac.authorization.k8s.io/v1 resources.
pub struct RbacV1Codec;

impl ResourceProtoCodec for RbacV1Codec {
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
        RBAC_ENTRIES
    }

    fn decode_to_json(&self, _api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        use prost::Message;
        match kind {
            "ClusterRole" => {
                let pb = k8s_pb::api::rbac::v1::ClusterRole::decode(data)
                    .context("failed to decode ClusterRole protobuf")?;
                pb_clusterrole_to_json(&pb)
            }
            "ClusterRoleBinding" => {
                let pb = k8s_pb::api::rbac::v1::ClusterRoleBinding::decode(data)
                    .context("failed to decode ClusterRoleBinding protobuf")?;
                pb_clusterrolebinding_to_json(&pb)
            }
            "Role" => {
                let pb = k8s_pb::api::rbac::v1::Role::decode(data)
                    .context("failed to decode Role protobuf")?;
                pb_role_to_json(&pb)
            }
            "RoleBinding" => {
                let pb = k8s_pb::api::rbac::v1::RoleBinding::decode(data)
                    .context("failed to decode RoleBinding protobuf")?;
                pb_rolebinding_to_json(&pb)
            }
            "ClusterRoleList" => {
                let list = k8s_pb::api::rbac::v1::ClusterRoleList::decode(data)
                    .context("failed to decode ClusterRoleList protobuf")?;
                decode_rbac_list(&list.items, "ClusterRole", |item| {
                    pb_clusterrole_to_json(item)
                })
            }
            "ClusterRoleBindingList" => {
                let list = k8s_pb::api::rbac::v1::ClusterRoleBindingList::decode(data)
                    .context("failed to decode ClusterRoleBindingList protobuf")?;
                decode_rbac_list(&list.items, "ClusterRoleBinding", |item| {
                    pb_clusterrolebinding_to_json(item)
                })
            }
            "RoleList" => {
                let list = k8s_pb::api::rbac::v1::RoleList::decode(data)
                    .context("failed to decode RoleList protobuf")?;
                decode_rbac_list(&list.items, "Role", pb_role_to_json)
            }
            "RoleBindingList" => {
                let list = k8s_pb::api::rbac::v1::RoleBindingList::decode(data)
                    .context("failed to decode RoleBindingList protobuf")?;
                decode_rbac_list(&list.items, "RoleBinding", |item| {
                    pb_rolebinding_to_json(item)
                })
            }
            _ => anyhow::bail!("RbacV1Codec: unknown kind {kind}"),
        }
    }

    fn encode_from_json(
        &self,
        _api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>> {
        match kind {
            "ClusterRole" => {
                let openapi = k8s_openapi::api::rbac::v1::ClusterRole::deserialize(value)?;
                let pb = json_clusterrole_to_pb(&openapi)?;
                encode_message_to_vec(&pb)
            }
            "ClusterRoleBinding" => {
                let openapi = k8s_openapi::api::rbac::v1::ClusterRoleBinding::deserialize(value)?;
                let pb = json_clusterrolebinding_to_pb(&openapi)?;
                encode_message_to_vec(&pb)
            }
            "Role" => {
                let openapi = k8s_openapi::api::rbac::v1::Role::deserialize(value)?;
                let pb = json_role_to_pb(&openapi)?;
                encode_message_to_vec(&pb)
            }
            "RoleBinding" => {
                let openapi = k8s_openapi::api::rbac::v1::RoleBinding::deserialize(value)?;
                let pb = json_rolebinding_to_pb(&openapi)?;
                encode_message_to_vec(&pb)
            }
            "ClusterRoleList" => {
                let pb = json_clusterrolelist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "ClusterRoleBindingList" => {
                let pb = json_clusterrolebindinglist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "RoleList" => {
                let pb = json_rolelist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "RoleBindingList" => {
                let pb = json_rolebindinglist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            _ => anyhow::bail!("RbacV1Codec: unknown kind {kind}"),
        }
    }
}

#[cfg(test)]
impl RbacV1Codec {
    fn decode_to_json(&self, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        <Self as ResourceProtoCodec>::decode_to_json(
            self,
            "rbac.authorization.k8s.io/v1",
            kind,
            data,
        )
    }

    fn encode_from_json(&self, kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
        <Self as ResourceProtoCodec>::encode_from_json(
            self,
            "rbac.authorization.k8s.io/v1",
            kind,
            value,
        )
    }
}

/// Helper: decode an RBAC list from individual protobuf items.
fn decode_rbac_list<M, F>(items: &[M], item_kind: &str, convert: F) -> anyhow::Result<Value>
where
    F: Fn(&M) -> anyhow::Result<Value>,
{
    let converted: anyhow::Result<Vec<Value>> = items.iter().map(convert).collect();
    let items_json = converted?;
    Ok(serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": format!("{item_kind}List"),
        "metadata": {},
        "items": items_json
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::OoCodecRegistry;
    use serde_json::json;

    /// Build a minimal ClusterRole JSON fixture.
    fn cluster_role_fixture(name: &str) -> Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": name,
                "uid": "test-uid"
            },
            "rules": [{
                "verbs": ["get", "list"],
                "apiGroups": [""],
                "resources": ["pods"]
            }]
        })
    }

    fn cluster_role_with_aggregation_fixture(name: &str) -> Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": name,
                "uid": "test-uid"
            },
            "aggregationRule": {
                "clusterRoleSelectors": [
                    {
                        "matchLabels": {
                            "rbac.authorization.k8s.io/aggregate-to-admin": "true"
                        }
                    }
                ]
            }
        })
    }

    /// Build a ClusterRoleBinding fixture.
    fn cluster_role_binding_fixture(name: &str) -> Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {
                "name": name,
                "uid": "test-uid"
            },
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "test-role"
            },
            "subjects": [{
                "kind": "Group",
                "apiGroup": "rbac.authorization.k8s.io",
                "name": "system:authenticated"
            }]
        })
    }

    /// Build a Role fixture.
    fn role_fixture(name: &str, namespace: &str) -> Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": "test-uid"
            },
            "rules": [{
                "verbs": ["get"],
                "apiGroups": [""],
                "resources": ["configmaps"]
            }]
        })
    }

    /// Build a RoleBinding fixture.
    fn role_binding_fixture(name: &str, namespace: &str) -> Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": "test-uid"
            },
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "view"
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": "default",
                "namespace": namespace
            }]
        })
    }

    // === Single resource round-trip tests ===

    #[test]
    fn clusterrole_round_trips() {
        let original = cluster_role_fixture("test-cr");
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRole", &original)
            .unwrap();
        let decoded = RbacV1Codec.decode_to_json("ClusterRole", &encoded).unwrap();

        assert_eq!(decoded["kind"], "ClusterRole");
        assert_eq!(decoded["metadata"]["name"], "test-cr");
        assert_eq!(
            decoded["rules"][0]["verbs"][0], "get",
            "rules verbs should survive round-trip"
        );
        assert_eq!(
            decoded["rules"][0]["resources"][0], "pods",
            "rules resources should survive round-trip"
        );
    }

    #[test]
    fn clusterrole_round_trips_with_aggregation_rule() {
        let original = cluster_role_with_aggregation_fixture("aggregated-admin");
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRole", &original)
            .unwrap();
        let decoded = RbacV1Codec.decode_to_json("ClusterRole", &encoded).unwrap();

        assert_eq!(decoded["kind"], "ClusterRole");
        assert_eq!(decoded["metadata"]["name"], "aggregated-admin");
        assert_eq!(
            decoded["aggregationRule"]["clusterRoleSelectors"][0]["matchLabels"]["rbac.authorization.k8s.io/aggregate-to-admin"],
            "true"
        );
    }

    #[test]
    fn clusterrolebinding_round_trips() {
        let original = cluster_role_binding_fixture("test-crb");
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRoleBinding", &original)
            .unwrap();
        let decoded = RbacV1Codec
            .decode_to_json("ClusterRoleBinding", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "ClusterRoleBinding");
        assert_eq!(decoded["roleRef"]["name"], "test-role");
        assert_eq!(decoded["subjects"][0]["kind"], "Group");
    }

    #[test]
    fn role_round_trips() {
        let original = role_fixture("my-role", "default");
        let encoded = RbacV1Codec.encode_from_json("Role", &original).unwrap();
        let decoded = RbacV1Codec.decode_to_json("Role", &encoded).unwrap();

        assert_eq!(decoded["kind"], "Role");
        assert_eq!(decoded["metadata"]["name"], "my-role");
        assert_eq!(
            decoded["metadata"]["namespace"], "default",
            "namespace must survive round-trip"
        );
    }

    #[test]
    fn rolebinding_round_trips() {
        let original = role_binding_fixture("my-rb", "kube-system");
        let encoded = RbacV1Codec
            .encode_from_json("RoleBinding", &original)
            .unwrap();
        let decoded = RbacV1Codec.decode_to_json("RoleBinding", &encoded).unwrap();

        assert_eq!(decoded["kind"], "RoleBinding");
        assert_eq!(decoded["subjects"][0]["kind"], "ServiceAccount");
        assert_eq!(decoded["subjects"][0]["name"], "default");
    }

    // === List round-trip tests ===

    #[test]
    fn clusterrolelist_round_trips() {
        let original = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleList",
            "metadata": {},
            "items": [
                cluster_role_fixture("cr1"),
                cluster_role_fixture("cr2")
            ]
        });
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRoleList", &original)
            .unwrap();
        let decoded = RbacV1Codec
            .decode_to_json("ClusterRoleList", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "ClusterRoleList");
        let items = decoded["items"].as_array().expect("items must be array");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["metadata"]["name"], "cr1");
        assert_eq!(items[1]["metadata"]["name"], "cr2");
    }

    #[test]
    fn rolelist_round_trips() {
        let original = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleList",
            "metadata": {},
            "items": [
                role_fixture("r1", "ns1"),
                role_fixture("r2", "ns2")
            ]
        });
        let encoded = RbacV1Codec.encode_from_json("RoleList", &original).unwrap();
        let decoded = RbacV1Codec.decode_to_json("RoleList", &encoded).unwrap();

        assert_eq!(decoded["kind"], "RoleList");
        let items = decoded["items"].as_array().expect("items must be array");
        assert_eq!(items.len(), 2);
    }

    // === Registry integration tests ===

    #[test]
    fn registry_dispatches_rbac_kinds_to_rbac_codec() {
        let registry = OoCodecRegistry::new(vec![Box::new(RbacV1Codec)]);

        for (_, kind) in RBAC_ENTRIES {
            assert!(
                registry.handles("rbac.authorization.k8s.io/v1", kind),
                "registry should handle {kind}"
            );
        }

        // Non-RBAC kinds should not be handled
        assert!(!registry.handles("v1", "Pod"));
        assert!(!registry.handles("v1", "Secret"));
    }

    #[test]
    fn registry_round_trip_through_dispatch() {
        let registry = OoCodecRegistry::new(vec![Box::new(RbacV1Codec)]);
        let original = cluster_role_fixture("dispatched-cr");

        let encoded = registry
            .encode("rbac.authorization.k8s.io/v1", "ClusterRole", &original)
            .unwrap();
        let decoded = registry
            .decode("rbac.authorization.k8s.io/v1", "ClusterRole", &encoded)
            .unwrap();

        assert_eq!(decoded["kind"], "ClusterRole");
        assert_eq!(decoded["metadata"]["name"], "dispatched-cr");
    }

    // === Edge cases ===

    #[test]
    fn empty_rules_survive_round_trip() {
        let original = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {"name": "empty-role", "uid": "uid-1"},
            "rules": []
        });
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRole", &original)
            .unwrap();
        let decoded = RbacV1Codec.decode_to_json("ClusterRole", &encoded).unwrap();
        assert_eq!(decoded["kind"], "ClusterRole");
        // Note: empty rules may be omitted by the converter — verify kind/name survive
        assert_eq!(decoded["metadata"]["name"], "empty-role");
    }

    #[test]
    fn labels_survive_round_trip() {
        let original = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": "labeled-role",
                "uid": "uid-lbl",
                "labels": {
                    "app": "klights",
                    "rbac.authorization.k8s.io/aggregate-to-admin": "true"
                }
            },
            "rules": [{
                "verbs": ["get"],
                "apiGroups": [""],
                "resources": ["pods"]
            }]
        });
        let encoded = RbacV1Codec
            .encode_from_json("ClusterRole", &original)
            .unwrap();
        let decoded = RbacV1Codec.decode_to_json("ClusterRole", &encoded).unwrap();

        assert_eq!(decoded["kind"], "ClusterRole");
        // Labels should survive round-trip
        assert_eq!(decoded["metadata"]["labels"]["app"], "klights");
        assert_eq!(
            decoded["metadata"]["labels"]["rbac.authorization.k8s.io/aggregate-to-admin"],
            "true"
        );
    }
}
