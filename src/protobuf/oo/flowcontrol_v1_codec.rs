//! FlowcontrolV1Codec: OO protobuf codec for flowcontrol.apiserver.k8s.io/v1 resources.
//!
//! Handles round-trip encode/decode for FlowSchema and
//! PriorityLevelConfiguration resources.

use crate::protobuf::ResourceProtoCodec;
use crate::protobuf::*;
use anyhow::Context;
use prost::Message;
use serde_json::Value;

const FLOWCONTROL_ENTRIES: &[(&str, &str)] = &[
    ("flowcontrol.apiserver.k8s.io", "FlowSchema"),
    ("flowcontrol.apiserver.k8s.io", "PriorityLevelConfiguration"),
    ("flowcontrol.apiserver.k8s.io", "FlowSchemaList"),
    (
        "flowcontrol.apiserver.k8s.io",
        "PriorityLevelConfigurationList",
    ),
];

pub struct FlowcontrolV1Codec;

impl ResourceProtoCodec for FlowcontrolV1Codec {
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
        FLOWCONTROL_ENTRIES
    }

    fn decode_to_json(&self, _api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        match kind {
            "FlowSchema" => {
                let pb = k8s_pb::api::flowcontrol::v1::FlowSchema::decode(data)
                    .context("failed to decode FlowSchema protobuf")?;
                pb_flowschema_to_json(&pb)
            }
            "PriorityLevelConfiguration" => {
                let pb = k8s_pb::api::flowcontrol::v1::PriorityLevelConfiguration::decode(data)
                    .context("failed to decode PriorityLevelConfiguration protobuf")?;
                pb_prioritylevelconfiguration_to_json(&pb)
            }
            "FlowSchemaList" => {
                let pb = k8s_pb::api::flowcontrol::v1::FlowSchemaList::decode(data)
                    .context("failed to decode FlowSchemaList protobuf")?;
                pb_flowschemalist_to_json(&pb)
            }
            "PriorityLevelConfigurationList" => {
                let pb = k8s_pb::api::flowcontrol::v1::PriorityLevelConfigurationList::decode(data)
                    .context("failed to decode PriorityLevelConfigurationList protobuf")?;
                pb_prioritylevelconfigurationlist_to_json(&pb)
            }
            _ => anyhow::bail!("FlowcontrolV1Codec: unknown kind {kind}"),
        }
    }

    fn encode_from_json(
        &self,
        _api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>> {
        match kind {
            "FlowSchema" => {
                let pb = json_flowschema_to_pb(value);
                encode_message_to_vec(&pb)
            }
            "PriorityLevelConfiguration" => {
                let pb = json_prioritylevelconfiguration_to_pb(value);
                encode_message_to_vec(&pb)
            }
            "FlowSchemaList" => {
                let pb = json_flowschemalist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            "PriorityLevelConfigurationList" => {
                let pb = json_prioritylevelconfigurationlist_to_pb(value)?;
                encode_message_to_vec(&pb)
            }
            _ => anyhow::bail!("FlowcontrolV1Codec: unknown kind {kind}"),
        }
    }
}

#[cfg(test)]
impl FlowcontrolV1Codec {
    fn decode_to_json(&self, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        <Self as ResourceProtoCodec>::decode_to_json(
            self,
            "flowcontrol.apiserver.k8s.io/v1",
            kind,
            data,
        )
    }

    fn encode_from_json(&self, kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
        <Self as ResourceProtoCodec>::encode_from_json(
            self,
            "flowcontrol.apiserver.k8s.io/v1",
            kind,
            value,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::OoCodecRegistry;
    use serde_json::json;

    fn flow_schema_fixture() -> Value {
        json!({
            "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
            "kind": "FlowSchema",
            "metadata": {"name": "fs-test"},
            "spec": {
                "priorityLevelConfiguration": {"name": "pl-test"},
                "matchingPrecedence": 100,
                "rules": [{
                    "subjects": [{"kind": "User", "user": {"name": "alice"}}],
                    "resourceRules": [{
                        "verbs": ["get"],
                        "apiGroups": [""],
                        "resources": ["pods"],
                        "clusterScope": true
                    }]
                }]
            }
        })
    }

    fn priority_level_fixture() -> Value {
        json!({
            "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
            "kind": "PriorityLevelConfiguration",
            "metadata": {"name": "pl-test"},
            "spec": {
                "type": "Limited",
                "limited": {
                    "nominalConcurrencyShares": 1,
                    "limitResponse": {"type": "Reject"}
                }
            }
        })
    }

    #[test]
    fn registry_handles_flowcontrol_v1_kinds() {
        let registry = OoCodecRegistry::new(vec![Box::new(FlowcontrolV1Codec)]);
        for kind in [
            "FlowSchema",
            "PriorityLevelConfiguration",
            "FlowSchemaList",
            "PriorityLevelConfigurationList",
        ] {
            assert!(registry.handles("flowcontrol.apiserver.k8s.io/v1", kind));
        }
        assert!(!registry.handles("v1", "FlowSchema"));
    }

    #[test]
    fn flowcontrol_codec_roundtrips_primary_resources() {
        let codec = FlowcontrolV1Codec;

        let encoded = codec
            .encode_from_json("FlowSchema", &flow_schema_fixture())
            .unwrap();
        let decoded = codec.decode_to_json("FlowSchema", &encoded).unwrap();
        assert_eq!(decoded["kind"], "FlowSchema");
        assert_eq!(decoded["metadata"]["name"], "fs-test");
        assert_eq!(
            decoded["spec"]["priorityLevelConfiguration"]["name"],
            "pl-test"
        );

        let encoded = codec
            .encode_from_json("PriorityLevelConfiguration", &priority_level_fixture())
            .unwrap();
        let decoded = codec
            .decode_to_json("PriorityLevelConfiguration", &encoded)
            .unwrap();
        assert_eq!(decoded["kind"], "PriorityLevelConfiguration");
        assert_eq!(decoded["metadata"]["name"], "pl-test");
        assert_eq!(decoded["spec"]["type"], "Limited");
    }
}
