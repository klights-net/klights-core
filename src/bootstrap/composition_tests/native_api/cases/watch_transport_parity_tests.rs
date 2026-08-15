use std::sync::Arc;

use k8s_native_service::test_protobuf::Message;
use klights_cluster_core::WatchReplayPosition;

#[test]
fn production_positioned_watch_has_json_protobuf_and_grpc_parity() {
    let position = WatchReplayPosition {
        resource_version: 73,
        event_id: 109,
        resource_version_filter_through_event_id: 0,
    };
    let resource = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "positioned",
            "namespace": "default",
            "uid": "uid-positioned",
            "resourceVersion": "73"
        },
        "data": {"key": "value"}
    })))
    .expect("valid ConfigMap");
    let event = klights_leader_api::ResourceEvent::try_new(
        klights_leader_api::WatchEventType::Modified,
        resource,
        Some(position),
    )
    .expect("valid positioned event");

    let json = k8s_native_service::watch::serialize_positioned_watch_event_for_stream_at(
        event.clone(),
        "ConfigMap",
        false,
        k8s_native_service::watch::WatchStreamFormat::Json,
        time::OffsetDateTime::now_utc(),
    )
    .expect("JSON delivery");
    let decoded_json: serde_json::Value = serde_json::from_slice(&json).unwrap();

    let protobuf = k8s_native_service::watch::serialize_positioned_watch_event_for_stream_at(
        event.clone(),
        "ConfigMap",
        false,
        k8s_native_service::watch::WatchStreamFormat::Protobuf,
        time::OffsetDateTime::now_utc(),
    )
    .expect("protobuf delivery");
    let frame_len = u32::from_be_bytes(protobuf[..4].try_into().unwrap()) as usize;
    assert_eq!(frame_len, protobuf.len() - 4);
    let watch =
        k8s_native_service::test_protobuf::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(
            &protobuf[4..],
        )
        .unwrap();
    assert_eq!(watch.r#type.as_deref(), Some("MODIFIED"));
    let raw = watch.object.and_then(|object| object.raw).unwrap();
    let decoded_protobuf = k8s_native_service::test_protobuf::decode_protobuf(&raw).unwrap();

    let grpc = klights_leader_rpc::server::resource_to_proto(event.resource());
    let grpc_object: serde_json::Value = serde_json::from_slice(&grpc.data_json).unwrap();
    assert_eq!(decoded_protobuf, decoded_json["object"]);
    assert_eq!(grpc_object, decoded_json["object"]);
    assert_eq!(grpc.resource_version, 73);
    assert_eq!(event.resume_position(), Some(position));
}
