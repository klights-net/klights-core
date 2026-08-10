use crate::pod_lifecycle_actor::message::LifecycleMessage;

#[test]
fn pod_watcher_limits_pod_events_to_local_node_field_selector() {
    assert_eq!(
        super::super::pod_watcher_node_field_selector("worker-a"),
        "spec.nodeName=worker-a"
    );
}

#[tokio::test]
async fn lifecycle_message_from_command_uses_command_uid_not_live_pod_uid() {
    let message = super::super::lifecycle_message_from_command(
        crate::lifecycle::LifecycleCommand::ReadinessChanged {
            pod_uid: "uid-old".to_string(),
            namespace: "default".to_string(),
            pod_name: "same-name".to_string(),
            container_name: "app".to_string(),
            ready: true,
        },
    )
    .await
    .expect("command should route without a live name lookup");

    let LifecycleMessage::LifecycleCommand { key, .. } = message else {
        panic!("expected lifecycle command message");
    };
    assert_eq!(key.uid, "uid-old");
}
