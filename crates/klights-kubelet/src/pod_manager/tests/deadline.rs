#[test]
fn test_parse_deadline_timer_delay_secs_uses_creation_timestamp_when_start_time_missing() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-04-28T00:00:02Z")
        .unwrap()
        .timestamp();
    let pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "ads-pod",
            "creationTimestamp": "2026-04-28T00:00:00Z"
        },
        "spec": {"activeDeadlineSeconds": 5},
        "status": {"phase": "Running"}
    });

    let parsed = super::super::deadline_timers::parse_deadline_timer_delay_secs_at(&pod, now)
        .expect("deadline timer metadata");
    assert_eq!(parsed.0, "default");
    assert_eq!(parsed.1, "ads-pod");
    assert_eq!(parsed.2, 3);
}

#[test]
fn test_parse_deadline_timer_delay_secs_skips_terminal_pods() {
    let pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "done-pod",
            "creationTimestamp": "2026-04-28T00:00:00Z"
        },
        "spec": {"activeDeadlineSeconds": 5},
        "status": {"phase": "Succeeded"}
    });
    assert!(
        super::super::deadline_timers::parse_deadline_timer_delay_secs_at(&pod, 0).is_none(),
        "terminal pods should not schedule deadline timers"
    );
}
