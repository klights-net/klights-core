use super::*;
use serde_json::json;

fn log_test_now() -> time::OffsetDateTime {
    time::OffsetDateTime::UNIX_EPOCH
}

#[test]
fn test_parse_cri_log_line_standard_format_no_timestamps() {
    let line = "2024-01-15T10:30:00.123456789Z stdout F Hello world";
    let result = parse_cri_log_line(line, false);
    assert_eq!(result, "Hello world");
}

#[test]
fn test_parse_cri_log_line_standard_format_with_timestamps() {
    let line = "2024-01-15T10:30:00.123456789Z stdout F Hello world";
    let result = parse_cri_log_line(line, true);
    assert_eq!(result, "2024-01-15T10:30:00.123456789Z Hello world");
}

#[test]
fn test_parse_cri_log_line_stderr_stream() {
    let line = "2024-01-15T10:30:00Z stderr F error message";
    let result = parse_cri_log_line(line, false);
    assert_eq!(result, "error message");
}

#[test]
fn test_parse_cri_log_line_partial_tag() {
    let line = "2024-01-15T10:30:00Z stdout P partial message continues";
    let result = parse_cri_log_line(line, false);
    assert_eq!(result, "partial message continues");
}

#[test]
fn test_parse_cri_log_line_message_with_spaces() {
    let line = "2024-01-15T10:30:00Z stdout F multi word message with spaces";
    let result = parse_cri_log_line(line, false);
    assert_eq!(result, "multi word message with spaces");
}

#[test]
fn test_parse_cri_log_line_malformed_fewer_than_four_parts_returns_as_is() {
    // Fewer than 4 space-separated parts => returned as-is
    let line = "short line";
    let result = parse_cri_log_line(line, false);
    assert_eq!(result, "short line");
}

#[test]
fn test_parse_cri_log_line_empty_string() {
    let result = parse_cri_log_line("", false);
    assert_eq!(result, "");
}

#[test]
fn test_log_query_deserialize_since_seconds() {
    let query: LogQuery = serde_json::from_value(json!({
        "sinceSeconds": 300
    }))
    .unwrap();
    assert_eq!(query.since_seconds, Some(300));
    assert_eq!(query.container, None);
    assert_eq!(query.tail_lines, None);
}

#[test]
fn test_log_query_deserialize_previous() {
    let query: LogQuery = serde_json::from_value(json!({
        "previous": "true"
    }))
    .unwrap();
    assert_eq!(query.previous, Some("true".to_string()));
}

#[test]
fn test_log_query_deserialize_all_params() {
    let query: LogQuery = serde_json::from_value(json!({
        "container": "web",
        "follow": "true",
        "tailLines": 100,
        "timestamps": "true",
        "sinceSeconds": 60,
        "previous": "false"
    }))
    .unwrap();
    assert_eq!(query.container, Some("web".to_string()));
    assert_eq!(query.follow, Some("true".to_string()));
    assert_eq!(query.tail_lines, Some(100));
    assert_eq!(query.timestamps, Some("true".to_string()));
    assert_eq!(query.since_seconds, Some(60));
    assert_eq!(query.previous, Some("false".to_string()));
}

#[tokio::test]
async fn test_build_log_output_waits_for_eventual_write() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();

    let writer_path = log_path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        tokio::fs::write(&writer_path, "hello from log\n")
            .await
            .unwrap();
    });

    let task_supervisor =
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default());
    let content = build_log_output(
        &log_path_str,
        &LogQuery {
            container: None,
            follow: None,
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        &task_supervisor,
        log_test_now(),
    )
    .await
    .unwrap();
    assert_eq!(content, "hello from log\n");
}

#[tokio::test]
async fn test_build_log_output_bytes_preserves_non_utf8_cri_payload() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    let mut raw = b"2026-06-13T16:40:46.427204231Z stdout F ".to_vec();
    raw.extend_from_slice(b"status ");
    raw.push(0xf6);
    raw.extend_from_slice(b" payload\n");
    tokio::fs::write(&log_path, raw).await.unwrap();

    let task_supervisor =
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default());
    let content = build_log_output_bytes(
        &log_path_str,
        &LogQuery {
            container: None,
            follow: None,
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        &task_supervisor,
        log_test_now(),
    )
    .await
    .unwrap();
    assert_eq!(content.as_ref(), b"status \xf6 payload\n");
}

#[tokio::test]
async fn test_follow_log_file_with_initial_query_applies_tail_before_following() {
    use futures::StreamExt as _;
    use tokio::io::AsyncWriteExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    tokio::fs::write(
        &log_path,
        concat!(
            "2026-05-08T00:00:00.000000000Z stdout F one\n",
            "2026-05-08T00:00:01.000000000Z stdout F two\n",
            "2026-05-08T00:00:02.000000000Z stdout F three\n",
        ),
    )
    .await
    .unwrap();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: Some(2),
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let initial = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(initial.as_ref(), concat!("two\n", "three\n",).as_bytes());

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "follow stream must wait for new data after the initial tail snapshot"
    );

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .await
        .unwrap();
    file.write_all(b"2026-05-08T00:00:03.000000000Z stdout F four\n")
        .await
        .unwrap();
    file.flush().await.unwrap();

    let appended = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(appended.as_ref(), b"four\n");
}

#[tokio::test]
async fn test_follow_log_file_waits_for_late_log_file_creation() {
    use futures::StreamExt as _;
    use tokio::io::AsyncWriteExt as _;

    let dir = tempfile::tempdir().unwrap();
    let container_dir = dir.path().join("container");
    tokio::fs::create_dir_all(&container_dir).await.unwrap();
    let log_path = container_dir.join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "follow stream must stay open while the container log file is not created yet"
    );

    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&log_path)
        .await
        .unwrap();
    file.write_all(b"2026-05-08T00:00:00.000000000Z stdout F late hello\n")
        .await
        .unwrap();
    file.flush().await.unwrap();

    let appended = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(appended.as_ref(), b"late hello\n");
}

#[tokio::test]
async fn test_follow_log_file_closes_if_pod_deleted_before_log_file_exists() {
    use futures::StreamExt as _;

    let dir = tempfile::tempdir().unwrap();
    let container_dir = dir.path().join("container");
    tokio::fs::create_dir_all(&container_dir).await.unwrap();
    let log_path = container_dir.join("0.log").to_string_lossy().to_string();
    let (tx, rx) = tokio::sync::broadcast::channel(8);

    let stream = follow_log_file_with_termination_watch(
        log_path,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        PodLogFollowTermination::new_for_test(
            rx,
            "default".to_string(),
            "late-delete".to_string(),
            "late-delete-uid".to_string(),
            "main".to_string(),
            false,
        ),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let event = klights_leader_api::ResourceEvent::try_new(
        klights_leader_api::WatchEventType::Deleted,
        klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "late-delete",
            "uid": "late-delete-uid"
        },
        "spec": {"containers": [{"name": "main"}]},
        "status": {"phase": "Pending"}
        })))
        .unwrap(),
        None,
    )
    .unwrap();
    tx.send(event).unwrap();

    let item = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("stream must close when the pod is deleted before log file creation");
    assert!(
        item.is_none(),
        "deleted pod without a log file must close the follow stream"
    );
}

#[tokio::test]
async fn test_follow_log_file_strips_cri_prefix_from_initial_and_live_lines() {
    use futures::StreamExt as _;
    use tokio::io::AsyncWriteExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    let initial = b"2026-05-08T00:00:00.000000000Z stdout F initial\n";
    tokio::fs::write(&log_path, initial).await.unwrap();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.as_ref(), b"initial\n");

    let appended = b"2026-05-08T00:00:01.000000000Z stdout F live\n";
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .await
        .unwrap();
    file.write_all(appended).await.unwrap();
    file.flush().await.unwrap();

    let next = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.as_ref(), b"live\n");
}

#[tokio::test]
async fn test_follow_log_file_without_pod_watch_exits_after_close_write() {
    use futures::StreamExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    tokio::fs::write(&log_path, b"").await.unwrap();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "empty follow stream must wait for the first live log write"
    );

    tokio::fs::write(
        &log_path,
        b"2026-05-08T00:00:01.000000000Z stdout F terminal\n",
    )
    .await
    .unwrap();

    let live = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.as_ref(), b"terminal\n");

    let done = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("log follow should close after the writer closes the log file");
    assert!(
        done.is_none(),
        "log follow without a pod watch must close on the terminal log-file close event"
    );
}

#[tokio::test]
async fn test_follow_log_file_exits_after_matching_pod_deleted_event() {
    use futures::StreamExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    tokio::fs::write(
        &log_path,
        b"2026-05-08T00:00:00.000000000Z stdout F finished\n",
    )
    .await
    .unwrap();

    let task_supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let (pod_event_tx, watch_rx) = tokio::sync::broadcast::channel(8);
    let stream = follow_log_file_with_termination_watch(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: None,
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        task_supervisor,
        PodLogFollowTermination::new_for_test(
            watch_rx,
            "default".to_string(),
            "done".to_string(),
            "uid-1".to_string(),
            "main".to_string(),
            false,
        ),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.as_ref(), b"finished\n");

    let event = klights_leader_api::ResourceEvent::try_new(
        klights_leader_api::WatchEventType::Deleted,
        klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "done",
            "uid": "uid-1"
        },
        "status": {
            "phase": "Succeeded"
        }
        })))
        .unwrap(),
        None,
    )
    .unwrap();
    pod_event_tx.send(event).unwrap();

    let done = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("terminated pod log follow should close promptly");
    assert!(
        done.is_none(),
        "terminated pod log follow must close instead of waiting for more writes"
    );
}

#[tokio::test]
async fn test_follow_log_file_since_time_then_follows_new_inotify_writes() {
    use futures::StreamExt as _;
    use tokio::io::AsyncWriteExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    tokio::fs::write(
        &log_path,
        concat!(
            "2026-05-08T00:00:00.000000000Z stdout F old\n",
            "2026-05-08T00:00:10.000000000Z stdout F kept\n",
        ),
    )
    .await
    .unwrap();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: Some("2026-05-08T00:00:05Z".to_string()),
            limit_bytes: None,
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let initial = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(initial.as_ref(), b"kept\n");

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .await
        .unwrap();
    file.write_all(b"2026-05-08T00:00:11.000000000Z stdout F live\n")
        .await
        .unwrap();
    file.flush().await.unwrap();

    let next = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.as_ref(), b"live\n");
}

#[tokio::test]
async fn test_follow_log_file_since_time_respects_limit_bytes() {
    use futures::StreamExt as _;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("0.log");
    let log_path_str = log_path.to_string_lossy().to_string();
    tokio::fs::write(
        &log_path,
        concat!(
            "2026-05-08T00:00:00.000000000Z stdout F old\n",
            "2026-05-08T00:00:10.000000000Z stdout F abcdef\n",
        ),
    )
    .await
    .unwrap();

    let stream = follow_log_file_with_initial_query(
        log_path_str,
        LogQuery {
            container: None,
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_seconds: None,
            since_time: Some("2026-05-08T00:00:05Z".to_string()),
            limit_bytes: Some(43),
            previous: None,
            insecure_skip_tls_verify_backend: false,
        },
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        log_test_now(),
    );
    futures::pin_mut!(stream);

    let initial = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(initial.as_ref(), b"abc");
}

#[test]
fn test_since_seconds_cutoff_uses_explicit_operation_time() {
    let params = LogQuery {
        container: None,
        follow: None,
        tail_lines: None,
        timestamps: None,
        since_seconds: Some(90),
        since_time: None,
        limit_bytes: None,
        previous: None,
        insecure_skip_tls_verify_backend: false,
    };
    let now =
        time::OffsetDateTime::from_unix_timestamp(1_785_240_000).expect("fixed operation time");

    assert_eq!(
        log_query_since_cutoff_at(&params, now)
            .expect("sinceSeconds cutoff")
            .to_rfc3339(),
        "2026-07-28T11:58:30+00:00"
    );
}

#[test]
fn test_is_log_line_after_cutoff_no_cutoff_includes_all() {
    let line = "2024-01-15T10:30:00Z stdout F message";
    assert!(is_log_line_after_cutoff(line, None));
}

#[test]
fn test_is_log_line_after_cutoff_recent_line_included() {
    // Line from 1 second ago should be included with 60-second cutoff
    let now = chrono::Utc::now();
    let recent = now - chrono::Duration::seconds(1);
    let cutoff = now - chrono::Duration::seconds(60);
    let line = format!(
        "{} stdout F recent message",
        recent.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    );
    assert!(is_log_line_after_cutoff(&line, Some(&cutoff)));
}

#[test]
fn test_is_log_line_after_cutoff_old_line_excluded() {
    // Line from 2 hours ago should be excluded with 60-second cutoff
    let now = chrono::Utc::now();
    let old = now - chrono::Duration::seconds(7200);
    let cutoff = now - chrono::Duration::seconds(60);
    let line = format!(
        "{} stdout F old message",
        old.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    );
    assert!(!is_log_line_after_cutoff(&line, Some(&cutoff)));
}

#[test]
fn test_is_log_line_after_cutoff_exact_cutoff_included() {
    // Line exactly at cutoff time should be included (>=)
    let cutoff = chrono::DateTime::parse_from_rfc3339("2024-06-15T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let line = "2024-06-15T12:00:00Z stdout F exact boundary";
    assert!(is_log_line_after_cutoff(line, Some(&cutoff)));
}

#[test]
fn test_is_log_line_after_cutoff_malformed_line_included() {
    let cutoff = chrono::Utc::now();
    // No space => can't extract timestamp => include
    assert!(is_log_line_after_cutoff("nospaces", Some(&cutoff)));
}

#[test]
fn test_is_log_line_after_cutoff_unparseable_timestamp_included() {
    let cutoff = chrono::Utc::now();
    let line = "not-a-timestamp stdout F message";
    assert!(is_log_line_after_cutoff(line, Some(&cutoff)));
}

#[test]
fn test_filter_logs_by_since_seconds() {
    let now = chrono::Utc::now();
    let recent = now - chrono::Duration::seconds(10);
    let old = now - chrono::Duration::seconds(3600);
    let cutoff = now - chrono::Duration::seconds(60);

    let lines = [
        format!(
            "{} stdout F old line",
            old.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        ),
        format!(
            "{} stdout F recent line",
            recent.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        ),
    ];

    let filtered: Vec<String> = lines
        .iter()
        .filter(|line| is_log_line_after_cutoff(line, Some(&cutoff)))
        .map(|line| parse_cri_log_line(line, false))
        .collect();

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], "recent line");
}

#[test]
fn test_filter_logs_by_since_time_rfc3339() {
    let now = chrono::Utc::now();
    let recent = now - chrono::Duration::seconds(10);
    let old = now - chrono::Duration::seconds(3600);
    // Use sinceTime = 60 seconds ago
    let cutoff = now - chrono::Duration::seconds(60);

    let recent_ts = recent.format("%Y-%m-%dT%H:%M:%S.%9fZ").to_string();
    let old_ts = old.format("%Y-%m-%dT%H:%M:%S.%9fZ").to_string();

    let lines = [
        format!("{old_ts} stdout F old message"),
        format!("{recent_ts} stdout F new message"),
    ];

    let filtered: Vec<String> = lines
        .iter()
        .filter(|l| is_log_line_after_cutoff(l, Some(&cutoff)))
        .map(|l| parse_cri_log_line(l, false))
        .collect();

    assert_eq!(filtered, vec!["new message".to_string()]);
}
