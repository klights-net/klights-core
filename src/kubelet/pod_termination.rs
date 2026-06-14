use serde_json::Value;

pub fn parse_cri_log_message(line: &str) -> String {
    line.splitn(4, ' ').nth(3).unwrap_or(line).to_string()
}

pub fn utf8_tail(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut start = value.len() - max_bytes;
    while start < value.len() && !value.is_char_boundary(start) {
        start += 1;
    }
    value[start..].to_string()
}

pub fn find_pod_container_spec<'a>(pod: &'a Value, container_name: &str) -> Option<&'a Value> {
    pod.pointer("/spec/containers")
        .and_then(|v| v.as_array())
        .into_iter()
        .chain(
            pod.pointer("/spec/initContainers")
                .and_then(|v| v.as_array()),
        )
        .flatten()
        .find(|container| container.get("name").and_then(|v| v.as_str()) == Some(container_name))
}

pub fn termination_message_policy(container_spec: Option<&Value>) -> &str {
    container_spec
        .and_then(|spec| spec.get("terminationMessagePolicy"))
        .and_then(|v| v.as_str())
        .filter(|policy| !policy.is_empty())
        .unwrap_or("File")
}

/// Returns the host-side file path for a container's termination message log.
/// Pattern mirrors the /etc/hosts host path under KLIGHTS_DATA_ROOT.
pub fn termination_log_host_path(
    containerd_ns: &str,
    namespace: &str,
    pod_name: &str,
    container_name: &str,
) -> String {
    crate::paths::containerd_termination_log_path(
        containerd_ns,
        namespace,
        pod_name,
        container_name,
    )
    .to_string_lossy()
    .into_owned()
}

pub fn container_log_host_path(
    containerd_ns: &str,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    container_name: &str,
) -> String {
    crate::paths::pod_log_dir_path(containerd_ns, namespace, pod_name, pod_uid)
        .join(container_name)
        .join("0.log")
        .to_string_lossy()
        .into_owned()
}

/// Returns the container-side termination message path from the container spec.
/// Defaults to /dev/termination-log per K8s spec.
/// Also treats empty string as absent — protobuf proto3 default for string fields is "",
/// which would cause containerd to mount at path "" and fail.
pub fn get_termination_message_path(container_spec: &Value) -> &str {
    let path = container_spec
        .get("terminationMessagePath")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if path.is_empty() {
        "/dev/termination-log"
    } else {
        path
    }
}

/// Reads the termination message from the host-side file.
/// Returns empty string if file doesn't exist or can't be read.
/// Truncates to 4096 bytes (K8s limit).
#[cfg(test)]
pub fn read_termination_message(host_path: &str) -> String {
    match std::fs::read(host_path) {
        Ok(bytes) => {
            let truncated = if bytes.len() > 4096 {
                &bytes[..4096]
            } else {
                &bytes
            };
            String::from_utf8_lossy(truncated).into_owned()
        }
        Err(_) => String::new(),
    }
}

#[cfg(test)]
pub fn read_termination_message_from_logs(log_path: &str) -> String {
    const MAX_LOG_BYTES: usize = 2048;
    const MAX_LOG_LINES: usize = 80;

    let content = match crate::utils::read_utf8_file(log_path) {
        Ok(content) => content,
        Err(_) => return String::new(),
    };

    let mut lines: Vec<String> = content.lines().map(parse_cri_log_message).collect();
    if lines.len() > MAX_LOG_LINES {
        lines = lines.split_off(lines.len() - MAX_LOG_LINES);
    }

    utf8_tail(&lines.join("\n"), MAX_LOG_BYTES)
}

pub async fn read_termination_message_async(host_path: &str) -> String {
    let host_path_owned = host_path.to_string();
    match crate::kubelet::file_blocking::run_blocking_file_keyed(
        "pod_termination_read_message",
        host_path_owned.clone(),
        move || std::fs::read(&host_path_owned).map_err(anyhow::Error::from),
    )
    .await
    {
        Ok(bytes) => {
            let truncated = if bytes.len() > 4096 {
                &bytes[..4096]
            } else {
                &bytes
            };
            String::from_utf8_lossy(truncated).into_owned()
        }
        Err(_) => String::new(),
    }
}

async fn read_termination_message_from_logs_async(log_path: &str) -> String {
    const MAX_LOG_BYTES: usize = 2048;
    const MAX_LOG_LINES: usize = 80;

    let log_path_owned = log_path.to_string();
    let content = match crate::kubelet::file_blocking::run_blocking_file_keyed(
        "pod_termination_read_logs",
        log_path_owned.clone(),
        move || std::fs::read_to_string(&log_path_owned).map_err(anyhow::Error::from),
    )
    .await
    {
        Ok(content) => content,
        Err(_) => return String::new(),
    };

    let mut lines: Vec<String> = content.lines().map(parse_cri_log_message).collect();
    if lines.len() > MAX_LOG_LINES {
        lines = lines.split_off(lines.len() - MAX_LOG_LINES);
    }

    utf8_tail(&lines.join("\n"), MAX_LOG_BYTES)
}

pub async fn read_termination_message_with_fallback_async(
    termination_path: &str,
    log_path: &str,
    policy: &str,
    exit_code: i32,
) -> String {
    let message = read_termination_message_async(termination_path).await;
    if !message.is_empty() {
        return message;
    }

    if policy != "FallbackToLogsOnError" || exit_code == 0 {
        return String::new();
    }

    read_termination_message_from_logs_async(log_path).await
}

#[cfg(test)]
pub fn read_termination_message_with_fallback(
    termination_path: &str,
    log_path: &str,
    policy: &str,
    exit_code: i32,
) -> String {
    let message = read_termination_message(termination_path);
    if !message.is_empty() {
        return message;
    }

    if policy != "FallbackToLogsOnError" || exit_code == 0 {
        return String::new();
    }

    read_termination_message_from_logs(log_path)
}

pub async fn ensure_termination_log_host_file(
    containerd_ns: &str,
    namespace: &str,
    pod_name: &str,
    container_name: &str,
) -> String {
    let path = termination_log_host_path(containerd_ns, namespace, pod_name, container_name);
    crate::kubelet::pod_fs::PodFs::ensure_termination_log(std::path::PathBuf::from(&path)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_termination_log_host_path_returns_expected_path() {
        let path = termination_log_host_path("klights", "default", "mypod", "mycontainer");
        assert_eq!(
            path,
            crate::paths::containerd_termination_log_path(
                "klights",
                "default",
                "mypod",
                "mycontainer",
            )
            .to_string_lossy()
            .into_owned()
        );
    }

    #[test]
    fn test_read_termination_message_returns_empty_when_file_absent() {
        let msg = read_termination_message("/nonexistent/path/that/does/not/exist");
        assert_eq!(msg, "");
    }

    #[test]
    fn test_read_termination_message_truncates_to_4096_bytes() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-log");
        let big_content = "x".repeat(5000);
        std::fs::File::create(&path)
            .unwrap()
            .write_all(big_content.as_bytes())
            .unwrap();
        let msg = read_termination_message(path.to_str().unwrap());
        assert_eq!(msg.len(), 4096, "Must truncate to 4096 bytes");
    }

    #[test]
    fn test_read_termination_message_returns_content_under_4096() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-log");
        let content = "container exited with error\n";
        std::fs::File::create(&path)
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
        let msg = read_termination_message(path.to_str().unwrap());
        assert_eq!(msg, content);
    }

    #[test]
    fn test_termination_message_file_wins_over_fallback_logs() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let term_path = dir.path().join("termination-log");
        let log_path = dir.path().join("0.log");

        std::fs::File::create(&term_path)
            .unwrap()
            .write_all(b"from file")
            .unwrap();
        std::fs::File::create(&log_path)
            .unwrap()
            .write_all(b"2026-04-25T00:00:00.000000000Z stdout F from logs\n")
            .unwrap();

        let msg = read_termination_message_with_fallback(
            term_path.to_str().unwrap(),
            log_path.to_str().unwrap(),
            "FallbackToLogsOnError",
            1,
        );
        assert_eq!(msg, "from file");
    }

    #[test]
    fn test_termination_message_falls_back_to_parsed_logs_on_error() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let term_path = dir.path().join("missing-termination-log");
        let log_path = dir.path().join("0.log");

        std::fs::File::create(&log_path)
            .unwrap()
            .write_all(
                b"2026-04-25T00:00:00.000000000Z stdout F starting\n\
                  2026-04-25T00:00:01.000000000Z stdout F DONE\n",
            )
            .unwrap();

        let msg = read_termination_message_with_fallback(
            term_path.to_str().unwrap(),
            log_path.to_str().unwrap(),
            "FallbackToLogsOnError",
            1,
        );
        assert_eq!(msg, "starting\nDONE");
    }

    #[test]
    fn test_termination_message_fallback_requires_policy_and_nonzero_exit() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let term_path = dir.path().join("missing-termination-log");
        let log_path = dir.path().join("0.log");

        std::fs::File::create(&log_path)
            .unwrap()
            .write_all(b"2026-04-25T00:00:00.000000000Z stdout F DONE\n")
            .unwrap();

        assert_eq!(
            read_termination_message_with_fallback(
                term_path.to_str().unwrap(),
                log_path.to_str().unwrap(),
                "File",
                1,
            ),
            ""
        );
        assert_eq!(
            read_termination_message_with_fallback(
                term_path.to_str().unwrap(),
                log_path.to_str().unwrap(),
                "FallbackToLogsOnError",
                0,
            ),
            ""
        );
    }

    #[test]
    fn test_termination_message_path_default_when_absent() {
        let container = serde_json::json!({"name": "app", "image": "nginx"});
        let path = get_termination_message_path(&container);
        assert_eq!(path, "/dev/termination-log");
    }

    #[test]
    fn test_termination_message_path_custom_when_specified() {
        let container = serde_json::json!({"name": "app", "image": "nginx", "terminationMessagePath": "/tmp/my-termination-log"});
        let path = get_termination_message_path(&container);
        assert_eq!(path, "/tmp/my-termination-log");
    }

    #[test]
    fn test_termination_message_path_default_when_empty_string() {
        // When terminationMessagePath is "" (e.g. from protobuf proto3 default or empty JSON),
        // must fall back to /dev/termination-log — empty string causes containerd mount failure.
        let container =
            serde_json::json!({"name": "app", "image": "nginx", "terminationMessagePath": ""});
        let path = get_termination_message_path(&container);
        assert_eq!(path, "/dev/termination-log");
    }
}
