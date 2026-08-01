//! CRI exec/attach retry policy shared by local API and remote node runtime.

#[derive(Debug, Clone, Copy)]
pub struct ExecStreamOptions {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

pub struct ExecRequest<'a> {
    pub container_id: &'a str,
    pub command: &'a [String],
    pub stream_options: ExecStreamOptions,
}

pub struct AttachRequest<'a> {
    pub container_id: &'a str,
    pub stream_options: ExecStreamOptions,
}

fn is_created_state_error<T>(result: &anyhow::Result<T>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|error| error.to_string().contains("CONTAINER_CREATED state"))
}

pub async fn exec_sync_with_created_state_retry(
    cri_client: &mut crate::cri::CriClient,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    container_id: &str,
    command: &[String],
    timeout_seconds: i64,
) -> anyhow::Result<k8s_cri::v1::ExecSyncResponse> {
    use std::time::Duration;

    let first = cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await;
    if !is_created_state_error(&first) {
        return first;
    }
    let _ = task_supervisor
        .sleep(
            "exec_sync_retry_created_state_250ms",
            Duration::from_millis(250),
        )
        .await;
    let second = cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await;
    if !is_created_state_error(&second) {
        return second;
    }
    let _ = task_supervisor
        .sleep(
            "exec_sync_retry_created_state_500ms",
            Duration::from_millis(500),
        )
        .await;
    cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await
}

pub async fn exec_with_created_state_retry(
    cri_client: &mut crate::cri::CriClient,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    request: ExecRequest<'_>,
) -> anyhow::Result<k8s_cri::v1::ExecResponse> {
    use std::time::Duration;

    let ExecRequest {
        container_id,
        command,
        stream_options,
    } = request;
    let first = cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if !is_created_state_error(&first) {
        return first;
    }
    let _ = task_supervisor
        .sleep("exec_retry_created_state_250ms", Duration::from_millis(250))
        .await;
    let second = cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if !is_created_state_error(&second) {
        return second;
    }
    let _ = task_supervisor
        .sleep("exec_retry_created_state_500ms", Duration::from_millis(500))
        .await;
    cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await
}

pub async fn attach_with_created_state_retry(
    cri_client: &mut crate::cri::CriClient,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    request: AttachRequest<'_>,
) -> anyhow::Result<k8s_cri::v1::AttachResponse> {
    use std::time::Duration;

    let AttachRequest {
        container_id,
        stream_options,
    } = request;
    let first = cri_client
        .attach(
            container_id,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if !is_created_state_error(&first) {
        return first;
    }
    let _ = task_supervisor
        .sleep(
            "attach_retry_created_state_250ms",
            Duration::from_millis(250),
        )
        .await;
    let second = cri_client
        .attach(
            container_id,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if !is_created_state_error(&second) {
        return second;
    }
    let _ = task_supervisor
        .sleep(
            "attach_retry_created_state_500ms",
            Duration::from_millis(500),
        )
        .await;
    cri_client
        .attach(
            container_id,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await
}
