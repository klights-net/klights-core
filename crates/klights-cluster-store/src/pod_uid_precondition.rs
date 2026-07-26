//! Focused read capability for UID-bound Pod command admission.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodUidPreconditionRequest {
    namespace: String,
    name: String,
    expected_uid: String,
}

impl PodUidPreconditionRequest {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        expected_uid: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            expected_uid: expected_uid.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn expected_uid(&self) -> &str {
        &self.expected_uid
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodUidPreconditionState {
    Matches,
    Missing,
    Mismatch { actual_uid: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodUidPreconditionError {
    message: String,
}

impl PodUidPreconditionError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for PodUidPreconditionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PodUidPreconditionError {}

pub type PodUidPreconditionFuture<'a> = Pin<
    Box<dyn Future<Output = Result<PodUidPreconditionState, PodUidPreconditionError>> + Send + 'a>,
>;

pub trait PodUidPreconditionRead: Send + Sync {
    fn read_pod_uid_precondition(
        &self,
        request: PodUidPreconditionRequest,
    ) -> PodUidPreconditionFuture<'_>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_preserves_the_uid_bound_pod_identity() {
        let request = PodUidPreconditionRequest::new("default", "web", "uid-1");
        assert_eq!(request.namespace(), "default");
        assert_eq!(request.name(), "web");
        assert_eq!(request.expected_uid(), "uid-1");
    }
}
