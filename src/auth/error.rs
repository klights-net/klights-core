//! Framework-neutral authentication and authorization failures.

/// A policy-layer auth failure.
///
/// HTTP status selection and Kubernetes `metav1.Status` serialization belong
/// to the API adapter; auth code only describes the failure category and
/// stable user-facing message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthError {
    InvalidRequest(String),
    Unauthenticated(String),
    Forbidden(String),
}

impl AuthError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest(message.into())
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::Unauthenticated(message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidRequest(message)
            | Self::Unauthenticated(message)
            | Self::Forbidden(message) => message,
        }
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::AuthError;

    #[test]
    fn categories_retain_their_stable_message() {
        let cases = [
            AuthError::invalid_request("invalid"),
            AuthError::unauthenticated("unauthenticated"),
            AuthError::forbidden("forbidden"),
        ];

        for error in cases {
            assert_eq!(error.to_string(), error.message());
        }
    }
}
