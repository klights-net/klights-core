use std::fmt;

/// Fatal node-local datastore open failures.
#[derive(Debug)]
pub enum NodeStoreOpenError {
    SchemaMismatch {
        path: String,
        expected: String,
        actual: String,
        hint: String,
    },
    Corrupt {
        path: String,
        details: String,
    },
}

impl fmt::Display for NodeStoreOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMismatch {
                path,
                expected,
                actual,
                hint,
            } => write!(
                formatter,
                "schema fingerprint mismatch at {path}: expected {expected}, got {actual}\n{hint}"
            ),
            Self::Corrupt { path, details } => {
                write!(
                    formatter,
                    "database corruption detected at {path}: {details}"
                )
            }
        }
    }
}

impl std::error::Error for NodeStoreOpenError {}

impl NodeStoreOpenError {
    pub fn corrupt(path: impl Into<String>, details: impl Into<String>) -> Self {
        Self::Corrupt {
            path: path.into(),
            details: details.into(),
        }
    }

    pub fn path_hint(&self) -> &str {
        match self {
            Self::SchemaMismatch { path, .. } | Self::Corrupt { path, .. } => path,
        }
    }
}
