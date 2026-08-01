#![cfg(test)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicatedCreateOptions {
    pub(crate) resource_version: i64,
    pub(crate) meta_uid: Option<String>,
}

impl ReplicatedCreateOptions {
    pub(crate) fn new(resource_version: i64, meta_uid: Option<String>) -> Self {
        Self {
            resource_version,
            meta_uid,
        }
    }
}
