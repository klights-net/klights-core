use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::datastore::Resource;

use super::store::PodStore;

#[async_trait]
pub trait StateOnlyWriter: Send + Sync {
    async fn write_status(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;
}

pub struct StatusOnlyWriterService {
    store: Arc<PodStore>,
}

impl StatusOnlyWriterService {
    pub fn new(store: Arc<PodStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl StateOnlyWriter for StatusOnlyWriterService {
    async fn write_status(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.store
            .update_status(ns, name, status, expected_rv)
            .await
    }
}
