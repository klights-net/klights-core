use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct ServiceRoutingResource {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub data: Arc<Value>,
}

#[derive(Clone, Debug, Default)]
pub struct ServiceRoutingSnapshot {
    pub services: Vec<ServiceRoutingResource>,
    pub endpoints: Vec<ServiceRoutingResource>,
    pub endpoint_slices: Vec<ServiceRoutingResource>,
}

#[derive(Clone, Debug, Default)]
pub struct NetworkPolicySnapshot {
    pub policies: Vec<Arc<Value>>,
    pub pods: Vec<Arc<Value>>,
    pub namespaces: Vec<Arc<Value>>,
}

pub type RoutingStateFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

pub trait RoutingStateSource: Send + Sync {
    fn service_routing_snapshot(&self) -> RoutingStateFuture<'_, ServiceRoutingSnapshot>;
    fn network_policy_snapshot(&self) -> RoutingStateFuture<'_, NetworkPolicySnapshot>;
}
