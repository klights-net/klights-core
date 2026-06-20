#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchDeliveryScope {
    Cluster,
    Namespaced(String),
    NamespacedAll,
}

impl WatchDeliveryScope {
    pub fn matches_namespace(&self, namespace: Option<&str>) -> bool {
        match self {
            Self::Cluster => namespace.is_none(),
            Self::Namespaced(expected) => namespace == Some(expected.as_str()),
            Self::NamespacedAll => namespace.is_some(),
        }
    }
}
