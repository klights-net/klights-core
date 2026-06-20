#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchDeliveryScope {
    All,
    Cluster,
    Namespaced(String),
    NamespacedAll,
}

impl WatchDeliveryScope {
    pub fn matches_namespace(&self, namespace: Option<&str>) -> bool {
        let namespace = namespace.filter(|value| !value.is_empty());
        match self {
            Self::All => true,
            Self::Cluster => namespace.is_none(),
            Self::Namespaced(expected) => namespace == Some(expected.as_str()),
            Self::NamespacedAll => namespace.is_some(),
        }
    }
}
