use anyhow::Result;

use crate::datastore::DatastoreBackend;

pub(crate) async fn stamp_from_store(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_from_store_impl(db, node_name, node, false).await
}

pub(crate) async fn stamp_from_network_metadata(
    store: &dyn crate::datastore::NetworkMetadataStore,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_from_network_metadata_impl(store, node_name, node, false).await
}

#[cfg(test)]
pub(crate) async fn stamp_and_publish_external_ip_from_store(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_from_store_impl(db, node_name, node, true).await
}

async fn stamp_from_store_impl(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
    publish_external_ip: bool,
) -> Result<bool> {
    let mut changed = false;
    if let Some(subnet) = db.get_node_subnet(node_name).await? {
        changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet.to_string());
    }
    if let Some(metadata) = db.get_node_dataplane(node_name).await? {
        if publish_external_ip {
            changed |=
                klights_cluster_core::set_node_external_ip(node, &metadata.endpoint.to_string());
        }
        changed |= klights_types::set_node_dataplane_annotations(
            node,
            &metadata.endpoint.to_string(),
            metadata.mode.as_str(),
            metadata.encryption.as_str(),
            metadata.public_key.as_ref().map(|key| key.as_str()),
            metadata.port,
        );
    }
    Ok(changed)
}

async fn stamp_from_network_metadata_impl(
    db: &dyn crate::datastore::NetworkMetadataStore,
    node_name: &str,
    node: &mut serde_json::Value,
    publish_external_ip: bool,
) -> Result<bool> {
    let mut changed = false;
    if let Some(subnet) = db.get_node_subnet(node_name).await? {
        changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet.to_string());
    }
    if let Some(metadata) = db.get_node_dataplane(node_name).await? {
        if publish_external_ip {
            changed |=
                klights_cluster_core::set_node_external_ip(node, &metadata.endpoint.to_string());
        }
        changed |= klights_types::set_node_dataplane_annotations(
            node,
            &metadata.endpoint.to_string(),
            metadata.mode.as_str(),
            metadata.encryption.as_str(),
            metadata.public_key.as_ref().map(|key| key.as_str()),
            metadata.port,
        );
    }
    Ok(changed)
}
