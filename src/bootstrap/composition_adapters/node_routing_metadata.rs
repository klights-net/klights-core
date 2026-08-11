use anyhow::Result;

use crate::datastore::DatastoreBackend;

pub(crate) async fn stamp_from_store(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_from_store_impl(db, node_name, node).await
}

pub(crate) async fn stamp_from_worker_store(
    store: &klights_kubelet::worker_store::WorkerStoreAdapter,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    let mut changed = false;
    if let Some(subnet) = store.get_node_subnet(node_name).await? {
        changed |= klights_cluster_core::set_node_pod_cidr(node, subnet.subnet());
    }
    if let Some(metadata) = store.get_node_dataplane(node_name).await? {
        let mode = match metadata.mode() {
            klights_leader_api::NetworkNodeMode::Root => "root",
            klights_leader_api::NetworkNodeMode::Rootless => "rootless",
        };
        let encryption = match metadata.encryption() {
            klights_leader_api::DataplaneEncryption::WireGuard => "wireguard",
            klights_leader_api::DataplaneEncryption::Direct => "direct",
        };
        changed |= klights_types::set_node_dataplane_annotations(
            node,
            &metadata.endpoint().to_string(),
            mode,
            encryption,
            metadata.public_key(),
            metadata.port(),
        );
    }
    Ok(changed)
}

async fn stamp_from_store_impl(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    let mut changed = false;
    if let Some(subnet) = db.get_node_subnet(node_name).await? {
        changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet.to_string());
    }
    if let Some(metadata) = db.get_node_dataplane(node_name).await? {
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
