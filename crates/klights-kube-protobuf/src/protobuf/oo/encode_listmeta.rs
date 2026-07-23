/// Convert k8s-openapi ListMeta to k8s-pb ListMeta
pub fn json_listmeta_to_pb(
    meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::ListMeta {
    k8s_pb::apimachinery::pkg::apis::meta::v1::ListMeta {
        resource_version: meta.resource_version.clone(),
        r#continue: meta.continue_.clone(),
        remaining_item_count: meta.remaining_item_count,
        self_link: None, // Deprecated in K8s 1.20+
    }
}
