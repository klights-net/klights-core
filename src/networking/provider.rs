//! `CniAddRequest` — argument type shared by [`Datapath::cni_add`] and
//! the test mocks. The umbrella `NetworkProvider` trait that used to
//! live here was deleted in Task 7 of the network refactor; callers
//! now depend on the narrow `Datapath` / `PeerRouter` traits in
//! `src/networking/{datapath,peer_router}.rs` and the parent
//! `Network` struct that holds them on AppState.

#[derive(Debug, Clone)]
pub struct CniAddRequest {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub netns_setns_path: String,
    pub netns_record_path: String,
    pub host_network: bool,
}
