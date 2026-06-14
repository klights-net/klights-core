use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub cluster_id: String,
    pub voters: Vec<String>,
    pub term: i64,
    pub leader_hint: Option<String>,
}
