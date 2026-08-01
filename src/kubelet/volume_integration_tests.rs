pub use klights_kubelet::volumes::*;

#[path = "volumes/tests_core.rs"]
mod tests_core;
#[path = "volumes/tests_downward.rs"]
mod tests_downward;
#[path = "volumes/tests_projected_a.rs"]
mod tests_projected_a;
#[path = "volumes/tests_projected_b.rs"]
mod tests_projected_b;
#[path = "volumes/tests_refresh_subpath.rs"]
mod tests_refresh_subpath;
