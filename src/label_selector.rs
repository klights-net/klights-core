//! Transitional compatibility path for canonical label-selector parsing.

#[deprecated(note = "use klights_types selector types directly; removed in Phase 3.4")]
pub use klights_types::label_selector::{
    LabelRequirement, LabelSelector, LabelSelectorParseError, split_selector,
};

#[deprecated(note = "use klights_types::parse_label_selector; removed in Phase 3.4")]
pub fn parse_label_selector(selector: &str) -> anyhow::Result<Vec<LabelRequirement>> {
    Ok(klights_types::parse_label_selector(selector)?)
}
