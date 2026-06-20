use super::events::WatchEvent;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchEventFilter {
    field_selectors: Vec<TargetFieldSelector>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TargetFieldSelector {
    api_version: String,
    kind: String,
    field_selector: String,
}

impl WatchEventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_field_selector(
        mut self,
        api_version: impl Into<String>,
        kind: impl Into<String>,
        field_selector: impl Into<String>,
    ) -> Self {
        self.field_selectors.push(TargetFieldSelector {
            api_version: api_version.into(),
            kind: kind.into(),
            field_selector: field_selector.into(),
        });
        self
    }

    pub fn matches(&self, event: &WatchEvent) -> bool {
        if self.field_selectors.is_empty() {
            return true;
        }
        let Some(kind) = event.object.get("kind").and_then(|kind| kind.as_str()) else {
            return true;
        };
        let api_version = event
            .object
            .get("apiVersion")
            .and_then(|api_version| api_version.as_str());

        for selector in &self.field_selectors {
            if selector.kind != kind {
                continue;
            }
            if api_version.is_some_and(|actual| actual != selector.api_version) {
                continue;
            }
            if !event.matches_field_selector(Some(selector.field_selector.as_str())) {
                return false;
            }
        }
        true
    }
}
