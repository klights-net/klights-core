use super::WatchEvent;

#[derive(Clone, Copy, Debug)]
pub struct WatchEventSelection<'a> {
    api_version: &'a str,
    kind: &'a str,
    namespace: Option<&'a str>,
    label_selector: Option<&'a str>,
    field_selector: Option<&'a str>,
}

impl<'a> WatchEventSelection<'a> {
    pub fn new(api_version: &'a str, kind: &'a str) -> Self {
        Self {
            api_version,
            kind,
            namespace: None,
            label_selector: None,
            field_selector: None,
        }
    }

    pub fn namespace(mut self, namespace: Option<&'a str>) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn label_selector(mut self, label_selector: Option<&'a str>) -> Self {
        self.label_selector = label_selector;
        self
    }

    pub fn field_selector(mut self, field_selector: Option<&'a str>) -> Self {
        self.field_selector = field_selector;
        self
    }

    pub fn matches(self, event: &WatchEvent) -> bool {
        event.object.get("apiVersion").and_then(|v| v.as_str()) == Some(self.api_version)
            && event.object.get("kind").and_then(|v| v.as_str()) == Some(self.kind)
            && self.namespace.is_none_or(|namespace| {
                event
                    .object
                    .pointer("/metadata/namespace")
                    .and_then(|v| v.as_str())
                    == Some(namespace)
            })
            && event.matches_filter(self.kind, self.namespace, self.label_selector)
            && event.matches_field_selector(self.field_selector)
    }
}
