use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct CrdResourceInfo {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub singular: String,
    pub namespaced: bool,
    pub selectable_fields: Vec<String>,
}

type CrdResourceMap = HashMap<(String, String, String), CrdResourceInfo>;

#[derive(Debug, Clone, Default)]
pub struct CrdRegistry {
    resources: Arc<RwLock<CrdResourceMap>>,
}

impl CrdRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, info: CrdResourceInfo) {
        let key = (
            info.group.clone(),
            info.version.clone(),
            info.plural.clone(),
        );
        self.resources.write().await.insert(key, info);
    }

    pub async fn replace_all(&self, infos: Vec<CrdResourceInfo>) {
        let mut resources = self.resources.write().await;
        resources.clear();
        for info in infos {
            let key = (
                info.group.clone(),
                info.version.clone(),
                info.plural.clone(),
            );
            resources.insert(key, info);
        }
    }

    pub async fn get(&self, group: &str, version: &str, plural: &str) -> Option<CrdResourceInfo> {
        self.resources
            .read()
            .await
            .get(&(group.to_string(), version.to_string(), plural.to_string()))
            .cloned()
    }

    pub async fn list_group_versions(&self) -> Vec<(String, String)> {
        let resources = self.resources.read().await;
        let mut versions: Vec<_> = resources
            .keys()
            .map(|(group, version, _)| (group.clone(), version.clone()))
            .collect();
        versions.sort();
        versions.dedup();
        versions
    }

    pub async fn list_versions_by_group(&self) -> BTreeMap<String, Vec<String>> {
        let resources = self.resources.read().await;
        let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (group, version, _) in resources.keys() {
            grouped
                .entry(group.clone())
                .or_default()
                .insert(version.clone());
        }
        grouped
            .into_iter()
            .map(|(group, versions)| (group, versions.into_iter().collect()))
            .collect()
    }

    pub async fn list_resources(&self, group: &str, version: &str) -> Vec<CrdResourceInfo> {
        self.resources
            .read()
            .await
            .values()
            .filter(|info| info.group == group && info.version == version)
            .cloned()
            .collect()
    }

    pub async fn remove(&self, group: &str, version: &str, plural: &str) {
        self.resources.write().await.remove(&(
            group.to_string(),
            version.to_string(),
            plural.to_string(),
        ));
    }
}

pub fn resource_infos_from_value(crd: &serde_json::Value) -> Result<Vec<CrdResourceInfo>, String> {
    let group = required_string(crd, "/spec/group")?;
    let kind = required_string(crd, "/spec/names/kind")?;
    let plural = required_string(crd, "/spec/names/plural")?;
    let singular = crd
        .pointer("/spec/names/singular")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| plural.to_lowercase());
    let namespaced = required_string(crd, "/spec/scope")? == "Namespaced";
    let versions = crd
        .pointer("/spec/versions")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "CRD spec.versions must be an array".to_string())?;
    let mut infos = Vec::new();
    for version in versions {
        if version.get("served").and_then(serde_json::Value::as_bool) != Some(true) {
            continue;
        }
        let version_name = version
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "served CRD version requires a name".to_string())?;
        let mut selectable_fields = BTreeSet::new();
        if let Some(fields) = version
            .get("selectableFields")
            .and_then(serde_json::Value::as_array)
        {
            for field in fields {
                if let Some(path) = field.get("jsonPath").and_then(serde_json::Value::as_str)
                    && let Some(path) = normalize_selectable_json_path(path)
                {
                    selectable_fields.insert(path);
                }
            }
        }
        infos.push(CrdResourceInfo {
            group: group.clone(),
            version: version_name.to_string(),
            kind: kind.clone(),
            plural: plural.clone(),
            singular: singular.clone(),
            namespaced,
            selectable_fields: selectable_fields.into_iter().collect(),
        });
    }
    Ok(infos)
}

fn required_string(value: &serde_json::Value, pointer: &str) -> Result<String, String> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("CRD field {pointer} must be a non-empty string"))
}

fn normalize_selectable_json_path(path: &str) -> Option<String> {
    let trimmed = path.trim();
    let stripped = trimmed
        .strip_prefix("$.")
        .or_else(|| trimmed.strip_prefix('.'))
        .or_else(|| trimmed.strip_prefix('$'))
        .unwrap_or(trimmed)
        .trim();
    (!stripped.is_empty()).then(|| stripped.to_string())
}
