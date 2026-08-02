//! Root-owned cluster-engine selection and composition dispatch.
//!
//! The embedded graph remains the only implemented engine. Reserved external
//! names fail closed here, before the embedded composition closure can open a
//! store, construct Raft, or bind a listener.

use std::fmt;

const CLUSTER_ENGINE_ENV: &str = "KLIGHTS_CLUSTER_ENGINE";

#[derive(Clone, Copy)]
enum RegisteredAdapter {
    Embedded,
    Reserved,
}

#[derive(Clone, Copy)]
struct EngineRegistration {
    name: &'static str,
    adapter: RegisteredAdapter,
}

const ENGINE_REGISTRY: [EngineRegistration; 2] = [
    EngineRegistration {
        name: "embedded",
        adapter: RegisteredAdapter::Embedded,
    },
    EngineRegistration {
        name: "tikv",
        adapter: RegisteredAdapter::Reserved,
    },
];

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ClusterEngineConfigError {
    UnknownName { name: String },
    NotUnicode,
}

impl fmt::Display for ClusterEngineConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownName { name } => {
                write!(
                    formatter,
                    "unknown {CLUSTER_ENGINE_ENV} value `{name}`; expected one of: "
                )?;
                for (index, registration) in ENGINE_REGISTRY.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    formatter.write_str(registration.name)?;
                }
                Ok(())
            }
            Self::NotUnicode => write!(formatter, "{CLUSTER_ENGINE_ENV} must be valid Unicode"),
        }
    }
}

impl std::error::Error for ClusterEngineConfigError {}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("cluster engine `{engine_name}` is known but its adapter is not implemented")]
pub(crate) struct EngineNotImplemented {
    engine_name: &'static str,
}

impl EngineNotImplemented {
    #[cfg(test)]
    pub(crate) fn engine_name(&self) -> &'static str {
        self.engine_name
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SelectedClusterEngine {
    Embedded,
}

impl SelectedClusterEngine {
    #[cfg(test)]
    pub(crate) const fn is_embedded(self) -> bool {
        matches!(self, Self::Embedded)
    }
}

pub(crate) fn select_from_reader(
    read: impl FnOnce() -> Result<String, std::env::VarError>,
) -> anyhow::Result<SelectedClusterEngine> {
    let requested = match read() {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "embedded".to_string(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(ClusterEngineConfigError::NotUnicode.into());
        }
    };
    let registration = ENGINE_REGISTRY
        .iter()
        .find(|registration| registration.name == requested)
        .ok_or(ClusterEngineConfigError::UnknownName { name: requested })?;

    match registration.adapter {
        RegisteredAdapter::Embedded => Ok(SelectedClusterEngine::Embedded),
        RegisteredAdapter::Reserved => Err(EngineNotImplemented {
            engine_name: registration.name,
        }
        .into()),
    }
}

fn select_from_environment() -> anyhow::Result<SelectedClusterEngine> {
    select_from_reader(|| std::env::var(CLUSTER_ENGINE_ENV))
}

pub(crate) fn run_selected<StartEmbedded, Embedded>(
    start_embedded: StartEmbedded,
) -> anyhow::Result<Embedded>
where
    StartEmbedded: FnOnce() -> Embedded,
{
    match select_from_environment()? {
        SelectedClusterEngine::Embedded => Ok(start_embedded()),
    }
}

#[cfg(test)]
pub(crate) fn known_engine_names() -> Vec<&'static str> {
    ENGINE_REGISTRY
        .iter()
        .map(|registration| registration.name)
        .collect()
}
