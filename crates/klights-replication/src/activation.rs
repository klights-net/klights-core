//! Process-local projection of the Raft-committed command codec activation.

use anyhow::{Context, Result};

use crate::materializer::RaftCommitMaterializer;
use klights_cluster_store::{
    COMMAND_CODEC_ACTIVATION_VERSION_META_KEY, COMMAND_CODEC_V3_ACTIVATION_VALUE,
};

/// Process-local mirror of the Raft-committed exact-v3 activation marker.
pub struct CommandCodecV3Activation {
    activated: std::sync::atomic::AtomicBool,
    startup_gate_enforced: std::sync::atomic::AtomicBool,
}

impl CommandCodecV3Activation {
    pub async fn load(materializer: &dyn RaftCommitMaterializer) -> Result<Self> {
        let value = materializer
            .read_raft_metadata(COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)
            .await
            .context("read command codec activation marker")?;
        let activated = match value.as_deref() {
            None => false,
            Some(COMMAND_CODEC_V3_ACTIVATION_VALUE) => true,
            Some(other) => {
                anyhow::bail!(
                    "unsupported persisted command codec activation version {other:?}; required exact version {COMMAND_CODEC_V3_ACTIVATION_VALUE}"
                )
            }
        };
        Ok(Self {
            activated: std::sync::atomic::AtomicBool::new(activated),
            startup_gate_enforced: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn enforce_startup_gate(&self) {
        self.startup_gate_enforced
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn mark_command_codec_v3_activated(&self) {
        self.activated
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn clear_command_codec_v3_activation(&self) {
        self.activated
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub fn is_activated(&self) -> bool {
        self.activated.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn ensure_command_codec_v3_activated(&self) -> Result<()> {
        if !self
            .startup_gate_enforced
            .load(std::sync::atomic::Ordering::Acquire)
            || self.is_activated()
        {
            Ok(())
        } else {
            anyhow::bail!(
                "command proposal capability is unavailable until the Raft-committed exact-v3 codec activation marker applies"
            )
        }
    }
}
