//! Persistence-facing cluster topology values.

use std::fmt;
use std::net::IpAddr;

use base64::Engine;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataplaneMetadataError(String);

impl DataplaneMetadataError {
    fn invalid(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for DataplaneMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DataplaneMetadataError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DataplaneEncryption {
    #[default]
    Enabled,
    Disabled,
}

impl DataplaneEncryption {
    pub fn parse(raw: Option<&str>) -> Result<Self, DataplaneMetadataError> {
        match raw.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("enabled") => Ok(Self::Enabled),
            Some("disabled") => Ok(Self::Disabled),
            Some(other) => Err(DataplaneMetadataError::invalid(format!(
                "invalid dataplane encryption mode '{other}', expected enabled or disabled"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataplaneMode {
    Root,
    Rootless,
}

impl DataplaneMode {
    pub fn parse(raw: &str) -> Result<Self, DataplaneMetadataError> {
        match raw.trim() {
            "root" => Ok(Self::Root),
            "rootless" => Ok(Self::Rootless),
            other => Err(DataplaneMetadataError::invalid(format!(
                "invalid dataplane mode '{other}', expected root or rootless"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Rootless => "rootless",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireGuardPublicKey(String);

impl WireGuardPublicKey {
    pub fn parse(raw: &str) -> Result<Self, DataplaneMetadataError> {
        let trimmed = raw.trim();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(trimmed)
            .map_err(|_| DataplaneMetadataError::invalid("WireGuard public key must be base64"))?;
        if bytes.len() != 32 {
            return Err(DataplaneMetadataError::invalid(format!(
                "WireGuard public key must decode to 32 bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WireGuardPublicKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataplanePeerMetadata {
    pub node_name: String,
    pub mode: DataplaneMode,
    pub encryption: DataplaneEncryption,
    pub public_key: Option<WireGuardPublicKey>,
    pub endpoint: IpAddr,
    pub port: Option<u16>,
}

impl DataplanePeerMetadata {
    pub fn try_new(
        node_name: String,
        mode: DataplaneMode,
        encryption: DataplaneEncryption,
        public_key: Option<String>,
        endpoint: Option<String>,
        port: Option<u16>,
    ) -> Result<Self, DataplaneMetadataError> {
        if node_name.trim().is_empty() {
            return Err(DataplaneMetadataError::invalid(
                "dataplane peer node_name is required",
            ));
        }
        let endpoint = endpoint
            .as_deref()
            .ok_or_else(|| DataplaneMetadataError::invalid("dataplane peer endpoint is required"))?
            .parse::<IpAddr>()
            .map_err(|_| {
                DataplaneMetadataError::invalid("dataplane peer endpoint must be an IP address")
            })?;
        let public_key = match encryption {
            DataplaneEncryption::Enabled => {
                let raw = public_key.as_deref().ok_or_else(|| {
                    DataplaneMetadataError::invalid(
                        "WireGuard public key is required when encryption is enabled",
                    )
                })?;
                let port = port.ok_or_else(|| {
                    DataplaneMetadataError::invalid(
                        "WireGuard listen port is required when encryption is enabled",
                    )
                })?;
                if port == 0 {
                    return Err(DataplaneMetadataError::invalid(
                        "WireGuard listen port must be non-zero",
                    ));
                }
                Some(WireGuardPublicKey::parse(raw)?)
            }
            DataplaneEncryption::Disabled => None,
        };
        Ok(Self {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port: port.filter(|value| *value != 0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_topology_validates_wireguard_shape_without_network_runtime() {
        let key = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let metadata = DataplanePeerMetadata::try_new(
            "cp-2".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some(key.clone()),
            Some("192.0.2.2".to_string()),
            Some(7679),
        )
        .unwrap();
        assert_eq!(metadata.public_key.unwrap().as_str(), key);
        assert!(
            DataplanePeerMetadata::try_new(
                "cp-2".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("invalid".to_string()),
                Some("192.0.2.2".to_string()),
                Some(7679),
            )
            .is_err()
        );
    }
}
