//! Typed errors exposed by the public Kubernetes codec boundary.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    MissingApiVersion,
    MissingKind,
    UnsupportedResource { api_version: String, kind: String },
    Encode { message: String },
    Decode { message: String },
    Framing { message: String },
}

impl CodecError {
    pub(crate) fn encode(error: impl std::fmt::Display) -> Self {
        Self::Encode {
            message: error.to_string(),
        }
    }

    pub(crate) fn decode(error: impl std::fmt::Display) -> Self {
        Self::Decode {
            message: error.to_string(),
        }
    }

    pub(crate) fn framing(error: impl std::fmt::Display) -> Self {
        Self::Framing {
            message: error.to_string(),
        }
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingApiVersion => formatter.write_str("Missing apiVersion in JSON"),
            Self::MissingKind => formatter.write_str("Missing kind in JSON"),
            Self::UnsupportedResource { api_version, kind } => {
                write!(
                    formatter,
                    "Unknown kind for protobuf encoding: {api_version}/{kind}"
                )
            }
            Self::Encode { message } | Self::Decode { message } | Self::Framing { message } => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for CodecError {}
