/// Kind of container-lifecycle transition observed by the kubelet.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KubeletEventKind {
    Created,
    Started,
    Stopped,
    Deleted,
}

impl KubeletEventKind {
    /// Short stable label for diagnostics and tracing.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}
