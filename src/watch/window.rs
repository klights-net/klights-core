#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowPolicy {
    StopAndWait,
    Sliding(std::num::NonZeroUsize),
}

impl WindowPolicy {
    pub fn default_watch_delivery() -> Self {
        Self::Sliding(std::num::NonZeroUsize::new(3).expect("3 is non-zero"))
    }

    pub fn limit(self) -> std::num::NonZeroUsize {
        match self {
            Self::StopAndWait => std::num::NonZeroUsize::new(1).expect("1 is non-zero"),
            Self::Sliding(limit) => limit,
        }
    }
}
