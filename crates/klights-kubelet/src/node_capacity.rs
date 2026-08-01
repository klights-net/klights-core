#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapacity {
    memory_ki: u64,
    cpu_cores: u64,
}

impl NodeCapacity {
    pub fn new(memory_ki: u64, cpu_cores: u64) -> Self {
        Self {
            memory_ki,
            cpu_cores: cpu_cores.max(1),
        }
    }

    pub fn memory_ki(self) -> u64 {
        self.memory_ki
    }

    pub fn cpu_cores(self) -> u64 {
        self.cpu_cores
    }
}

impl Default for NodeCapacity {
    fn default() -> Self {
        Self::new(8 * 1024 * 1024, 1)
    }
}
