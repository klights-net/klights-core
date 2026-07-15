//! Transitional compatibility path for canonical Kubernetes quantity parsing.

#[deprecated(note = "use klights_types quantity functions directly; removed in Phase 3.4")]
pub use klights_types::quantity::{
    format_cpu_milli, format_memory_bytes, format_resource_quantity, is_binary_quantity_resource,
    parse_cpu_milli, parse_decimal_si_quantity, parse_memory_bytes, parse_resource_quantity,
};
