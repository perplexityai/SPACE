//! Shared SPACE sandbox, template, snapshot and node domain types.
//!
//! These newtypes turn repeated primitive values into named domain concepts:
//! sandbox IDs are not template names, node paths are not storage URIs, and
//! snapshot names are not template references. Value types that have their own
//! `pplx-space-*` crate are imported from that crate, not through `types`.
//! Boundary code should parse raw strings, integers, and UUIDs into these
//! types once and pass the typed values through the rest of the call graph.
//!
//! The module stays dependency-light. It intentionally does not depend on
//! transport, database, web framework, or daemon-specific crates.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! # use pplx_space_sandbox_model::*;
//! # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
//! # use pplx_space_byte_size::MemorySize;
//! # use pplx_space_template_model::TemplateName;
//! let sandbox_id = SandboxId::new();
//! let template = TemplateName::parse("seed").unwrap();
//! let memory = MemorySize::gib(4).unwrap();
//! let compute = ComputeShape::new(CpuLimit::vcpu(2).unwrap(), memory);
//!
//! assert_eq!(template.as_str(), "seed");
//! assert_eq!(compute.memory().as_mib_floor(), 4096);
//! ```

/// Transitional: `SandboxId` lives in `pplx-space-sandbox-model`. This re-export keeps the
/// old `types::SandboxId` path compiling for downstream crates until the common-core
/// deletion removes it.
pub use pplx_space_sandbox_model::SandboxId;
