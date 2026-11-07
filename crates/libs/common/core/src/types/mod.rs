//! Shared SPACE domain types that have not yet moved to their own crates.
//!
//! These newtypes turn repeated primitive values into named domain concepts:
//! sandbox IDs are not template names, node paths are not storage URIs, and
//! snapshot names are not template references. The module holds the sandbox,
//! template, snapshot, node, node-group and node-filesystem types, object-store
//! keys, KEKs, restore-retirement records, subjects and admin observations, and
//! re-exports the cluster IDs.
//! Account IDs, byte sizes, guest paths and processes, execution and runtime
//! specs, digests, CPU architectures, network roles, Linux and Kubernetes IDs
//! and auto-lifecycle policies live in their own `pplx-space-*` crates; import
//! them from there. Boundary code should parse raw strings, integers, and UUIDs
//! into these types once and pass the typed values through the rest of the
//! call graph.
//!
//! The module stays dependency-light. It intentionally does not depend on
//! transport, database, web framework, or daemon-specific crates.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! # use pplx_space_execution_spec::{ComputeShape, CpuLimit};
//! # use pplx_space_byte_size::MemorySize;
//! let sandbox_id = SandboxId::new();
//! let template = TemplateName::parse("seed").unwrap();
//! let memory = MemorySize::gib(4).unwrap();
//! let compute = ComputeShape::new(CpuLimit::vcpu(2).unwrap(), memory);
//!
//! assert_eq!(template.as_str(), "seed");
//! assert_eq!(compute.memory().as_mib_floor(), 4096);
//! ```

pub mod admin;
pub mod cluster;
pub mod kek;
pub mod node;
pub mod node_fs;
pub mod node_group;
pub mod object_store;
#[cfg(feature = "sqlx")]
mod postgres;
pub mod restore_retirement;
pub mod sandbox;
#[cfg(feature = "serde")]
pub mod sandbox_artifact;
pub mod snapshot;
pub mod subject;
pub mod template;

#[cfg(feature = "serde")]
pub use admin::*;
pub use cluster::*;
pub use kek::*;
pub use node::*;
pub use node_fs::*;
pub use node_group::*;
pub use object_store::*;
pub use restore_retirement::*;
pub use sandbox::*;
#[cfg(feature = "serde")]
pub use sandbox_artifact::*;
pub use snapshot::*;
pub use subject::{SubjectId, SubjectIdParseError, SubjectKind};
pub use template::{
    BuildRunId, BuildRunIdParseError, DEFAULT_TEMPLATE_TAG, ORPHANED_SANDBOX_TEMPLATE, OwnerType,
    ResolvedTemplateReference, ResolvedTemplateVariant, RuntimeScopedTemplateReference,
    SystemTemplate, SystemTemplateParseError, TemplateArtifactDigest, TemplateArtifactDigestPrefix,
    TemplateBuildId, TemplateBuildJobState, TemplateBuildJobStateParseError, TemplateBuildState,
    TemplateCachePartition, TemplateDesiredState, TemplateDesiredStateParseError, TemplateId,
    TemplateIndexDigest, TemplateIndexDigestPrefix, TemplateName, TemplateParseError,
    TemplateReference, TemplateReferenceParseError, TemplateReferenceResolveError,
    TemplateSelector, TemplateTag, TemplateTagParseError, TemplateVisibility,
    TemplateVisibilityParseError,
};
