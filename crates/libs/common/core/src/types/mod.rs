//! Shared SPACE domain types.
//!
//! These newtypes turn repeated primitive values into named domain concepts:
//! sandbox IDs are not template names, memory sizes are not disk sizes, and
//! guest paths are not node paths. Boundary code should parse raw strings,
//! integers, and UUIDs into these types once and pass the typed values through
//! the rest of the call graph.
//!
//! The module stays dependency-light. It intentionally does not depend on
//! transport, database, web framework, or daemon-specific crates.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let sandbox_id = SandboxId::new();
//! let template = TemplateName::parse("seed").unwrap();
//! let memory = MemorySize::gib(4).unwrap();
//! let compute = ComputeShape::new(CpuLimit::vcpu(2).unwrap(), memory);
//!
//! assert_eq!(template.as_str(), "seed");
//! assert_eq!(compute.memory().as_mib_floor(), 4096);
//! ```

pub mod admin;
pub mod auth;
pub mod auto_lifecycle;
pub mod balloon;
pub mod cluster;
pub mod compute;
pub mod data;
pub use pplx_space_db_identity as database;
pub mod disk_io;
pub mod env;
pub mod environment;
pub use pplx_space_execution_spec::execution;
pub mod federated_identity;
pub use pplx_space_execution_spec::host_cpu;
pub mod host_linux;
pub mod kek;
pub mod label;
pub mod materialization;
pub mod network;
pub mod node;
pub mod node_fs;
pub mod node_group;
pub mod object_store;
pub mod path;
#[cfg(feature = "sqlx")]
mod postgres;
pub mod privacy;
pub mod process;
pub mod runtime;
pub mod sandbox;
#[cfg(feature = "serde")]
pub mod sandbox_artifact;
pub use pplx_space_byte_size as size;
pub mod snapshot;
#[cfg(feature = "rusqlite")]
mod sqlite;
pub mod subject;
pub mod template;
#[cfg(feature = "serde")]
pub mod template_runtime;
pub mod time;
pub mod transport_vsock;
pub use pplx_space_semver as version;

#[cfg(feature = "serde")]
pub use admin::*;
pub use auth::*;
pub use auto_lifecycle::*;
pub use balloon::*;
pub use cluster::*;
pub use compute::*;
pub use data::*;
pub use database::*;
pub use disk_io::*;
pub use env::*;
pub use environment::*;
pub use execution::*;
pub use federated_identity::*;
pub use host_cpu::*;
pub use host_linux::*;
pub use kek::*;
pub use label::*;
pub use materialization::*;
pub use network::*;
pub use node::*;
pub use node_fs::*;
pub use node_group::*;
pub use object_store::*;
pub use path::*;
pub use privacy::*;
pub use process::*;
pub use runtime::{
    EffectiveStartup, RUNTIME_CLOUD_HYPERVISOR, RUNTIME_CLOUD_HYPERVISOR_ALIAS,
    RUNTIME_FIRECRACKER, RUNTIME_FIRECRACKER_ALIAS, RUNTIME_GVISOR, RUNTIME_GVISOR_ALIAS,
    RuntimeCapabilities, RuntimeDeploymentId, RuntimeDeploymentIdParseError, RuntimeKind,
    RuntimeKindParseError, StartupPolicy, UnsupportedStartupPolicyError,
};
pub use sandbox::*;
#[cfg(feature = "serde")]
pub use sandbox_artifact::*;
pub use size::*;
pub use snapshot::*;
pub use subject::{SubjectId, SubjectIdParseError, SubjectKind};
pub use template::{
    Arch, ArchParseError, BuildRunId, BuildRunIdParseError, ContentHash,
    DEFAULT_DIGEST_PREFIX_HEX_LEN, DEFAULT_TEMPLATE_TAG, Digest, DigestParseError, DigestPrefix,
    DigestPrefixParseError, DigestPrefixResolveError, HashAlg, HashAlgParseError,
    MIN_DIGEST_PREFIX_HEX_LEN, ORPHANED_SANDBOX_TEMPLATE, OwnerType, ResolvedTemplateReference,
    ResolvedTemplateVariant, RuntimeScopedTemplateReference, SystemTemplate,
    SystemTemplateParseError, TemplateArtifactDigest, TemplateArtifactDigestPrefix,
    TemplateBuildId, TemplateBuildJobState, TemplateBuildJobStateParseError, TemplateBuildState,
    TemplateDesiredState, TemplateDesiredStateParseError, TemplateId, TemplateIndexDigest,
    TemplateIndexDigestPrefix, TemplateName, TemplateParseError, TemplateReference,
    TemplateReferenceParseError, TemplateReferenceResolveError, TemplateSelector, TemplateTag,
    TemplateTagParseError, TemplateVisibility, TemplateVisibilityParseError,
};
#[cfg(feature = "serde")]
pub use template::{
    BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION, BOUND_TEMPLATE_METADATA_SCHEMA_VERSION,
    TEMPLATE_MANIFEST_SCHEMA_VERSION, TEMPLATE_METADATA_SCHEMA_VERSION, TemplateManifest,
    TemplateManifestCompression, TemplateManifestFile, TemplateManifestFilenameError,
    TemplateManifestSchemaError, TemplateMetadata, TemplateMetadataSchemaError, ZSTD_CODEC,
    ZSTD_FILENAME_SUFFIX,
};
#[cfg(feature = "serde")]
pub use template_runtime::*;
pub use time::*;
pub use transport_vsock::*;
pub use version::{SemVer, SemVerParseError};
