//! Template references, digests, build IDs, and architecture identifiers.
//!
//! SPACE keeps runtime selection separate from template reference strings. The
//! user-facing template grammar is OCI-like:
//! `seed` means `seed:latest`, `seed:v1` selects a mutable tag, and
//! `seed@sha256:...` selects a runtime-specific artifact digest after a
//! [`RuntimeKind`](pplx_space_execution_spec::RuntimeKind) is provided. Short digest prefixes such as
//! `seed@sha256:0123456789ab` are accepted at input boundaries, but they must
//! be resolved against a repository/index before they become exact immutable
//! references. Logical template index digests use the explicit resolved form
//! `seed@index:sha256:...`.
//!
//! # Examples
//!
//! ```rust
//! # use std::str::FromStr;
//! # use pplx_space_common_core::types::*;
//! # use pplx_space_execution_spec::RuntimeKind;
//! let input = TemplateReference::from_str("seed").unwrap();
//! assert_eq!(input.to_string(), "seed:latest");
//!
//! let pinned = TemplateReference::from_str(
//!     "seed@sha256:0000000000000000000000000000000000000000000000000000000000000000",
//! )
//! .unwrap();
//! assert_eq!(
//!     String::from(pinned),
//!     concat!(
//!         "seed@",
//!         "sha256:0000000000000000000000000000000000000000000000000000000000000000",
//!     ),
//! );
//!
//! let runtime = RuntimeKind::parse("fc").unwrap();
//! assert_eq!(runtime, RuntimeKind::Firecracker);
//! assert_eq!(runtime.to_string(), "firecracker");
//! ```
//!

mod build;
mod cache_partition;
mod desired_state;
mod digest;
mod identity;
mod reference;
mod system;
mod visibility;

use pplx_space_content_digest as hash;

pub use build::{
    BuildRunId, BuildRunIdParseError, OwnerType, TemplateBuildId, TemplateBuildJobState,
    TemplateBuildJobStateParseError, TemplateBuildState,
};
pub use cache_partition::TemplateCachePartition;
pub use desired_state::{TemplateDesiredState, TemplateDesiredStateParseError};
pub use digest::{
    TemplateArtifactDigest, TemplateArtifactDigestPrefix, TemplateIndexDigest,
    TemplateIndexDigestPrefix,
};
pub use identity::{
    DEFAULT_TEMPLATE_TAG, ORPHANED_SANDBOX_TEMPLATE, TemplateId, TemplateName, TemplateParseError,
    TemplateTag, TemplateTagParseError,
};
pub use reference::{
    ResolvedTemplateReference, ResolvedTemplateVariant, RuntimeScopedTemplateReference,
    TemplateReference, TemplateReferenceParseError, TemplateReferenceResolveError,
    TemplateSelector,
};
pub use system::{SystemTemplate, SystemTemplateParseError};
pub use visibility::{TemplateVisibility, TemplateVisibilityParseError};
