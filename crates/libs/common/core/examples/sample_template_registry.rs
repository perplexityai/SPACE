//! Minimal in-memory template registry using the common-core template types.
//!
//! This example models tags as cross-runtime aliases to a logical template
//! index. A digest selector is runtime-scoped by definition, so
//! `seed@sha256:...` is matched against the selected runtime's concrete
//! artifact digest.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::types::{
    DEFAULT_TEMPLATE_TAG, RuntimeKind, TemplateArtifactDigest, TemplateArtifactDigestPrefix,
    TemplateIndexDigest, TemplateName, TemplateReference, TemplateReferenceParseError,
    TemplateSelector, TemplateTag,
};
use pplx_space_content_digest::{ContentHash, Digest};

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedTemplateSelection {
    index: TemplateIndexDigest,
    variant: pplx_space_common_core::types::ResolvedTemplateVariant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateIndexRecord {
    variants: BTreeMap<RuntimeKind, TemplateArtifactDigest>,
}

#[derive(Clone, Debug, Default)]
struct SampleTemplateRegistry {
    tags: BTreeMap<(TemplateName, TemplateTag), TemplateIndexDigest>,
    indices: BTreeMap<(TemplateName, TemplateIndexDigest), TemplateIndexRecord>,
}

impl SampleTemplateRegistry {
    fn sample() -> Self {
        let mut registry = Self::default();

        let seed = TemplateName::parse("seed").expect("sample template name is valid");
        let seed_index = index_digest(0x10);
        registry
            .tags
            .insert((seed.clone(), TemplateTag::latest()), seed_index);
        registry.tags.insert(
            (
                seed.clone(),
                TemplateTag::parse("stable").expect("sample tag is valid"),
            ),
            seed_index,
        );
        registry.indices.insert(
            (seed.clone(), seed_index),
            TemplateIndexRecord {
                variants: BTreeMap::from([
                    (RuntimeKind::Firecracker, artifact_digest(0xf1)),
                    (RuntimeKind::CloudHypervisor, artifact_digest(0xc1)),
                    (RuntimeKind::Gvisor, artifact_digest(0x91)),
                ]),
            },
        );

        let code_runtime =
            TemplateName::parse("code-runtime").expect("sample template name is valid");
        let code_runtime_index = index_digest(0x20);
        registry.tags.insert(
            (code_runtime.clone(), TemplateTag::latest()),
            code_runtime_index,
        );
        registry.indices.insert(
            (code_runtime.clone(), code_runtime_index),
            TemplateIndexRecord {
                variants: BTreeMap::from([
                    (RuntimeKind::Firecracker, artifact_digest(0xf2)),
                    (RuntimeKind::Gvisor, artifact_digest(0x92)),
                ]),
            },
        );

        let app_runtime =
            TemplateName::parse("app-runtime").expect("sample template name is valid");
        let app_runtime_index = index_digest(0x30);
        registry.tags.insert(
            (app_runtime.clone(), TemplateTag::latest()),
            app_runtime_index,
        );
        registry.indices.insert(
            (app_runtime, app_runtime_index),
            TemplateIndexRecord {
                variants: BTreeMap::from([(RuntimeKind::Firecracker, artifact_digest(0xf3))]),
            },
        );

        registry
    }

    fn resolve(
        &self,
        runtime: RuntimeKind,
        input: &str,
    ) -> Result<ResolvedTemplateSelection, RegistryError> {
        let reference = TemplateReference::from_str(input)?;
        let name = reference.name().clone();

        match reference.selector() {
            TemplateSelector::Tag(tag) => self.resolve_tag(runtime, name, tag.clone()),
            TemplateSelector::ArtifactDigest(digest) => {
                self.resolve_artifact_digest(runtime, name, ArtifactSelector::Full(*digest))
            }
            TemplateSelector::ArtifactDigestPrefix(prefix) => self.resolve_artifact_digest(
                runtime,
                name,
                ArtifactSelector::Prefix(prefix.clone()),
            ),
        }
    }

    fn resolve_tag(
        &self,
        runtime: RuntimeKind,
        name: TemplateName,
        tag: TemplateTag,
    ) -> Result<ResolvedTemplateSelection, RegistryError> {
        let index = *self.tags.get(&(name.clone(), tag.clone())).ok_or_else(|| {
            RegistryError::UnknownTag {
                name: name.clone(),
                tag,
            }
        })?;

        self.resolve_index(runtime, name, index)
    }

    fn resolve_index(
        &self,
        runtime: RuntimeKind,
        name: TemplateName,
        index: TemplateIndexDigest,
    ) -> Result<ResolvedTemplateSelection, RegistryError> {
        let record = self.indices.get(&(name.clone(), index)).ok_or_else(|| {
            RegistryError::UnknownIndex {
                name: name.clone(),
                index,
            }
        })?;
        let artifact =
            *record
                .variants
                .get(&runtime)
                .ok_or_else(|| RegistryError::MissingRuntimeVariant {
                    runtime,
                    name: name.clone(),
                    index,
                })?;

        Ok(ResolvedTemplateSelection {
            index,
            variant: pplx_space_common_core::types::ResolvedTemplateVariant::new(
                runtime, name, artifact,
            ),
        })
    }

    fn resolve_artifact_digest(
        &self,
        runtime: RuntimeKind,
        name: TemplateName,
        selector: ArtifactSelector,
    ) -> Result<ResolvedTemplateSelection, RegistryError> {
        let mut matched = None;

        for ((candidate_name, index), record) in &self.indices {
            if candidate_name != &name {
                continue;
            }
            let Some(artifact) = record.variants.get(&runtime).copied() else {
                continue;
            };
            if selector.matches(artifact) {
                if matched.is_some() {
                    return Err(RegistryError::AmbiguousArtifactDigest {
                        runtime,
                        name,
                        selector: selector.to_string(),
                    });
                }
                matched = Some((*index, artifact));
            }
        }

        let Some((index, artifact)) = matched else {
            return Err(RegistryError::UnknownArtifactDigest {
                runtime,
                name,
                selector: selector.to_string(),
            });
        };

        Ok(ResolvedTemplateSelection {
            index,
            variant: pplx_space_common_core::types::ResolvedTemplateVariant::new(
                runtime, name, artifact,
            ),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArtifactSelector {
    Full(TemplateArtifactDigest),
    Prefix(TemplateArtifactDigestPrefix),
}

impl ArtifactSelector {
    fn matches(&self, artifact: TemplateArtifactDigest) -> bool {
        match self {
            Self::Full(digest) => *digest == artifact,
            Self::Prefix(prefix) => prefix.matches(artifact),
        }
    }
}

impl fmt::Display for ArtifactSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(digest) => digest.fmt(f),
            Self::Prefix(prefix) => prefix.fmt(f),
        }
    }
}

#[derive(Debug)]
enum RegistryError {
    InvalidInput(TemplateReferenceParseError),
    UnknownTag {
        name: TemplateName,
        tag: TemplateTag,
    },
    UnknownIndex {
        name: TemplateName,
        index: TemplateIndexDigest,
    },
    MissingRuntimeVariant {
        runtime: RuntimeKind,
        name: TemplateName,
        index: TemplateIndexDigest,
    },
    UnknownArtifactDigest {
        runtime: RuntimeKind,
        name: TemplateName,
        selector: String,
    },
    AmbiguousArtifactDigest {
        runtime: RuntimeKind,
        name: TemplateName,
        selector: String,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(err) => write!(f, "invalid template input: {err}"),
            Self::UnknownTag { name, tag } => write!(f, "unknown tag {name}:{tag}"),
            Self::UnknownIndex { name, index } => {
                write!(f, "unknown template index {name}@{index}")
            }
            Self::MissingRuntimeVariant {
                runtime,
                name,
                index,
            } => write!(
                f,
                "template {name}@{index} has no variant for runtime {runtime}"
            ),
            Self::UnknownArtifactDigest {
                runtime,
                name,
                selector,
            } => write!(
                f,
                "unknown artifact digest {name}@{selector} for runtime {runtime}"
            ),
            Self::AmbiguousArtifactDigest {
                runtime,
                name,
                selector,
            } => write!(
                f,
                "artifact digest {name}@{selector} matched multiple {runtime} variants"
            ),
        }
    }
}

impl Error for RegistryError {}

impl From<TemplateReferenceParseError> for RegistryError {
    fn from(value: TemplateReferenceParseError) -> Self {
        Self::InvalidInput(value)
    }
}

fn index_digest(byte: u8) -> TemplateIndexDigest {
    TemplateIndexDigest::new(ContentHash::new(Digest::sha256([byte; 32])))
}

fn artifact_digest(byte: u8) -> TemplateArtifactDigest {
    TemplateArtifactDigest::new(ContentHash::new(Digest::sha256([byte; 32])))
}

fn main() -> Result<(), RegistryError> {
    let registry = SampleTemplateRegistry::sample();

    let seed_fc = registry.resolve(RuntimeKind::Firecracker, "seed")?;
    assert_eq!(seed_fc.index, index_digest(0x10));
    assert_eq!(seed_fc.variant.artifact_digest(), artifact_digest(0xf1));

    let seed_gvisor = registry.resolve(RuntimeKind::Gvisor, "seed:latest")?;
    assert_eq!(seed_gvisor.index, seed_fc.index);
    assert_eq!(seed_gvisor.variant.artifact_digest(), artifact_digest(0x91));

    let seed_ch = registry.resolve(RuntimeKind::CloudHypervisor, "seed:stable")?;
    assert_eq!(seed_ch.index, seed_fc.index);
    assert_eq!(seed_ch.variant.artifact_digest(), artifact_digest(0xc1));

    let fc_digest_input = format!("seed@{}", seed_fc.variant.artifact_digest());
    let seed_fc_by_digest = registry.resolve(RuntimeKind::Firecracker, &fc_digest_input)?;
    assert_eq!(seed_fc_by_digest, seed_fc);
    assert!(matches!(
        registry.resolve(RuntimeKind::Gvisor, &fc_digest_input),
        Err(RegistryError::UnknownArtifactDigest { .. })
    ));

    let fc_short_digest_input =
        format!("seed@{}", seed_fc.variant.artifact_digest().short_string());
    let seed_fc_by_short_digest =
        registry.resolve(RuntimeKind::Firecracker, &fc_short_digest_input)?;
    assert_eq!(seed_fc_by_short_digest, seed_fc);

    let code_runtime_latest = registry.resolve(RuntimeKind::Gvisor, "code-runtime")?;
    assert_eq!(code_runtime_latest.index, index_digest(0x20));
    assert_eq!(
        code_runtime_latest.variant.artifact_digest(),
        artifact_digest(0x92)
    );

    assert!(matches!(
        registry.resolve(RuntimeKind::CloudHypervisor, "code-runtime:latest"),
        Err(RegistryError::MissingRuntimeVariant { .. })
    ));

    println!(
        "default tag '{}' resolved for firecracker and gvisor to one logical index {}",
        DEFAULT_TEMPLATE_TAG, seed_fc.index
    );
    println!("firecracker variant: {}", seed_fc.variant.short_string());
    println!("gvisor variant: {}", seed_gvisor.variant.short_string());
    println!("firecracker artifact digest input resolves only for firecracker: {fc_digest_input}");
    println!("firecracker artifact short digest input also resolves: {fc_short_digest_input}");

    Ok(())
}
