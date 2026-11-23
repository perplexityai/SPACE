//! A release's `metadata.json`, and the twin's document made from it.

use bytes::Bytes;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_catalog::catalog::CompiledEnvironment;
use pplx_space_execution_spec::ExecutionMetadata;
use pplx_space_template_manifest::TemplateMetadata;
use pplx_space_template_model::TemplateReference;

use super::MoveError;

/// A build's `metadata.json`: the typed view this build reads, and the
/// document as written, which a twin's copy keeps field for field.
pub(super) struct TemplateDocument(TemplateMetadata, serde_json::Map<String, serde_json::Value>);

/// A twin's `metadata.json` and the execution metadata its release row
/// records, which the document carries too.
pub(super) struct TwinDocument {
    pub(super) document: Bytes,
    pub(super) execution_json: serde_json::Value,
}

impl TemplateDocument {
    /// Parse a build's `metadata.json`, refusing a document whose schema
    /// version and execution metadata disagree, or whose schema version this
    /// build doesn't know: rewriting one for a twin would carry it forward
    /// as valid-looking metadata.
    pub(super) fn parse(bytes: &[u8]) -> Result<Self, MoveError> {
        // An empty document is no metadata, as a node's pull reads it: the
        // build's execution metadata then comes from its manifest.
        if bytes.is_empty() {
            return Ok(Self(TemplateMetadata::default(), serde_json::Map::new()));
        }
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
            .map_err(|error| MoveError::Failed(format!("metadata.json doesn't parse: {error}")))?;
        let document: TemplateMetadata =
            serde_json::from_value(serde_json::Value::Object(raw.clone())).map_err(|error| {
                MoveError::Failed(format!("metadata.json doesn't parse: {error}"))
            })?;
        document.execution_metadata().map_err(|error| {
            MoveError::Failed(format!("metadata.json is inconsistent: {error}"))
        })?;
        Ok(Self(document, raw))
    }

    /// The base the build was made from, as `(template, hash)`, from
    /// `base_resolved` (`<template>@<digest>`, as build-manager writes it).
    /// `None` for a build with no base, and for a value that isn't a template
    /// name and full digest, which names no release to order after.
    pub(super) fn base(&self) -> Result<Option<(String, String)>, MoveError> {
        let Some(resolved) = self.0.base_resolved.as_deref() else {
            return Ok(None);
        };
        let base = resolved
            .parse::<TemplateReference>()
            .ok()
            .and_then(|reference| {
                let digest = reference.artifact_digest()?;
                Some((reference.name().as_str().to_owned(), digest.to_string()))
            });
        if base.is_none() {
            tracing::warn!(
                base_resolved = resolved,
                "runtime_upgrade: a build's base isn't recorded as <template>@<digest>; its \
                 twin is made without waiting for the base's"
            );
        }
        Ok(base)
    }

    /// The execution metadata the document records, if any.
    pub(super) fn execution(&self) -> Result<Option<ExecutionMetadata>, MoveError> {
        self.0
            .execution_metadata()
            .map(|execution| execution.cloned())
            .map_err(|error| MoveError::Failed(format!("metadata.json is inconsistent: {error}")))
    }

    /// The twin's document: this one, with `target`'s execution metadata,
    /// after checking that a build made under `source` cold boots under
    /// `target` and that `target` runs the build's guest image.
    pub(super) fn twin(
        &self,
        target: &CompiledEnvironment,
        source: &ExecutionMetadata,
    ) -> Result<TwinDocument, MoveError> {
        let definition = target.definition();
        definition.check_cold_boot_source(source).map_err(|error| {
            MoveError::Failed(format!(
                "the build doesn't cold boot under {}: {error}",
                definition.reference()
            ))
        })?;
        if let Some(image) = self.0.guest_image
            && !target.supports_guest_image(image)
        {
            return Err(MoveError::Failed(format!(
                "{} doesn't run the build's guest image",
                definition.reference()
            )));
        }
        let execution = definition.execution_requirements().map_err(|error| {
            MoveError::Failed(format!(
                "{}'s execution requirements: {error}",
                definition.reference()
            ))
        })?;
        // The schema version the binding calls for, as the typed view
        // computes it.
        let mut typed = self.0.clone();
        typed.set_execution_metadata(execution.clone());
        let execution_json = serde_json::to_value(&execution).map_err(|error| {
            MoveError::Failed(format!("serializing the execution metadata: {error}"))
        })?;
        // The original's document with only its execution metadata and
        // schema version replaced, so fields this build doesn't model, or
        // newer builds add, carry over unchanged.
        let mut twin = self.1.clone();
        twin.insert("execution".to_owned(), execution_json.clone());
        twin.insert(
            "schema_version".to_owned(),
            serde_json::to_value(typed.schema_version).map_err(|error| {
                MoveError::Failed(format!("serializing the schema version: {error}"))
            })?,
        );
        let twin = serde_json::Value::Object(twin);
        // The document a reader parses must agree with itself.
        let reread: TemplateMetadata = serde_json::from_value(twin.clone()).map_err(|error| {
            MoveError::Failed(format!("the twin's metadata.json doesn't parse: {error}"))
        })?;
        reread.execution_metadata().map_err(|error| {
            MoveError::Failed(format!("the twin's metadata.json is inconsistent: {error}"))
        })?;
        let document = serde_json::to_vec(&twin)
            .map_err(|error| MoveError::Failed(format!("serializing metadata.json: {error}")))?;
        Ok(TwinDocument {
            document: Bytes::from(document),
            execution_json,
        })
    }
}

/// The execution metadata a build's `manifest.json` records, or `None` for a
/// legacy manifest, one without a `schema_version` or an `execution` block.
/// A manifest the schema refuses fails, as template-manager refuses it: one
/// whose `schema_version` requires an `execution` block it lacks, for
/// instance.
/// Template-manager takes the manifest's ahead of the metadata's
/// (`build_row_manifest`) and refuses a build whose two disagree, so the
/// driver, which reads the metadata's first, reads this only for a build
/// whose metadata records none.
pub(super) fn manifest_execution(manifest: &[u8]) -> Result<Option<ExecutionMetadata>, MoveError> {
    let manifest: pplx_space_template_manifest::TemplateManifest = serde_json::from_slice(manifest)
        .map_err(|error| MoveError::Failed(format!("manifest.json doesn't parse: {error}")))?;
    manifest
        .execution_metadata()
        .map(|execution| execution.cloned())
        .map_err(|error| {
            MoveError::Failed(format!(
                "manifest.json's execution metadata is invalid: {error}"
            ))
        })
}

/// The architecture of a build that records no execution metadata, as
/// template-manager resolves it (`template_arch`): the metadata's `arch`,
/// else the manifest's, else the node's own. A build recording neither boots
/// as the node's architecture, which for the initial environment is x86_64.
/// A value that names no architecture fails.
pub(super) fn legacy_architecture(
    manifest: &[u8],
    document: &TemplateDocument,
) -> Result<Arch, MoveError> {
    let manifest: serde_json::Value = serde_json::from_slice(manifest)
        .map_err(|error| MoveError::Failed(format!("manifest.json doesn't parse: {error}")))?;
    let recorded = document
        .0
        .arch
        .as_deref()
        .filter(|arch| !arch.is_empty())
        .or_else(|| {
            manifest
                .get("arch")
                .and_then(serde_json::Value::as_str)
                .filter(|arch| !arch.is_empty())
        });
    let Some(recorded) = recorded else {
        return Ok(Arch::X86_64);
    };
    recorded.parse().map_err(|_| {
        MoveError::Failed(format!(
            "the build's architecture {recorded:?} isn't one this cluster knows"
        ))
    })
}
