//! Typed `manifest.json` and `metadata.json` documents that travel with
//! every template build: the manifest lists the build's files and digests,
//! and the metadata describes how it was built and how it runs.

mod manifest;
mod metadata;

pub use manifest::{
    BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION, GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION,
    TEMPLATE_MANIFEST_SCHEMA_VERSION, TemplateManifest, TemplateManifestCompression,
    TemplateManifestFile, TemplateManifestFilenameError, TemplateManifestSchemaError, ZSTD_CODEC,
    ZSTD_FILENAME_SUFFIX,
};
pub use metadata::{
    BOUND_TEMPLATE_METADATA_SCHEMA_VERSION, GUEST_TEMPLATE_METADATA_SCHEMA_VERSION,
    TEMPLATE_METADATA_SCHEMA_VERSION, TemplateMetadata, TemplateMetadataSchemaError,
};
