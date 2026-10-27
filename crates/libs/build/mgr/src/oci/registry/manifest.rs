use oci_spec::distribution::Reference as OciReference;
use oci_spec::image::{Arch as OciArch, Descriptor, ImageIndex, ImageManifest, Os};
use serde::Deserialize;

use super::{Arch, Digest, ManifestPayload, ManifestSource, RegistryError, ResolvedImage};

const MAX_INDEX_DEPTH: usize = 4;

#[derive(Deserialize)]
#[serde(untagged)]
enum RegistryDocument {
    Index(Box<ImageIndex>),
    Manifest(Box<ImageManifest>),
}

pub(super) async fn resolve(
    reference: &OciReference,
    arch: Arch,
    source: &dyn ManifestSource,
) -> Result<ResolvedImage, RegistryError> {
    let selector = reference
        .digest()
        .or_else(|| reference.tag())
        .expect("parsed OCI references always have a tag or digest");
    let mut payload = source.fetch(reference.repository(), selector).await?;
    validate_requested_digest(reference, &payload)?;
    let root_digest = payload.digest;

    for _ in 0..MAX_INDEX_DEPTH {
        let document: RegistryDocument = serde_json::from_slice(&payload.body)
            .map_err(|source| RegistryError::InvalidManifest { source })?;
        match document {
            RegistryDocument::Manifest(manifest) => {
                reject_alternate_urls(manifest.config())?;
                if manifest.config().size() > 4 * 1024 * 1024 {
                    return Err(RegistryError::ImageConfigTooLarge);
                }
                for layer in manifest.layers() {
                    reject_alternate_urls(layer)?;
                }
                let config_digest = manifest
                    .config()
                    .digest()
                    .to_string()
                    .parse()
                    .map_err(|source| RegistryError::InvalidManifestDigest { source })?;
                return Ok(ResolvedImage {
                    reference: reference.clone_with_digest(root_digest.to_string()),
                    config_digest,
                });
            }
            RegistryDocument::Index(index) => {
                let selected = selected_platform(index.manifests(), arch)?;
                reject_alternate_urls(selected)?;
                let expected = selected
                    .digest()
                    .to_string()
                    .parse::<Digest>()
                    .map_err(|source| RegistryError::InvalidManifestDigest { source })?;
                payload = source
                    .fetch(reference.repository(), &expected.to_string())
                    .await?;
                if payload.digest != expected {
                    return Err(RegistryError::ManifestDigestMismatch);
                }
            }
        }
    }
    Err(RegistryError::IndexNestingTooDeep)
}

fn validate_requested_digest(
    reference: &OciReference,
    payload: &ManifestPayload,
) -> Result<(), RegistryError> {
    let Some(expected) = reference.digest() else {
        return Ok(());
    };
    let expected = expected
        .parse::<Digest>()
        .map_err(|source| RegistryError::InvalidManifestDigest { source })?;
    if payload.digest != expected {
        return Err(RegistryError::ManifestDigestMismatch);
    }
    Ok(())
}

fn selected_platform(descriptors: &[Descriptor], arch: Arch) -> Result<&Descriptor, RegistryError> {
    let mut matches = descriptors.iter().filter(|descriptor| {
        descriptor.platform().as_ref().is_some_and(|platform| {
            platform.os() == &Os::Linux && platform.architecture() == &oci_arch(arch)
        })
    });
    let selected = matches
        .next()
        .ok_or(RegistryError::MissingOrAmbiguousPlatform)?;
    if matches.next().is_some() {
        return Err(RegistryError::MissingOrAmbiguousPlatform);
    }
    Ok(selected)
}

fn reject_alternate_urls(descriptor: &Descriptor) -> Result<(), RegistryError> {
    if descriptor
        .urls()
        .as_ref()
        .is_some_and(|urls| !urls.is_empty())
    {
        return Err(RegistryError::AlternateDownloadUrls);
    }
    Ok(())
}

const fn oci_arch(arch: Arch) -> OciArch {
    match arch {
        Arch::X86_64 => OciArch::Amd64,
        Arch::Aarch64 => OciArch::ARM64,
    }
}
