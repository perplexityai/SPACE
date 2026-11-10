//! Node-installed authority, preserving each platform target independently.
//! RPC callers supply identities, never artifact paths or new authorization.
use super::*;
use model::catalog::Catalog;
pub(super) use model::catalog::InstalledPolicy as Policy;

const POLICY: &str = "/etc/space/guest-update/policy.json";
const BUNDLES: &str = "/opt/space/guest-update";

pub(super) struct Bundle {
    pub policy: Policy,
    pub descriptor: model::BundleDescriptor,
    pub service: std::fs::File,
    pub cli: std::fs::File,
}

#[cfg(unix)]
fn trusted_file(path: &Path) -> Result<std::fs::File, Status> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    // Parent paths are fixed by the service, not RPC inputs. Refuse writable
    // parents and symlinks as well as mutable or symlinked leaf payloads.
    for parent in path.ancestors().skip(1) {
        let m = std::fs::symlink_metadata(parent).map_err(internal)?;
        if !m.is_dir() || m.uid() != 0 || m.mode() & 0o022 != 0 {
            return Err(Status::failed_precondition(
                "update artifact parent is not root-owned and immutable",
            ));
        }
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(internal)?;
    let m = file.metadata().map_err(internal)?;
    if !m.is_file() || m.uid() != 0 || m.mode() & 0o022 != 0 {
        return Err(Status::failed_precondition(
            "update artifact is not a root-owned regular file",
        ));
    }
    Ok(file)
}

fn metadata(path: &Path) -> Result<Vec<u8>, Status> {
    use std::io::Read;
    let file = trusted_file(path)?;
    let mut bytes = Vec::new();
    file.take(model::MAX_METADATA_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(internal)?;
    if bytes.len() > model::MAX_METADATA_BYTES {
        return Err(Status::resource_exhausted("promotion metadata too large"));
    }
    Ok(bytes)
}

pub(super) async fn catalog() -> Result<Catalog, Status> {
    tokio::task::spawn_blocking(|| Catalog::parse(&metadata(Path::new(POLICY))?).map_err(internal))
        .await
        .map_err(internal)?
}

pub(super) async fn load(target: model::ArtifactDigest) -> Result<Bundle, Status> {
    let catalog = catalog().await?;
    tokio::task::spawn_blocking(move || {
        let (platform, architecture, policy) = catalog.target(target).ok_or_else(|| {
            Status::permission_denied("target is not installed in the node catalog")
        })?;
        let policy = policy.clone();
        let root = Path::new(BUNDLES).join(policy.promotion.target.to_string());
        let descriptor = model::BundleDescriptor::parse(&metadata(&root.join("artifact.json"))?)
            .map_err(internal)?;
        if descriptor.digest().map_err(internal)? != policy.promotion.target
            || descriptor.version != policy.promotion.target_version
            || descriptor.guest_update_required_since
                != policy.promotion.guest_update_required_since
            || descriptor.version > policy.promotion.maximum_guest
            || semver::Version::parse(env!("CARGO_PKG_VERSION")).map_err(internal)?
                < policy.promotion.minimum_host
            || descriptor.recovery_platform().ok() != Some(platform)
            || descriptor.architecture != architecture
            || descriptor.architecture.to_string() != std::env::consts::ARCH
        {
            return Err(Status::failed_precondition(
                "promotion identity or compatibility ceiling rejected",
            ));
        }
        pplx_space_spaced_common::validate_spaced_guest_compatibility(
            Some(&descriptor.version.to_string()),
            Some(&descriptor.compatibility_family),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let platform = descriptor.recovery_platform().map_err(internal)?;
        let service = trusted_file(&root.join(platform.binary_name(model::Binary::Service)))?;
        let cli = trusted_file(&root.join(platform.binary_name(model::Binary::Cli)))?;
        Ok(Bundle {
            policy,
            descriptor,
            service,
            cli,
        })
    })
    .await
    .map_err(internal)?
}

pub(super) fn promotion(value: wire::Promotion) -> Result<model::policy::Promotion, Status> {
    fn parse_error(error: impl std::fmt::Display) -> Status {
        Status::invalid_argument(error.to_string())
    }
    let value = model::policy::Promotion {
        channel: value.channel,
        target_version: value.target_version.parse().map_err(parse_error)?,
        guest_update_required_since: value
            .guest_update_required_since
            .parse()
            .map_err(parse_error)?,
        intent: match wire::SelectionIntent::try_from(value.intent) {
            Ok(wire::SelectionIntent::Upgrade) => model::policy::SelectionIntent::Upgrade,
            Ok(wire::SelectionIntent::Rollback) => model::policy::SelectionIntent::Rollback,
            _ => {
                return Err(Status::invalid_argument(
                    "release selection intent is required",
                ));
            }
        },
        target: value.target_digest.parse().map_err(parse_error)?,
        minimum_host: value.minimum_host.parse().map_err(parse_error)?,
        maximum_guest: value.maximum_guest.parse().map_err(parse_error)?,
    };
    value
        .accept_after(None)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    Ok(value)
}

pub(super) fn authorizes(policy: &Policy, action: &Action, template: Option<&str>) -> bool {
    template.is_some_and(|template| {
        policy
            .qualified_templates
            .iter()
            .any(|qualified| qualified == template)
    }) && matches!(action,
        Action::Apply { promotion, authorization }
            if promotion.as_ref() == &policy.promotion
                && authorization == &policy.authorization)
}

/// Recheck current authority immediately before an irreversible guest transition.
pub(super) async fn permits(action: &Action, template: Option<&str>) -> bool {
    let Action::Apply { promotion, .. } = action else {
        return false;
    };
    load(promotion.target)
        .await
        .as_ref()
        .is_ok_and(|bundle| authorizes(&bundle.policy, action, template))
}
