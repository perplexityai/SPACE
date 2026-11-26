//! Release registration: the release a `RegisterTemplateRelease` attaches
//! to or inserts, its execution-metadata check, and the release its tags go
//! to. Releases are unique per template, stored spelling of the hash and node
//! cache partition; a
//! release bound to another environment than its original is that release's
//! twin, which the schema admits once `idx_template_releases_template_name_hash_unique`
//! is dropped.

use pplx_space_accounts::OrgId;
use tonic::Status;
use uuid::Uuid;

use super::metadata::{TemplateReleaseRow, TemplateReleaseTagRow};

/// A registration's validated execution metadata: the JSONB stored on
/// `template_releases.execution_metadata` and the environment it binds.
pub(super) struct ReleaseExecution {
    pub(super) json: serde_json::Value,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
}

impl ReleaseExecution {
    /// The node cache partition of the binding: nodes cache an unbound build
    /// and one bound to the initial environment in the same legacy partition,
    /// so the registration writer treats a release of either as both.
    fn partition(&self) -> pplx_space_template_model::TemplateCachePartition {
        pplx_space_execution_catalog::catalog::template_cache_partition(self.environment)
    }
}

/// Validates and re-serializes the registration's execution metadata into the
/// JSONB shape stored on `template_releases.execution_metadata` (the same
/// serialization the node publish path produces, so both writers agree).
pub(super) fn validated_execution_metadata(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<Option<ReleaseExecution>, Status> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    let execution = pplx_space_execution_spec::ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let json = serde_json::to_value(&execution)
        .map_err(|error| Status::internal(format!("serialize execution metadata: {error}")))?;
    Ok(Some(ReleaseExecution {
        json,
        environment: execution.environment(),
    }))
}

/// The release a registration of `(template_name, hash)` with `execution`
/// attaches to, inserted when there is none.
///
/// Releases are unique per template, stored spelling of the hash and node
/// cache partition. A hash's
/// original release ([`find_original_release_row`]) is the one tags resolve,
/// whatever environment it's bound to: the first registered under its
/// spelling of the digest, preferring the spelling whose original owns a
/// storage location where both exist. Lookups without an environment consider
/// only originals (each spelling's first-registered release), never twins. A
/// later registration of another partition is its twin. A registration with
/// execution metadata attaches to the release of its own
/// partition ([`ReleaseExecution::partition`]); a legacy-partition one also
/// attaches to a release recorded without evidence, which it fills. One with
/// no execution metadata attaches to the original release
/// ([`find_original_release_row`]). An unbound release and one bound to the
/// initial environment share the legacy partition, so a registration of either
/// attaches to the other and its metadata check refuses the difference: no
/// writer adds an initial-bound row beside an unbound one, which the unique
/// index, keyed on the raw binding, would admit. A registration bound to an
/// environment no release of the hash has inserts that release's twin. While
/// the `(template_name, hash)` unique index still stands the insert does
/// nothing, and the registration falls back to the original release, as
/// before twins: it's refused as different execution metadata, or fills an
/// original recorded without evidence.
pub(super) async fn ensure_template_release(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    runtime: &str,
    hash: &str,
    org_id: Option<OrgId>,
    execution: Option<&ReleaseExecution>,
) -> Result<EnsuredRelease, Status> {
    // The original decides which of the digest's spellings (bare or
    // `sha256:`) the release lives under, as before twins. The partition
    // lookup prefers that stored spelling, and the insert uses it only, so a
    // release under the other spelling is never filled (only one already
    // recorded for the partition is found there), and an insert names the
    // original's exact text: the `(template_name, hash)` index refuses it
    // while it stands, and a twin keeps its original's hash after.
    let original = find_original_release_row(tx, template_name, hash).await?;
    let release_hash = original
        .as_ref()
        .map_or(hash, |original| original.hash.as_str())
        .to_owned();
    if let Some(existing) = find_registration_target(
        tx,
        template_name,
        &release_hash,
        original.clone(),
        execution,
    )
    .await?
    {
        return accept_existing_release(tx, existing, runtime, org_id, execution)
            .await
            .map(EnsuredRelease::existing);
    }
    // A twin has its original's runtime and owner. The owner can differ: the
    // `(template_name, org_id)` foreign key doesn't check a release recorded
    // without an owner, which an ownerless template later given one keeps.
    if let Some(original) = &original {
        check_release_identity(original, runtime, org_id)?;
    }

    if let Some(inserted) = pplx_space_template_release_store::registration::insert_release(
        &mut **tx,
        template_name,
        runtime,
        &release_hash,
        org_id,
        execution.map(|execution| &execution.json),
    )
    .await
    .map_err(sqlx::Error::from)
    .map_err(|error| {
        crate::template_publication::storage_status("insert template release", &error)
    })? {
        return Ok(EnsuredRelease {
            release: TemplateReleaseRow::from(inserted),
            twin_of: original.map(|original| original.id),
        });
    }

    // The insert conflicted: with a concurrent registration of the same
    // partition, or with the `(template_name, hash)` index, which admits no
    // twin. The original release then decides, as it did before twins.
    let original = find_original_release_row(tx, template_name, hash).await?;
    let existing = match find_registration_target(
        tx,
        template_name,
        &release_hash,
        original.clone(),
        execution,
    )
    .await?
    {
        Some(existing) => existing,
        None => original.ok_or_else(|| {
            Status::internal(format!(
                "release '{template_name}@{hash}' conflicted on insert but could not be reloaded",
            ))
        })?,
    };
    accept_existing_release(tx, existing, runtime, org_id, execution)
        .await
        .map(EnsuredRelease::existing)
}

/// The release [`ensure_template_release`] attached to or inserted.
pub(super) struct EnsuredRelease {
    pub(super) release: TemplateReleaseRow,
    /// The original release's id when this registration inserted `release`
    /// as its twin, which only a schema without the `(template_name, hash)`
    /// index admits.
    pub(super) twin_of: Option<Uuid>,
}

impl EnsuredRelease {
    fn existing(release: TemplateReleaseRow) -> Self {
        Self {
            release,
            twin_of: None,
        }
    }
}

/// The existing release a registration attaches to: with execution metadata,
/// the release of its partition stored under `release_hash`, the original's
/// spelling of the digest; without, the original itself.
///
/// A release of the partition with execution metadata under the digest's
/// other spelling counts too, when it's a twin: which spelling's original is
/// preferred changes when the other one gains a storage location, and a retry
/// must still find the twin the first registration inserted (bound to any
/// environment, the initial one included) rather than insert a second one, or
/// fill a release recorded without evidence under the preferred spelling. So
/// a release with execution metadata under the preferred spelling comes
/// first, then a twin under the other spelling, then one without evidence
/// under the preferred spelling, which the registration fills, as before
/// twins. The other spelling's original is never a target, even when it's
/// bound to the registration's environment: attaching would give it this
/// build's storage location, which can make it the preferred original, and a
/// later registration would then move tags such as `latest` onto it. With no
/// target, the registration inserts under the preferred spelling: while the
/// old index stands, that conflicts and falls back to the original, which it
/// fills or refuses, as before twins; once it's gone, it inserts a twin, so a
/// digest can have a release bound to the environment under each spelling,
/// and the one under the other spelling never gains storage through
/// registration. A release under the other spelling is never filled. Where
/// the preferred spelling has both, the recorded one is where filling the
/// other would have led ([`MetadataCheck::BindingTaken`]).
async fn find_registration_target(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    release_hash: &str,
    original: Option<TemplateReleaseRow>,
    execution: Option<&ReleaseExecution>,
) -> Result<Option<TemplateReleaseRow>, Status> {
    let Some(execution) = execution else {
        return Ok(original);
    };
    let partition = execution.partition();
    let other_spelling = match release_hash.strip_prefix("sha256:") {
        Some(bare) => bare.to_owned(),
        None => format!("sha256:{release_hash}"),
    };
    if let Some(found) = find_partition_release_row(
        tx,
        template_name,
        release_hash,
        partition,
        Evidence::Recorded,
    )
    .await?
    {
        return Ok(Some(found));
    }
    let other = find_partition_release_row(
        tx,
        template_name,
        &other_spelling,
        partition,
        Evidence::Recorded,
    )
    .await?;
    // A twin there comes first. The other spelling's original is a release of
    // its own, as before twins, and never a target.
    if let Some(other) = other
        && Some(other.id) != first_registered_release_id(tx, template_name, &other_spelling).await?
    {
        return Ok(Some(other));
    }
    find_partition_release_row(tx, template_name, release_hash, partition, Evidence::Any).await
}

/// The first registered release of `template_name` stored under exactly
/// `hash`: that spelling's original.
async fn first_registered_release_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    hash: &str,
) -> Result<Option<Uuid>, Status> {
    pplx_space_template_release_store::registration::first_registered_release_id(
        &mut **tx,
        template_name,
        hash,
    )
    .await
    .map_err(sqlx::Error::from)
    .map_err(|err| crate::template_publication::storage_status("query template_release", &err))
}

/// Which releases of a partition a lookup may return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Evidence {
    /// A release recorded without execution metadata too, which the
    /// registration fills.
    Any,
    /// Only releases with execution metadata: the partition lookup's first
    /// steps, under the preferred spelling and the other one, and the retry
    /// after filling an evidence-less release collided with one of the same
    /// environment.
    Recorded,
}

/// What checking a registration's execution metadata against a release found.
enum MetadataCheck {
    /// The release has this metadata, now or since this check filled it.
    Accepted,
    /// The release has none, and filling it would duplicate the binding of
    /// another release of the same hash (a twin), which the unique index
    /// refuses once the `(template_name, hash)` index is gone.
    BindingTaken,
}

/// A registration may attach only to a release of its runtime and owner.
fn check_release_identity(
    release: &TemplateReleaseRow,
    runtime: &str,
    org_id: Option<OrgId>,
) -> Result<(), Status> {
    if release.release_type != runtime {
        return Err(Status::already_exists(format!(
            "release '{}@{}' already exists with runtime '{}' instead of '{runtime}'",
            release.template_name, release.hash, release.release_type,
        )));
    }
    if release.org_id != org_id {
        return Err(Status::permission_denied(
            "release ownership does not match template ownership",
        ));
    }
    Ok(())
}

pub(super) async fn accept_existing_release(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    existing: TemplateReleaseRow,
    runtime: &str,
    org_id: Option<OrgId>,
    execution: Option<&ReleaseExecution>,
) -> Result<TemplateReleaseRow, Status> {
    check_release_identity(&existing, runtime, org_id)?;
    let Some(execution) = execution else {
        return Ok(existing);
    };
    match check_template_release_execution_metadata(tx, existing.id, &execution.json).await? {
        // A release filled just now reports the environment it was filled with.
        MetadataCheck::Accepted => Ok(TemplateReleaseRow {
            environment: execution
                .environment
                .map(|environment| environment.to_string()),
            ..existing
        }),
        // Another release already carries this binding: attach to it instead,
        // under the same checks. It has metadata, so it is never filled and
        // the check can't come back here.
        MetadataCheck::BindingTaken => {
            let bound = find_partition_release_row(
                tx,
                &existing.template_name,
                &existing.hash,
                execution.partition(),
                Evidence::Recorded,
            )
            .await?
            .ok_or_else(|| {
                Status::internal(format!(
                    "release '{}@{}' binding collided but its holder could not be loaded",
                    existing.template_name, existing.hash,
                ))
            })?;
            check_release_identity(&bound, runtime, org_id)?;
            match check_template_release_execution_metadata(tx, bound.id, &execution.json).await? {
                MetadataCheck::Accepted => Ok(bound),
                MetadataCheck::BindingTaken => Err(Status::internal(
                    "release execution metadata collided on a release that already has it",
                )),
            }
        }
    }
}

/// The original release for `(template_name, hash)` under either spelling of
/// the hash: the one a lookup with no environment resolves. `hash` may be
/// either spelling: a registration passes the canonical one (`sha256:<hex>`),
/// and [`tag_release`] a release's stored one, which may be bare. Releases
/// registered before the prefix was required are stored bare, and a
/// republication of one of those must attach its tags to that row rather than
/// create a second, artifact-less release beside it.
///
/// The choice is `pplx_space_template_release_original::digest_original`, the
/// rule the API and the warm pool resolve the digest with too: where both
/// spellings exist, the original that owns a recorded storage location wins,
/// then the older. A twin never wins.
async fn find_original_release_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    hash: &str,
) -> Result<Option<TemplateReleaseRow>, Status> {
    let bare_hash = hash.trim_prefix("sha256:");
    let canonical_hash = format!("sha256:{bare_hash}");
    pplx_space_template_release_store::registration::original_release(
        &mut **tx,
        template_name,
        &canonical_hash,
        bare_hash,
    )
    .await
    .map(|release| release.map(TemplateReleaseRow::from))
    .map_err(sqlx::Error::from)
    .map_err(|err| crate::template_publication::storage_status("query template_release", &err))
}

/// The release of `template_name` stored under exactly `hash` in `partition`
/// that a registration attaches to: one bound to the partition's environment
/// (for the legacy partition, unbound or bound to the initial environment),
/// or, for the legacy partition with [`Evidence::Any`], one with no execution
/// metadata, which the registration fills. A release without evidence
/// predates execution metadata and counts as the legacy partition's, so a
/// registration bound to another environment never takes it over: it inserts
/// a twin, or while the `(template_name, hash)` index stands, falls back to
/// the original and fills it, as before twins. `hash` is the original's stored spelling, so a release
/// under the digest's other spelling is never chosen. Releases with a storage
/// location come first, then those with evidence, then by age.
async fn find_partition_release_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    hash: &str,
    partition: pplx_space_template_model::TemplateCachePartition,
    evidence: Evidence,
) -> Result<Option<TemplateReleaseRow>, Status> {
    // The legacy partition matches an unbound release or one bound to the
    // initial environment.
    let selector = pplx_space_template_release_store::PartitionSelector {
        environment: partition
            .environment()
            .unwrap_or_else(pplx_space_execution_catalog::catalog::initial_environment_reference),
        includes_unbound: partition.environment().is_none(),
    };
    pplx_space_template_release_store::registration::partition_release(
        &mut **tx,
        template_name,
        hash,
        selector,
        evidence == Evidence::Any,
    )
    .await
    .map(|release| release.map(TemplateReleaseRow::from))
    .map_err(sqlx::Error::from)
    .map_err(|err| crate::template_publication::storage_status("query template_release", &err))
}

/// Release identity is immutable. A row without evidence may be filled once;
/// an identical retry is read-only, and contradictory evidence is never acknowledged.
///
/// The fill runs in a savepoint: once the `(template_name, hash)` index is
/// gone, another release of the hash can already carry the binding, and the
/// unique index then refuses the fill (23505). That aborts only the savepoint,
/// and the caller attaches to that release instead
/// ([`MetadataCheck::BindingTaken`]).
async fn check_template_release_execution_metadata(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    execution: &serde_json::Value,
) -> Result<MetadataCheck, Status> {
    // Fence the check and backfill against node publication and other writers,
    // including those that do not take the parent-template publication lock.
    let existing = pplx_space_template_release_store::registration::lock_execution_metadata(
        &mut **tx, release_id,
    )
    .await
    .map_err(sqlx::Error::from)
    .map_err(|error| {
        crate::template_publication::storage_status("lock release execution metadata", &error)
    })?;
    match existing {
        Some(existing) if &existing != execution => Err(Status::already_exists(
            "template release already has different execution metadata",
        )),
        Some(_) => Ok(MetadataCheck::Accepted),
        None => fill_release_execution_metadata(tx, release_id, execution).await,
    }
}

async fn fill_release_execution_metadata(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    execution: &serde_json::Value,
) -> Result<MetadataCheck, Status> {
    let storage = |error: &sqlx::Error| {
        crate::template_publication::storage_status("fill release execution metadata", error)
    };
    let mut savepoint = sqlx::Acquire::begin(&mut **tx)
        .await
        .map_err(|error| storage(&error))?;
    match pplx_space_template_release_store::registration::fill_execution_metadata(
        &mut *savepoint,
        release_id,
        execution,
    )
    .await
    .map_err(sqlx::Error::from)
    {
        Ok(_) => {
            savepoint.commit().await.map_err(|error| storage(&error))?;
            Ok(MetadataCheck::Accepted)
        }
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
            savepoint
                .rollback()
                .await
                .map_err(|error| storage(&error))?;
            Ok(MetadataCheck::BindingTaken)
        }
        Err(error) => Err(storage(&error)),
    }
}

/// The release a registration's tags attach to: the original release of
/// `release`'s template and hash (the first registered), which is `release`
/// itself unless it's a twin. The original is resolved under both spellings
/// of the digest, as the registration chose it, so a release under the
/// spelling that owns no storage location never writes a tag the other
/// spelling's original holds. A tag names a template and hash; a lookup by tag
/// with no environment resolves the original, and one with an environment
/// reaches the twin through the hash.
pub(super) async fn tag_release(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release: &TemplateReleaseRow,
) -> Result<TemplateReleaseRow, Status> {
    find_original_release_row(tx, &release.template_name, &release.hash)
        .await?
        .ok_or_else(|| {
            Status::internal(format!(
                "release '{}@{}' has no original release",
                release.template_name, release.hash,
            ))
        })
}

/// The tags a twin's registration names, which must all already name its
/// hash (on the original release): a twin's registration confirms tags but
/// never writes one. Moving a tag off another hash would let a campaign pull
/// `latest` back to an older release, and creating or touching one would give
/// the original a newer tag move, which warm-pool retention ranks releases
/// by. This applies only once the `(template_name, hash)` index is gone:
/// before that, a registration bound to another environment is refused, or
/// fills an original recorded without evidence and, attaching to it, writes
/// its tags as any registration of a release does. After it, the same request
/// inserts a twin and is refused as a whole when one of its tags doesn't
/// already name the hash.
///
/// The confirmed tag rows stay share-locked until the registration commits,
/// so a tag deleted meanwhile (the admin tag delete takes no template lock)
/// waits for it, and a delete that commits first leaves the tag missing here
/// and the registration refused, rather than returning a deleted tag.
pub(super) async fn twin_tags(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    original: &TemplateReleaseRow,
    tags: &[String],
) -> Result<Vec<TemplateReleaseTagRow>, Status> {
    let bare_hash = original
        .hash
        .strip_prefix("sha256:")
        .unwrap_or(&original.hash);
    let rows = pplx_space_template_store::release_tags::share_lock_tags_of_digest(
        tx,
        &original.template_name,
        tags,
        &original.hash,
        bare_hash,
    )
    .await
    .map_err(|err| crate::template_publication::storage_status("check twin tags", &err))?;
    match tags
        .iter()
        .find(|tag| !rows.iter().any(|row| &row.tag == *tag))
    {
        Some(tag) => Err(Status::failed_precondition(format!(
            "tag '{tag}' doesn't name this release; a registration that attaches to a release \
             other than its hash's original confirms tags already on the hash and writes none"
        ))),
        None => Ok(rows
            .into_iter()
            .map(|row| TemplateReleaseTagRow {
                id: row.id,
                tag: row.tag,
                created_at: row.created_at,
            })
            .collect()),
    }
}
