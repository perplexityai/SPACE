//! The template campaign driver: moving template releases from one execution
//! environment to another by giving each release a *twin*.
//!
//! A twin is a second release of the same template name and hash, bound to the
//! campaign's target in its execution metadata. It shares its original's
//! artifacts and storage locations, has its own `metadata.json` in object
//! storage (the original's document with the target's execution metadata), and
//! gets copies of the original's operator-managed warm pool configs. Creates
//! of the release resolve to the twin where pods boot it, when no release of
//! the build is bound to the default ahead of it
//! (`pplx_space_template_release_store::resolved_release_sql`). A node pulls
//! the twin's document through template-manager, and the plan holds a
//! campaign (`nodes_dont_read_twins`), and a twin's commit releases its item,
//! until every worker of the substrate reports, through node-ctl's
//! `reads_twin_metadata`, that its template-manager does
//! (`pplx_space_runtime_upgrade_campaign_store::workers_not_reading_twins`).
//! node-ctl reports what template-manager's `GetCapabilities` answers.
//!
//! A campaign scans only while its target is the cluster default, so creates
//! move to twins under the campaign's pacing rather than all at once when the
//! default moves. Once the default moves, creates move one twin at a time, as the
//! campaign makes them, under its pacing.
//!
//! A derived template's twin is made after its base's. The base is read from
//! the build's `metadata.json` (`base_resolved`, `<template>@<hash>`). Leasing
//! takes pending items in id order, so an item can't wait for its base's item
//! without livelocking behind it; instead the derived item makes its base's
//! twin first, and the base's own item then finds it in place.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_catalog::catalog::{CompiledEnvironment, template_cache_partition};
use pplx_space_execution_spec::{EnvironmentReference, ExecutionMetadata};
use pplx_space_runtime_upgrade_campaign_store::{
    Campaign, CampaignKind, ItemError, ItemOutcome, LeasedItem,
};
use pplx_space_template_release_store::{
    self as releases, InsertTwin, PartitionSelector, ReleaseObjects, ReleaseSubject,
    TwinMetadataTarget,
};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::{CampaignDriver, Plan, ScanPlan, SubjectPage, SubjectScan, TemplateStorageOrigins};

mod metadata;

#[cfg(test)]
mod tests;

/// Drives `templates` campaigns.
pub struct TemplateDriver {
    pool: PgPool,
    /// For the credential-fenced storage-location sidecar.
    credentials_pool: PgPool,
    objects: Arc<dyn ReleaseObjects>,
    catalog: Catalog,
    /// Whether this cluster's pods boot the environment a twin is bound to:
    /// a Kubernetes substrate with the environment placement gate `on`, the
    /// only mode in which a pod requires its work's environment
    /// (`CreateRequest.required_environment`). Otherwise a create resolved to
    /// a twin still boots its build's legacy partition, so a campaign would
    /// complete without moving anything, and the policy's "templates moved
    /// before a block" check would pass on it.
    boots_twins: bool,
    /// The substrate's `nodes.managed_by`: the workers whose template-manager
    /// must read a twin's metadata before a campaign makes twins.
    managed_by: &'static str,
}

/// The compiled environments a twin can be bound to.
enum Catalog {
    /// The process's compiled catalog, read on first use.
    Compiled,
    #[cfg(test)]
    Fixed(Vec<CompiledEnvironment>),
}

impl Catalog {
    fn environments(&self) -> Result<&[CompiledEnvironment], String> {
        match self {
            // Templates run on x86_64 only: aarch64 has no qualified
            // execution environment.
            Self::Compiled => CompiledEnvironment::cached_for_architecture(Arch::X86_64)
                .map_err(|error| format!("reading the compiled environment catalog: {error}")),
            #[cfg(test)]
            Self::Fixed(environments) => Ok(environments),
        }
    }

    fn find(
        &self,
        reference: EnvironmentReference,
    ) -> Result<Option<&CompiledEnvironment>, String> {
        Ok(self
            .environments()?
            .iter()
            .find(|entry| entry.definition().reference() == reference))
    }
}

impl TemplateDriver {
    /// The driver a cluster runs: the compiled catalog, and S3 signed with
    /// the pod's credentials. Builds no client until a campaign needs one.
    /// `boots_twins` is whether the cluster's pods boot a twin's environment
    /// ([`Self::boots_twins`](TemplateDriver)); without it every campaign is
    /// held. `origins` is which storage locations the driver uses
    /// ([`TemplateStorageOrigins::client`]).
    /// `managed_by` is the substrate's (`SubstrateAdapter::managed_by`).
    pub fn new(
        pool: PgPool,
        credentials_pool: PgPool,
        boots_twins: bool,
        managed_by: &'static str,
        origins: TemplateStorageOrigins,
    ) -> Self {
        Self {
            pool,
            credentials_pool,
            objects: Arc::new(origins.client()),
            catalog: Catalog::Compiled,
            boots_twins,
            managed_by,
        }
    }

    #[cfg(test)]
    fn for_test(
        pool: PgPool,
        objects: Arc<dyn ReleaseObjects>,
        catalog: Vec<CompiledEnvironment>,
    ) -> Self {
        Self {
            credentials_pool: pool.clone(),
            pool,
            objects,
            catalog: Catalog::Fixed(catalog),
            boots_twins: true,
            managed_by: "k8s",
        }
    }
}

/// The releases of `environment`'s node cache partition: those bound to it,
/// and, for the initial environment, which shares the legacy partition, those
/// recorded without one.
fn partition_of(environment: EnvironmentReference) -> PartitionSelector {
    PartitionSelector {
        environment,
        includes_unbound: template_cache_partition(Some(environment))
            == pplx_space_template_model::TemplateCachePartition::Legacy,
    }
}

/// How moving one release ended, before it's recorded.
enum Moved {
    /// The release has a twin bound to the target, or is in its partition.
    Done,
    /// Not the campaign's to move.
    Skipped(String),
}

/// Why moving one release didn't finish.
enum MoveError {
    /// Worth another lease: an outage or a race.
    Retry(String),
    /// No lease will move it.
    Failed(String),
    /// Not attempted: the campaign stopped being movable while the item was
    /// in flight. Back to pending, without spending an attempt; the next
    /// tick's plan holds the campaign.
    Released(String),
}

impl MoveError {
    fn database(what: &str, error: impl std::fmt::Display) -> Self {
        Self::Retry(format!("{what}: {error}"))
    }
}

/// The base references from an item's release to a level of its lineage,
/// as `(template name, hash)`, nearest first.
type BasePath = Vec<(String, String)>;

/// A release whose own checks passed, waiting for its bases' twins.
struct PreparedTwin {
    subject: ReleaseSubject,
    locations: Vec<String>,
    twin: metadata::TwinDocument,
}

/// How preparing one level of a lineage walk ended.
enum Prepared {
    /// Nothing to commit at this level.
    Finished(Moved),
    /// Ready to commit once its base, if any, has its twin.
    Ready(Box<PreparedTwin>, Option<(String, String)>),
}

/// `error` from the level reached through the first `depth` of `bases`, the
/// base references from the item's release to it, named from the item's
/// release outward.
fn through_bases(error: MoveError, bases: &[(String, String)], depth: usize) -> MoveError {
    bases
        .iter()
        .take(depth)
        .rev()
        .fold(error, |error, (name, hash)| match error {
            MoveError::Retry(reason) => {
                MoveError::Retry(format!("the base {name}@{hash}: {reason}"))
            }
            // No later lease moves the base either, so none orders the
            // derived build after it.
            MoveError::Failed(reason) => {
                MoveError::Failed(format!("the base {name}@{hash} can't be moved: {reason}"))
            }
            // The default moved: the derived item is released too.
            released @ MoveError::Released(_) => released,
        })
}

#[async_trait]
impl CampaignDriver for TemplateDriver {
    fn kind(&self) -> CampaignKind {
        CampaignKind::Templates
    }

    async fn plan(&self, campaign: &Campaign) -> Result<Plan, String> {
        if !self.boots_twins {
            return Ok(Plan::Hold("twins_not_booted"));
        }
        let default = pplx_space_runtime_environment_policy_store::get_default(&self.pool)
            .await
            .map_err(|error| format!("reading the default environment: {error}"))?;
        if default != Some(campaign.to) {
            return Ok(Plan::Hold("target_not_default"));
        }
        if self.catalog.find(campaign.to)?.is_none() {
            return Ok(Plan::Hold("target_not_compiled"));
        }
        // A source the catalog doesn't compile can't certify the edge; the
        // per-release cold-boot check still runs on every item.
        if let Some(source) = self.catalog.find(campaign.from)?
            && !source.upgrade_targets().contains(&campaign.to)
        {
            return Ok(Plan::Hold("edge_not_certified"));
        }
        // A twin takes its template's creates once it commits, so every
        // worker such a create can land on must read a twin's own metadata
        // first. Merged isn't deployed: a fleet mid-rollout keeps nodes that
        // don't, and a stale snapshot may hide one.
        let readers = pplx_space_runtime_upgrade_campaign_store::workers_not_reading_twins(
            &self.pool,
            self.managed_by,
        )
        .await
        .map_err(|error| format!("reading which workers read twin metadata: {error}"))?;
        if readers.holds() {
            // The campaign read reports this hold but not which workers hold
            // it, so name them here: a row whose node went away without a
            // drain holds every template campaign until it's deleted.
            tracing::info!(
                campaign_id = %campaign.id,
                workers = readers.workers,
                holding = readers.holding,
                sample = ?readers.sample,
                "runtime_upgrade: a template campaign holds until every worker reports reading \
                 twin metadata"
            );
            return Ok(Plan::Hold("nodes_dont_read_twins"));
        }
        Ok(Plan::Scan(ScanPlan {
            moves: true,
            inputs: fingerprint(campaign.to),
        }))
    }

    async fn find_subjects(
        &self,
        campaign: &Campaign,
        _plan: ScanPlan,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<SubjectScan, String> {
        let on_source =
            releases::source_releases_page(&self.pool, partition_of(campaign.from), after, limit)
                .await
                .map_err(|error| format!("listing the source's releases: {error}"))?;
        Ok(SubjectScan::Page(SubjectPage {
            last: on_source.last().copied(),
            examined: on_source.len(),
            on_source,
            elsewhere: Vec::new(),
        }))
    }

    async fn upgrade(&self, campaign: &Campaign, item: &LeasedItem) -> ItemOutcome {
        match self.move_release(campaign, item.subject_id).await {
            Ok(Moved::Done) => ItemOutcome::Done,
            Ok(Moved::Skipped(reason)) => ItemOutcome::Skipped(ItemError::new(reason)),
            Err(MoveError::Retry(reason)) => ItemOutcome::Retry(ItemError::new(reason)),
            Err(MoveError::Failed(reason)) => ItemOutcome::Failed(ItemError::new(reason)),
            Err(MoveError::Released(reason)) => ItemOutcome::Release(ItemError::new(reason)),
        }
    }
}

/// A fingerprint of what the plan judged subjects by: the target, the
/// compiled definition of which is fixed for the process.
fn fingerprint(to: EnvironmentReference) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    to.to_string().hash(&mut hasher);
    hasher.finish()
}

impl TemplateDriver {
    /// Give `release_id` its twin bound to `campaign.to`, its bases' first.
    ///
    /// The walk is iterative, so a lineage's depth doesn't grow the stack:
    /// it prepares each level from `release_id` toward the root, running that
    /// level's own checks and finding its base, then commits the prepared
    /// twins from the root back to `release_id`. Builds don't cap a lineage's
    /// depth; an acyclic one ends at a build with no base, or at a base
    /// already on the target or no longer the campaign's, and a release seen
    /// twice is a cycle that fails the item. No level holds a connection
    /// while the walk moves on, and twins commit deepest first, so a walk an
    /// item deadline cuts short once it's committing keeps the twins it made
    /// (one cut short while still preparing commits none).
    ///
    /// The bases move inside `release_id`'s item, so one item can commit more
    /// than one twin: a derived template's twin can't come before its base's,
    /// and the lease order can't put the base's item first. A canary's one
    /// item therefore moves its build's lineage up to the first base that
    /// already has a twin, and the bases' own items then end done.
    async fn move_release(
        &self,
        campaign: &Campaign,
        release_id: Uuid,
    ) -> Result<Moved, MoveError> {
        // The prepared levels, `release_id`'s first, and the base references
        // between them: `bases[..k]` led from `release_id` to level `k`, so
        // each reference is kept once. `visited` is the cycle guard; `chain`
        // names the releases for its error.
        let mut levels: Vec<Box<PreparedTwin>> = Vec::new();
        let mut bases: BasePath = Vec::new();
        let mut visited: HashSet<Uuid> = HashSet::new();
        let mut chain: Vec<Uuid> = Vec::new();
        let mut next = Some(release_id);
        while let Some(id) = next.take() {
            let depth = levels.len();
            if !visited.insert(id) {
                return Err(through_bases(
                    MoveError::Failed(format!("release {id} is its own base, through {chain:?}")),
                    &bases,
                    depth,
                ));
            }
            let prepared = match self.prepare_twin(campaign, id).await {
                Ok(prepared) => prepared,
                Err(error) => return Err(through_bases(error, &bases, depth)),
            };
            let (level, base) = match prepared {
                Prepared::Finished(moved) if depth == 0 => return Ok(moved),
                // A base that's done, or isn't the campaign's to move, has
                // nothing to order: its derived builds go ahead.
                Prepared::Finished(_) => break,
                Prepared::Ready(level, base) => (level, base),
            };
            chain.push(id);
            levels.push(level);
            if let Some((base_name, base_hash)) = base {
                // The base's release on the campaign's source: in a later
                // campaign, the twin an earlier one made, not the base's
                // original. A base with no release on the source, such as one
                // never registered here or already moved off it, has nothing
                // to order. One naming its own release is a cycle.
                let found = releases::release_of_hash_in_partition(
                    &self.pool,
                    &base_name,
                    &base_hash,
                    partition_of(campaign.from),
                )
                .await
                .map_err(|error| {
                    through_bases(
                        MoveError::database("reading the base release", error),
                        &bases,
                        depth,
                    )
                })?;
                if let Some(base) = found {
                    bases.push((base_name, base_hash));
                    next = Some(base);
                }
            }
        }
        let mut moved = Moved::Done;
        for (depth, level) in levels.into_iter().enumerate().rev() {
            moved = self
                .commit_twin(campaign, level)
                .await
                .map_err(|error| through_bases(error, &bases, depth))?;
            // A base that went away while the item ran has no twin, so the
            // builds above it mustn't commit theirs: retry, and the next
            // lease resolves the base again (a base gone from the source
            // has nothing to order, so its derived builds then go ahead).
            if depth > 0
                && let Moved::Skipped(reason) = &moved
            {
                return Err(through_bases(
                    MoveError::Retry(format!("the base was skipped: {reason}")),
                    &bases,
                    depth,
                ));
            }
        }
        Ok(moved)
    }

    /// Run `release_id`'s own checks and build its twin's document, up to
    /// the base: a twin can't commit before its base's.
    async fn prepare_twin(
        &self,
        campaign: &Campaign,
        release_id: Uuid,
    ) -> Result<Prepared, MoveError> {
        let Some(subject) = releases::release_subject(&self.pool, release_id)
            .await
            .map_err(|error| MoveError::database("reading the release", error))?
        else {
            return Ok(Prepared::Finished(Moved::Skipped(
                "the release is gone".to_owned(),
            )));
        };
        if !subject.template_present {
            return Ok(Prepared::Finished(Moved::Skipped(
                "the template is being deleted".to_owned(),
            )));
        }
        let target = partition_of(campaign.to);
        if target.contains(subject.environment.as_deref()) {
            return Ok(Prepared::Finished(Moved::Done));
        }
        if releases::release_of_hash_in_partition(
            &self.pool,
            &subject.template_name,
            &subject.hash,
            target,
        )
        .await
        .map_err(|error| MoveError::database("looking for the twin", error))?
        .is_some()
        {
            // A twin this driver inserted got the manual pools of every
            // original of the build, both hash spellings, in its insert's
            // transaction, and an operator's later pool-config
            // write reaches it through the admin API's mirror while
            // resolution picks it. A release another inserter made there
            // (registration, or the Kind bootstrap) before this check keeps
            // its own pools; one made between this check and the insert gets
            // the insert's copy.
            return Ok(Prepared::Finished(Moved::Done));
        }
        if !partition_of(campaign.from).contains(subject.environment.as_deref()) {
            return Ok(Prepared::Finished(Moved::Skipped(
                "the release is no longer bound to the campaign's source".to_owned(),
            )));
        }
        if releases::has_encrypted_storage_locations(&self.credentials_pool, release_id)
            .await
            .map_err(|error| {
                MoveError::database("reading the storage locations' encryption", error)
            })?
        {
            // Still the campaign's to move, so a failure rather than a skip:
            // the campaign mustn't complete as if it had moved the release.
            return Err(MoveError::Failed(
                "the release's storage locations are encrypted under its owner's key, which a \
                 twin can't share yet"
                    .to_owned(),
            ));
        }
        let (locations, served, document) = self.read_metadata(&subject).await?;
        let document = metadata::TemplateDocument::parse(&document)?;

        // This release's own checks come before its base moves: the base's
        // twin takes the base's creates as soon as it commits, so a build that
        // can't run under the target mustn't move its base on the way to
        // failing.
        let source = self
            .source_execution(&subject, &document, &served, &locations)
            .await?;
        let target_environment = self
            .catalog
            .find(campaign.to)
            .map_err(MoveError::Retry)?
            .ok_or_else(|| {
                MoveError::Retry(format!("{} is not a compiled environment", campaign.to))
            })?;
        let twin = document.twin(target_environment, &source)?;
        let base = document.base()?;
        Ok(Prepared::Ready(
            Box::new(PreparedTwin {
                subject,
                locations,
                twin,
            }),
            base,
        ))
    }

    /// Write a prepared twin's `metadata.json`, then insert the twin and copy
    /// its build's originals' manual pools in one transaction.
    async fn commit_twin(
        &self,
        campaign: &Campaign,
        level: Box<PreparedTwin>,
    ) -> Result<Moved, MoveError> {
        let PreparedTwin {
            subject,
            locations,
            twin,
        } = *level;
        let target = partition_of(campaign.to);
        let prepared = match self
            .write_twin_metadata(&subject, &locations, campaign.to, &twin.document)
            .await
        {
            Ok(prepared) => prepared,
            // A write that fails after the default moved is the move's
            // release, not a spent attempt, as the commit's recheck would
            // have said had the write gone through.
            Err(error) => return Err(self.released_if_default_moved(campaign, error).await),
        };
        // The twin wins resolution as soon as it commits, so its manual pools
        // commit with it.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| MoveError::database("inserting the twin", error))?;
        // The plan saw the target as the default when the item was leased,
        // but the default can move while it's in flight. Recheck it under
        // the share lock every default write conflicts with, so no twin
        // commits for a target that isn't the default at commit time. Taken
        // first, before the template row: nothing that holds a template row
        // waits on the policy row.
        let default = pplx_space_runtime_environment_policy_store::share_lock_default(&mut tx)
            .await
            .map_err(|error| MoveError::database("reading the cluster default", error))?;
        if default != Some(campaign.to) {
            return Err(MoveError::Released(format!(
                "the cluster default moved off {} while the item was in flight",
                campaign.to
            )));
        }
        // The plan's worker check, again at commit: a worker that stopped
        // reporting twin support while the item was in flight (a rollback),
        // or one registered meanwhile without it, releases the item, and the
        // plan then holds. That narrows the window rather than closing it: it
        // sees only what committed before this statement, and takes no lock
        // on the nodes or their status.
        let readers = pplx_space_runtime_upgrade_campaign_store::workers_not_reading_twins(
            &mut *tx,
            self.managed_by,
        )
        .await
        .map_err(|error| MoveError::database("reading which workers read twin metadata", error))?;
        if readers.holds() {
            return Err(MoveError::Released(format!(
                "{} of {} worker(s) don't report reading twin metadata at the twin's commit, \
                 or no worker is registered",
                readers.holding, readers.workers
            )));
        }
        let inserted = match releases::insert_twin(
            &mut tx,
            subject.id,
            target,
            &twin.execution_json,
            &prepared,
        )
        .await
        .map_err(|error| MoveError::database("inserting the twin", error))?
        {
            InsertTwin::Inserted(twin) | InsertTwin::Existing(twin) => twin,
            InsertTwin::OriginalGone => {
                return Ok(Moved::Skipped(
                    "the release or its template went away".to_owned(),
                ));
            }
            InsertTwin::Refused => {
                return Err(MoveError::Failed(
                    "the (template_name, hash) unique index still stands, so a release can't \
                     have a twin; apply the migration that drops it"
                        .to_owned(),
                ));
            }
            InsertTwin::SlotTaken(occupant) => {
                return Err(MoveError::Failed(format!(
                    "release {occupant} has no storage locations and already holds this build's \
                     slot in {}'s partition, so a twin can't be inserted there; remove it first",
                    campaign.to
                )));
            }
        };
        copy_pools(&mut tx, campaign, &subject, inserted).await?;
        tx.commit()
            .await
            .map_err(|error| MoveError::database("inserting the twin", error))?;
        Ok(Moved::Done)
    }

    /// `error`, or the item's release if the cluster default has moved off
    /// the campaign's target: unlike the commit's recheck this takes no lock,
    /// as nothing commits after it.
    async fn released_if_default_moved(&self, campaign: &Campaign, error: MoveError) -> MoveError {
        match pplx_space_runtime_environment_policy_store::get_default(&self.pool).await {
            Ok(default) if default != Some(campaign.to) => MoveError::Released(format!(
                "the cluster default moved off {} while the item was in flight",
                campaign.to
            )),
            _ => error,
        }
    }

    /// The execution metadata the release records, which the target's
    /// cold-boot check compares against: the release's own (for a twin, its
    /// environment's requirements, which share the build's runtime, guest
    /// and boot components with the original's), else the document's, else
    /// the build's `manifest.json`'s, as template-manager reads it, else, for
    /// a build recorded without any, the initial environment's, whose
    /// partition it's cached in.
    async fn source_execution(
        &self,
        subject: &ReleaseSubject,
        document: &metadata::TemplateDocument,
        served: &TwinMetadataTarget,
        locations: &[String],
    ) -> Result<ExecutionMetadata, MoveError> {
        let recorded = self
            .recorded_execution(subject, document, served, locations)
            .await?;
        self.cold_boot_source(recorded)
    }

    /// `recorded` as the target's cold-boot check reads it, as template-manager
    /// reads a twin's source (`cold_boot_source_requirements`): unbound
    /// metadata the catalog reads as the initial environment's, as a build
    /// recorded before boot components were (its runtime and guest, no
    /// components), stands for that environment's full requirements. Compared
    /// as is, its missing components would refuse every twin of it.
    fn cold_boot_source(
        &self,
        recorded: ExecutionMetadata,
    ) -> Result<ExecutionMetadata, MoveError> {
        if recorded.environment().is_some() {
            return Ok(recorded);
        }
        let catalog = self.catalog.environments().map_err(MoveError::Retry)?;
        match CompiledEnvironment::of_execution(catalog, &recorded) {
            Some(entry) if entry.is_initial() => entry
                .definition()
                .execution_requirements()
                .map_err(|error| {
                    MoveError::Failed(format!("the initial environment's requirements: {error}"))
                }),
            _ => Ok(recorded),
        }
    }

    /// The execution metadata the release records: its own, else the
    /// document's, else its manifest's, else, for a build recording none, the
    /// initial environment's.
    async fn recorded_execution(
        &self,
        subject: &ReleaseSubject,
        document: &metadata::TemplateDocument,
        served: &TwinMetadataTarget,
        locations: &[String],
    ) -> Result<ExecutionMetadata, MoveError> {
        if let Some(recorded) = &subject.execution_metadata {
            return serde_json::from_value(recorded.clone()).map_err(|error| {
                MoveError::Failed(format!(
                    "the release's execution metadata is invalid: {error}"
                ))
            });
        }
        if let Some(execution) = document.execution()? {
            return Ok(execution);
        }
        // Nothing records how the build was made. Nodes cache it in the
        // legacy partition and boot it under the initial environment, which
        // is x86_64; only an x86_64 image boots there. Resolve its
        // architecture as template-manager does before assuming the initial
        // environment's provenance.
        let manifest = self.read_manifest(subject, served, locations).await?;
        if let Some(execution) = metadata::manifest_execution(&manifest)? {
            return Ok(execution);
        }
        let architecture = metadata::legacy_architecture(&manifest, document)?;
        if architecture != Arch::X86_64 {
            return Err(MoveError::Failed(format!(
                "the build records no execution metadata and is a {architecture} image, which \
                 the initial environment doesn't run"
            )));
        }
        let initial = pplx_space_execution_catalog::catalog::initial_environment_reference();
        let initial = self
            .catalog
            .find(initial)
            .map_err(MoveError::Retry)?
            .ok_or_else(|| MoveError::Retry("the initial environment isn't compiled".to_owned()))?;
        initial
            .definition()
            .execution_requirements()
            .map_err(|error| {
                MoveError::Failed(format!("the initial environment's requirements: {error}"))
            })
    }

    /// The release's storage locations, oldest first, and the build's shared
    /// `metadata.json` from the first of them that serves it, as a pull tries
    /// them, with that location.
    async fn read_metadata(
        &self,
        subject: &ReleaseSubject,
    ) -> Result<(Vec<String>, TwinMetadataTarget, Vec<u8>), MoveError> {
        let locations = releases::storage_locations(&self.pool, subject.id)
            .await
            .map_err(|error| MoveError::database("reading the storage locations", error))?;
        if locations.is_empty() {
            return Err(MoveError::Failed(
                "the release has no storage location".to_owned(),
            ));
        }
        let mut errors = Vec::new();
        for location in &locations {
            let target =
                match TwinMetadataTarget::parse(location, &subject.template_name, &subject.hash) {
                    Ok(target) => target,
                    Err(error) => {
                        errors.push(error.to_string());
                        continue;
                    }
                };
            match self.objects.read_metadata(&target).await {
                Ok(document) => return Ok((locations, target, document)),
                Err(error) => errors.push(format!("{}: {error}", target.bucket())),
            }
        }
        Err(MoveError::Retry(format!(
            "no storage location served metadata.json: {}",
            errors.join("; ")
        )))
    }

    /// The build's `manifest.json` from `served`, the location whose
    /// `metadata.json` was read, else the first of the release's other
    /// `locations` that serves it, as a pull would fall back.
    async fn read_manifest(
        &self,
        subject: &ReleaseSubject,
        served: &TwinMetadataTarget,
        locations: &[String],
    ) -> Result<Vec<u8>, MoveError> {
        let mut errors = Vec::new();
        match self.objects.read_manifest(served).await {
            Ok(manifest) => return Ok(manifest),
            Err(error) => errors.push(format!("{}: {error}", served.bucket())),
        }
        for location in locations {
            let target =
                match TwinMetadataTarget::parse(location, &subject.template_name, &subject.hash) {
                    Ok(target) if &target != served => target,
                    Ok(_) => continue,
                    Err(error) => {
                        errors.push(error.to_string());
                        continue;
                    }
                };
            match self.objects.read_manifest(&target).await {
                Ok(manifest) => return Ok(manifest),
                Err(error) => errors.push(format!("{}: {error}", target.bucket())),
            }
        }
        Err(MoveError::Retry(format!(
            "no storage location served manifest.json: {}",
            errors.join("; ")
        )))
    }

    /// Write the twin's `metadata.json` beside the build in each of
    /// `locations` that still serves the build's shared `metadata.json`, and
    /// return those it was written to: only they go on the twin, since a pull
    /// from any other would find the source's document, or no build at all.
    /// A location that accepts the write but has lost the build isn't one.
    async fn write_twin_metadata(
        &self,
        subject: &ReleaseSubject,
        locations: &[String],
        environment: EnvironmentReference,
        document: &bytes::Bytes,
    ) -> Result<Vec<String>, MoveError> {
        let mut prepared = Vec::new();
        let mut errors = Vec::new();
        for location in locations {
            let target =
                match TwinMetadataTarget::parse(location, &subject.template_name, &subject.hash) {
                    Ok(target) => target,
                    Err(error) => {
                        errors.push(error.to_string());
                        continue;
                    }
                };
            if let Err(error) = self.objects.read_metadata(&target).await {
                errors.push(format!(
                    "{}: doesn't serve the build's metadata.json: {error}",
                    target.bucket()
                ));
                continue;
            }
            // A pull reads the build's manifest.json from the location it
            // pulls from, so one without it can't serve the twin either.
            if let Err(error) = self.objects.read_manifest(&target).await {
                errors.push(format!(
                    "{}: doesn't serve the build's manifest.json: {error}",
                    target.bucket()
                ));
                continue;
            }
            match self
                .objects
                .write_twin_metadata(&target, environment, document.clone())
                .await
            {
                Ok(()) => prepared.push(location.clone()),
                Err(error) => errors.push(format!("{}: {error}", target.bucket())),
            }
        }
        if prepared.is_empty() {
            return Err(MoveError::Retry(format!(
                "writing the twin's metadata.json: {}",
                errors.join("; ")
            )));
        }
        if !errors.is_empty() {
            tracing::warn!(
                errors = %errors.join("; "),
                "runtime_upgrade: the twin's metadata.json missed some storage locations; the twin \
                 is given only those it reached"
            );
        }
        Ok(prepared)
    }
}

/// Copy the manual warm-pool configs of every original of `subject`'s build
/// on the campaign's source to `twin` on `conn`, the twin's transaction.
///
/// A build can have originals under both hash spellings, bare and
/// `sha256:`, and creates of either resolve to the one twin. Whichever
/// spelling's item makes the twin, the other's finds it and ends done, so
/// the copy takes every original's configs here, at the insert: a shape more
/// than one configures gets the newest original's settings, whatever the
/// lease order. `subject` itself is always among them.
async fn copy_pools(
    conn: &mut PgConnection,
    campaign: &Campaign,
    subject: &ReleaseSubject,
    twin: Uuid,
) -> Result<(), MoveError> {
    let mut originals = releases::releases_of_hash_in_partition(
        &mut *conn,
        &subject.template_name,
        &subject.hash,
        partition_of(campaign.from),
    )
    .await
    .map_err(|error| MoveError::database("reading the build's original releases", error))?;
    if !originals.contains(&subject.id) {
        originals.push(subject.id);
    }
    pplx_space_cluster_pool::WarmPoolStore::copy_manual_configs_from_releases(
        conn, &originals, twin,
    )
    .await
    .map(|_| ())
    .map_err(|error| MoveError::database("copying the release's pool configs", error))
}
