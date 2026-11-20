//! The prefetch environment pass: warm each configured tag's and latest
//! release's twin, once per environment the node can run that has one.
//!
//! A twin is a release's `(template, hash)` bound to another environment.
//! Its rootfs is the original's, but node caches keep one build per
//! partition, so a create bound to the twin's environment cold-pulls unless
//! that partition is warm. The environment-less passes in [`super::tick`]
//! warm only the release a lookup without an environment resolves; this
//! pass asks the cluster again, naming each runnable environment, and warms
//! what comes back in that environment's partition.
//!
//! It follows the same rules as those passes:
//!
//! * **Visibility.** It warms only what its own lookup reports `Generic`,
//!   hardened against a verdict the tick already reached. An
//!   `OrgSpecific` answer marks the template for the tick's pin drop, like
//!   either pass's; an `Unspecified` one warms nothing and keeps pins.
//! * **Pins.** A tag pin is written under the environment's partition, so the
//!   tag's pin per partition follows its own resolution there. A release
//!   pin names the digest, as every pin does, and goes into the tick's keep
//!   set; a template whose environment lookup failed leaves the release
//!   prune, as a failed environment-less lookup does.
//! * **Dispatch.** Nothing is pulled here. Pulls are queued for the tick's
//!   single drain, which drops any whose template an org-specific verdict
//!   reached.
//! * **Lookups.** Per environment, the cluster lookups run concurrently, up
//!   to [`LOOKUP_CONCURRENCY`] at a time, so one slow lookup doesn't hold up
//!   the rest or the drain behind them. Their answers are applied one by one
//!   in configuration order, so a verdict one reaches still gates the ones
//!   after it; a lookup the verdict makes moot is only discarded.
//!
//! A cluster that predates twins ignores the environment selector and says so
//! (`environment_applied` false). The volume lookups report that as an error,
//! counted as `resolve_error`, so nothing is warmed per environment and the
//! tick treats what's bound there as unknown for that lookup alone: its pins
//! hold within the pin grace of that lookup's own last answer. A node whose environments are
//! unknown (see [`crate::node_environments`]) runs no environment pass at
//! all.

use std::collections::{BTreeSet, HashSet};
use std::time::Instant;

use futures_util::{StreamExt, stream};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_api_client::proto::TemplateVisibility;
use pplx_space_template_model::{TemplateName, TemplateTag};
use tracing::warn;

use super::{hardened_visibility, mark_template_org_specific, spawn_blocking_store_op};
use crate::db::TemplateCachePartition;
use crate::metrics::{PrefetchEnvironmentOutcome as Outcome, PrefetchEnvironmentPass as Pass};
use crate::node_environments::{EnvironmentAwareness, NodeEnvironments};
use crate::pinned_digests::PinnedDigestsStore;
use crate::volume::{LatestReleasesOutcome, PullTrigger, TagResolveOutcome};

/// Cluster lookups one environment's pass keeps in flight at once.
const LOOKUP_CONCURRENCY: usize = 4;

/// A pull the environment pass decided on, for the tick's drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EnvironmentPull {
    pub(super) template: String,
    pub(super) content_hash: String,
    pub(super) environment: EnvironmentReference,
    pub(super) pass: Pass,
}

impl EnvironmentPull {
    /// The pull-path trigger its pass corresponds to.
    pub(super) fn trigger(&self) -> PullTrigger {
        match self.pass {
            Pass::Tag => PullTrigger::PrefetchTag,
            Pass::Release => PullTrigger::PrefetchLatestRelease,
        }
    }
}

/// What the pass hands back to the tick.
#[derive(Debug, Default)]
pub(super) struct EnvironmentPassResult {
    /// Pulls to queue, tag-pass ones first.
    pub(super) pulls: Vec<EnvironmentPull>,
    /// `(template, content_hash)` release pins the tick's prune keeps.
    pub(super) release_pins_keep: Vec<(String, String)>,
    /// Templates whose environment latest-releases lookup failed: their
    /// release pins stay untouched this tick.
    pub(super) unprunable_templates: BTreeSet<String>,
    /// Tag lookups whose environment selector went unapplied: what's bound
    /// there is unknown for that tag alone, so only its pin ages out, once
    /// the pin grace has passed since its own last answer.
    pub(super) ignored_tag_lookups: Vec<TagLookup>,
    /// Tag lookups whose selector applied, so the cluster answered what's
    /// bound there, whether or not anything is, each with when its RPC
    /// returned: each restarts its own pin grace from then.
    pub(super) answered_tag_lookups: Vec<(TagLookup, Instant)>,
    /// Latest-releases lookups whose environment selector went unapplied, as
    /// by a cluster that predates twins: what's bound there is unknown for
    /// that template and environment alone, so the template's release pins
    /// hold only until the pin grace has passed since that lookup's own last
    /// answer.
    pub(super) ignored_release_lookups: Vec<ReleaseLookupKey>,
    /// Latest-releases lookups whose selector applied, with when each RPC
    /// returned: each restarts its own pin grace from then.
    pub(super) answered_release_lookups: Vec<(ReleaseLookupKey, Instant)>,
}

/// One environment tag lookup, keyed as its pin is:
/// `(template, tag, partition)`.
pub(super) type TagLookup = (String, String, TemplateCachePartition);

/// One environment latest-releases lookup: `(template, environment)`.
pub(super) type ReleaseLookupKey = (String, EnvironmentReference);

/// The tick state the pass reads and extends.
pub(super) struct EnvironmentPass<'a> {
    pub(super) pins: &'a PinnedDigestsStore,
    pub(super) db: &'a std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    pub(super) tag_tuples: &'a [(TemplateName, TemplateTag)],
    pub(super) templates: &'a [String],
    pub(super) latest_n: u32,
    pub(super) environments: &'a BTreeSet<EnvironmentReference>,
    pub(super) org_specific_tag_tuples: &'a mut HashSet<(String, String)>,
    pub(super) org_specific_templates: &'a mut BTreeSet<String>,
    /// `(template, content_hash)` release pins the environment-less pass
    /// wrote this tick. A twin shares its original's digest, and so its pin,
    /// so these aren't written again; one whose upsert failed is retried,
    /// and its twin pulled only once a write succeeds.
    pub(super) pinned_releases: &'a HashSet<(String, String)>,
}

pub(super) async fn run(pass: EnvironmentPass<'_>) -> EnvironmentPassResult {
    let EnvironmentPass {
        pins,
        db,
        tag_tuples,
        templates,
        latest_n,
        environments,
        org_specific_tag_tuples,
        org_specific_templates,
        pinned_releases,
    } = pass;
    let mut result = EnvironmentPassResult::default();
    // `(template, hash, environment)` pulls already queued this tick.
    let mut queued: HashSet<(String, String, EnvironmentReference)> = HashSet::new();
    // Release pins written this tick, by either pass, and the keep entries
    // this pass added.
    let mut pinned: HashSet<(String, String)> = pinned_releases.clone();
    let mut kept: HashSet<(String, String)> = HashSet::new();

    for &environment in environments {
        let label = environment.to_string();
        let partition =
            pplx_space_node_runtime_core::catalog::template_cache_partition(Some(environment));

        // `skip` is read before the lookups start; the loop below checks
        // again, because a verdict it applies can reach a later template.
        // Each lookup owns its strings, so the futures borrow nothing from
        // the loop and the pass's future stays `Send`.
        let tag_lookups: Vec<_> = tag_tuples
            .iter()
            .map(|(name, tag)| {
                let skip = org_specific_templates.contains(name.as_str());
                let (name, tag) = (name.to_string(), tag.to_string());
                async move {
                    if skip {
                        return None;
                    }
                    // Timed when the answer arrives, not when the tick ends,
                    // so a long tick doesn't stretch the lookup's grace.
                    let outcome =
                        crate::volume::resolve_tag_in(&name, &tag, Some(environment)).await;
                    Some((outcome, Instant::now()))
                }
            })
            .collect();
        let tag_lookups: Vec<Option<(TagResolveOutcome, Instant)>> = stream::iter(tag_lookups)
            .buffered(LOOKUP_CONCURRENCY)
            .collect()
            .await;
        for ((name, tag), lookup) in tag_tuples.iter().zip(tag_lookups) {
            let (name, tag) = (name.as_str(), tag.as_str());
            if org_specific_templates.contains(name) {
                continue;
            }
            let Some((lookup, answered_at)) = lookup else {
                continue;
            };
            if owns_tag_pin(partition)
                && matches!(
                    lookup,
                    TagResolveOutcome::Resolved { .. } | TagResolveOutcome::Unresolved
                )
            {
                result
                    .answered_tag_lookups
                    .push(((name.to_string(), tag.to_string(), partition), answered_at));
            }
            let (hash, visibility, served) = match lookup {
                TagResolveOutcome::Resolved {
                    hash,
                    visibility,
                    environment: served,
                } => (hash, visibility, served),
                TagResolveOutcome::Unresolved => {
                    record(name, &label, Pass::Tag, Outcome::NotBound);
                    // The tag's release has no build bound here now, as
                    // after the tag moves to a release without a twin
                    // yet: the pin this pass wrote before would keep the
                    // old digest from eviction everywhere. The legacy
                    // partition's pin is the environment-less pass's.
                    if owns_tag_pin(partition)
                        && let Err(error) = spawn_blocking_store_op(pins.clone(), {
                            let (name, tag) = (name.to_string(), tag.to_string());
                            move |store| store.delete_tag(&name, &tag, partition)
                        })
                        .await
                    {
                        warn!(
                            template = %name,
                            tag = %tag,
                            environment = %environment,
                            %error,
                            "prefetch: dropping a stale environment tag pin failed",
                        );
                    }
                    continue;
                }
                TagResolveOutcome::Error => {
                    record(name, &label, Pass::Tag, Outcome::ResolveError);
                    continue;
                }
                // An ignored selector's visibility verdict doesn't depend on
                // the selector, nor a malformed binding's on the binding, so
                // either still drops the template's pins.
                TagResolveOutcome::SelectorIgnored { org_specific: true }
                | TagResolveOutcome::InvalidOrgSpecific => {
                    record(name, &label, Pass::Tag, Outcome::SkippedOrgSpecific);
                    mark_template_org_specific(
                        tag_tuples,
                        org_specific_tag_tuples,
                        org_specific_templates,
                        name,
                    );
                    continue;
                }
                // As in the latest-N arm: the tick doesn't know what's
                // bound there for this tag, so it holds the tag's pin for
                // the grace period and then lets it age out.
                TagResolveOutcome::SelectorIgnored {
                    org_specific: false,
                } => {
                    record(name, &label, Pass::Tag, Outcome::ResolveError);
                    if owns_tag_pin(partition) {
                        result.ignored_tag_lookups.push((
                            name.to_string(),
                            tag.to_string(),
                            partition,
                        ));
                    }
                    continue;
                }
            };
            match hardened_visibility(org_specific_templates.contains(name), visibility) {
                TemplateVisibility::Generic => {}
                TemplateVisibility::OrgSpecific => {
                    record(name, &label, Pass::Tag, Outcome::SkippedOrgSpecific);
                    mark_template_org_specific(
                        tag_tuples,
                        org_specific_tag_tuples,
                        org_specific_templates,
                        name,
                    );
                    continue;
                }
                TemplateVisibility::Unspecified => {
                    record(name, &label, Pass::Tag, Outcome::VisibilityUnknown);
                    continue;
                }
            }
            // The legacy partition's tag pin is the environment-less pass's,
            // which resolves the same tag there.
            if owns_tag_pin(partition)
                && let Err(error) = spawn_blocking_store_op(pins.clone(), {
                    let (name, tag, hash) = (name.to_string(), tag.to_string(), hash.clone());
                    move |store| store.upsert_tag(&name, &tag, partition, &hash)
                })
                .await
            {
                warn!(
                    template = %name,
                    tag = %tag,
                    environment = %environment,
                    %error,
                    "prefetch: environment tag pin upsert failed; skipping dispatch",
                );
                record(name, &label, Pass::Tag, Outcome::PinError);
                continue;
            }
            // A legacy-partition pull is warmed under the environment-less
            // pass's pin, so it waits until a pin covers the digest: that
            // pass's lookup may have failed this tick, before writing one.
            if !owns_tag_pin(partition) {
                let pinned = spawn_blocking_store_op(pins.clone(), {
                    let name = name.to_string();
                    move |store| store.list_pinned_hashes_for_template(&name)
                })
                .await;
                if !digest_is_pinned(pinned.as_deref(), &hash) {
                    record(name, &label, Pass::Tag, Outcome::PinError);
                    continue;
                }
            }
            let outcome =
                decide(db, &mut queued, name, &hash, served, environment, partition).await;
            // A pull is counted once, when the drain dispatches or
            // suppresses it, so a later verdict can't count it twice.
            if outcome == Outcome::Dispatched {
                result.pulls.push(EnvironmentPull {
                    template: name.to_string(),
                    content_hash: hash,
                    environment,
                    pass: Pass::Tag,
                });
            } else {
                record(name, &label, Pass::Tag, outcome);
            }
        }

        // Runs whatever `latest_n` says, as the environment-less latest pass
        // does: it's also a visibility resolver, so it asks for at least one
        // release, and warms and pins nothing when `latest_n` is zero.
        let release_lookups: Vec<_> = templates
            .iter()
            .map(|template| {
                let skip = org_specific_templates.contains(template);
                let template = template.clone();
                async move {
                    if skip {
                        return None;
                    }
                    let outcome = crate::volume::list_latest_releases_in(
                        &template,
                        latest_n.max(1),
                        Some(environment),
                    )
                    .await;
                    // Timed as the tag lookups are.
                    Some((outcome, Instant::now()))
                }
            })
            .collect();
        let release_lookups: Vec<Option<(LatestReleasesOutcome, Instant)>> =
            stream::iter(release_lookups)
                .buffered(LOOKUP_CONCURRENCY)
                .collect()
                .await;
        for (template, lookup) in templates.iter().zip(release_lookups) {
            if org_specific_templates.contains(template) {
                continue;
            }
            let Some((lookup, answered_at)) = lookup else {
                continue;
            };
            let (visibility, releases) = match lookup {
                LatestReleasesOutcome::Resolved {
                    visibility,
                    releases,
                } => {
                    result
                        .answered_release_lookups
                        .push(((template.clone(), environment), answered_at));
                    (visibility, releases)
                }
                LatestReleasesOutcome::Error => {
                    record(template, &label, Pass::Release, Outcome::ResolveError);
                    result.unprunable_templates.insert(template.clone());
                    continue;
                }
                // As in the tag arm, the verdict still drops the pins.
                LatestReleasesOutcome::SelectorIgnored { org_specific: true } => {
                    record(template, &label, Pass::Release, Outcome::SkippedOrgSpecific);
                    mark_template_org_specific(
                        tag_tuples,
                        org_specific_tag_tuples,
                        org_specific_templates,
                        template,
                    );
                    continue;
                }
                // The tick holds this template's release pins for a bounded
                // time instead of marking it unprunable: a cluster that
                // keeps ignoring the selector would otherwise stop its
                // prune for good.
                LatestReleasesOutcome::SelectorIgnored {
                    org_specific: false,
                } => {
                    record(template, &label, Pass::Release, Outcome::ResolveError);
                    result
                        .ignored_release_lookups
                        .push((template.clone(), environment));
                    continue;
                }
            };
            let visibility =
                hardened_visibility(org_specific_templates.contains(template), visibility);
            // With no release bound to the environment only a verdict is
            // news: it drops the template's pins like any other. Unknown
            // visibility keeps them, below.
            if nothing_bound(&releases, visibility) {
                record(template, &label, Pass::Release, Outcome::NotBound);
                continue;
            }
            match visibility {
                TemplateVisibility::Generic => {}
                TemplateVisibility::OrgSpecific => {
                    record(template, &label, Pass::Release, Outcome::SkippedOrgSpecific);
                    mark_template_org_specific(
                        tag_tuples,
                        org_specific_tag_tuples,
                        org_specific_templates,
                        template,
                    );
                    continue;
                }
                TemplateVisibility::Unspecified => {
                    // Unknown is not a verdict: keep this template's pins.
                    record(template, &label, Pass::Release, Outcome::VisibilityUnknown);
                    result.unprunable_templates.insert(template.clone());
                    continue;
                }
            }
            if latest_n == 0 {
                continue;
            }
            for release in releases {
                let pin = (template.clone(), release.content_hash.clone());
                // Kept before the upsert, as the environment-less pass does:
                // a failed upsert mustn't drop a pin the window still wants.
                if kept.insert(pin.clone()) {
                    result.release_pins_keep.push(pin.clone());
                }
                // A pin written this tick already, by the release this twins
                // or by another environment's twin, isn't written again.
                if !pinned.contains(&pin) {
                    if let Err(error) = spawn_blocking_store_op(pins.clone(), {
                        let template = template.clone();
                        let (hash, id, created_at) = (
                            release.content_hash.clone(),
                            release.release_id.clone(),
                            release.created_at.clone(),
                        );
                        move |store| store.upsert_release(&template, &hash, &id, &created_at)
                    })
                    .await
                    {
                        warn!(
                            template = %template,
                            hash = %release.content_hash,
                            environment = %environment,
                            %error,
                            "prefetch: environment release pin upsert failed; skipping dispatch",
                        );
                        record(template, &label, Pass::Release, Outcome::PinError);
                        continue;
                    }
                    pinned.insert(pin);
                }
                let outcome = decide(
                    db,
                    &mut queued,
                    template,
                    &release.content_hash,
                    release.environment,
                    environment,
                    partition,
                )
                .await;
                // Counted at the drain, as the tag pass's pulls are.
                if outcome == Outcome::Dispatched {
                    result.pulls.push(EnvironmentPull {
                        template: template.clone(),
                        content_hash: release.content_hash,
                        environment,
                        pass: Pass::Release,
                    });
                } else {
                    record(template, &label, Pass::Release, outcome);
                }
            }
        }
    }
    result
}

/// Whether the environment pass writes and drops a tag's pin in
/// `partition`. Not in the legacy partition, the initial environment's
/// included: the environment-less pass resolves the same tag there and owns
/// that pin, which this pass deleting or rewriting would leave flapping
/// every tick.
fn owns_tag_pin(partition: TemplateCachePartition) -> bool {
    partition != TemplateCachePartition::Legacy
}

/// Whether one resolved twin needs a pull: not when this tick already queued
/// it, nor when its environment's partition holds its build. Nor when the
/// cluster served it `served` bound to no environment in the legacy
/// partition: the environment-less passes warm unbound releases there, and
/// an unbound release registered before releases recorded their binding
/// can land in another partition by its manifest, so pulling it here would
/// fetch its control files and refuse it again on every tick. The catalog
/// lookup locks the SQLite connection, so it runs on the blocking pool: a
/// pull or create holding that lock mustn't stall this async worker.
async fn decide(
    db: &std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    queued: &mut HashSet<(String, String, EnvironmentReference)>,
    template: &str,
    content_hash: &str,
    served: Option<EnvironmentReference>,
    environment: EnvironmentReference,
    partition: TemplateCachePartition,
) -> Outcome {
    if served.is_none() && partition == TemplateCachePartition::Legacy {
        return Outcome::Unbound;
    }
    let key = (template.to_string(), content_hash.to_string(), environment);
    if queued.contains(&key) {
        return Outcome::Coalesced;
    }
    let cached = tokio::task::spawn_blocking({
        let db = db.clone();
        let (template, content_hash) = (template.to_string(), content_hash.to_string());
        move || crate::volume::local_build_bound_to_exists(&db, &template, &content_hash, partition)
    })
    .await
    // A panicked lookup reads as not cached, as an unreadable catalog does.
    .unwrap_or(false);
    if cached {
        Outcome::AlreadyCached
    } else {
        queued.insert(key);
        Outcome::Dispatched
    }
}

/// Whether `hash` is among the digests a pin covers for its template; an
/// unreadable pin table covers nothing.
pub(super) fn digest_is_pinned(pinned: Result<&[String], &String>, hash: &str) -> bool {
    pinned.is_ok_and(|pinned| pinned.iter().any(|pinned| pinned == hash))
}

/// Whether a latest-releases answer says plainly that nothing is bound to the
/// environment: no releases, from a generic template. An org-specific
/// verdict and unknown visibility are handled as such.
pub(super) fn nothing_bound<T>(releases: &[T], visibility: TemplateVisibility) -> bool {
    releases.is_empty() && visibility == TemplateVisibility::Generic
}

/// The environments the pass warms: `None`, so the pass doesn't run, when
/// the node's environments are unknown ([`crate::node_environments`]) or it
/// runs none.
pub(super) fn environments_to_warm(
    node: Option<&NodeEnvironments>,
) -> Option<&BTreeSet<EnvironmentReference>> {
    node.map(|node| &node.runnable)
        .filter(|runnable| !runnable.is_empty())
}

/// The node's environments for one tick: read when [`reads_environments`]
/// says so, else `None`. A failed read keeps `last`, what the previous read
/// returned; a successful one replaces it.
pub(super) async fn tick_environments(
    awareness: EnvironmentAwareness,
    tag_tuples: &[(TemplateName, TemplateTag)],
    last: &mut Option<LastEnvironments>,
    read: impl AsyncFnOnce() -> Option<NodeEnvironments>,
) -> Option<NodeEnvironments> {
    if !reads_environments(awareness, tag_tuples) {
        return None;
    }
    match read().await {
        Some(read) => {
            *last = Some(LastEnvironments {
                environments: read.clone(),
                read_at: std::time::Instant::now(),
            });
            Some(read)
        }
        None => last
            .as_ref()
            .filter(|last| last.read_at.elapsed() < super::ENVIRONMENT_PIN_GRACE)
            .map(|last| last.environments.clone()),
    }
}

/// The node's environments as last read, and when. A failed read keeps
/// them for [`super::ENVIRONMENT_PIN_GRACE`], so a slow or restarting
/// node-manager costs no pins; after that they're unknown again, as before
/// the first read, so a node-manager that keeps refusing doesn't keep the
/// pass warming environments the node may no longer run.
pub(super) struct LastEnvironments {
    pub(super) environments: NodeEnvironments,
    pub(super) read_at: std::time::Instant,
}

/// Whether the tick reads the node's environments and may run the pass:
/// environment awareness is on and the tick has tags to resolve.
pub(super) fn reads_environments(
    awareness: EnvironmentAwareness,
    tag_tuples: &[(TemplateName, TemplateTag)],
) -> bool {
    awareness.is_on() && !tag_tuples.is_empty()
}

/// The partitions whose tag pins survive the tick. While the node's
/// environments are unknown and `holding` (within the pin grace of the last
/// tick that knew them, as release pins are held), `None`, which prunes
/// nothing, so a slow or restarting node-manager never costs a pin; past the
/// grace only the legacy partition's survive, as with awareness off, since
/// the pass that rewrites the others doesn't run and they'd otherwise pin
/// their digests for good. A database that never had the pass on has none
/// to drop.
pub(super) fn tag_pin_partitions_to_keep(
    awareness: EnvironmentAwareness,
    node: Option<&NodeEnvironments>,
    holding: bool,
) -> Option<Vec<TemplateCachePartition>> {
    if !awareness.is_on() {
        return Some(vec![TemplateCachePartition::Legacy]);
    }
    match node {
        Some(node) => Some(tag_pin_partitions(&node.runnable)),
        None if holding => None,
        None => Some(vec![TemplateCachePartition::Legacy]),
    }
}

/// The partitions whose tag pins survive the tick: the legacy one, which the
/// environment-less pass owns, and each runnable environment's.
pub(super) fn tag_pin_partitions(
    environments: &BTreeSet<EnvironmentReference>,
) -> Vec<TemplateCachePartition> {
    std::iter::once(TemplateCachePartition::Legacy)
        .chain(environments.iter().map(|environment| {
            pplx_space_node_runtime_core::catalog::template_cache_partition(Some(*environment))
        }))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The pass's pulls still safe to dispatch: an org-specific verdict reached
/// by the drain suppresses the rest, as [`super::partition_pending_tag_pulls`]
/// does for the tag pass. Each pull is counted here, once, as `dispatched`
/// or `skipped_org_specific`.
pub(super) fn dispatchable_environment_pulls(
    pulls: Vec<EnvironmentPull>,
    org_specific_templates: &BTreeSet<String>,
) -> Vec<EnvironmentPull> {
    let (dispatchable, suppressed): (Vec<_>, Vec<_>) = pulls
        .into_iter()
        .partition(|pull| !org_specific_templates.contains(&pull.template));
    for pull in suppressed {
        record(
            &pull.template,
            &pull.environment.to_string(),
            pull.pass,
            Outcome::SkippedOrgSpecific,
        );
    }
    for pull in &dispatchable {
        record(
            &pull.template,
            &pull.environment.to_string(),
            pull.pass,
            Outcome::Dispatched,
        );
    }
    dispatchable
}

fn record(template: &str, environment: &str, pass: Pass, outcome: Outcome) {
    crate::metrics::prefetch_environment_candidate(template, environment, pass, outcome);
}

#[cfg(test)]
mod tests;
