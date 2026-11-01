use std::collections::HashSet;

use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;
use pplx_space_common_core::kubernetes::{SANDBOX_CLIENT_ID_LABEL, SANDBOX_ID_ANNOTATION};
use pplx_space_common_core::types::{ClientId, SandboxId};

use super::*;

fn id(value: &str) -> ClientId {
    ClientId::parse(value).expect("valid id")
}

/// A sandbox pod for `sandbox`, labeled `client` when set, in `phase`.
fn pod(
    name: &str,
    sandbox: SandboxId,
    client: Option<&str>,
    phase: &str,
    deleting: bool,
) -> SandboxPodView {
    let mut labels = serde_json::Map::new();
    if let Some(client) = client {
        labels.insert(SANDBOX_CLIENT_ID_LABEL.into(), client.into());
    }
    let mut metadata = serde_json::json!({
        "name": name,
        "namespace": "space",
        "labels": labels,
        "annotations": { SANDBOX_ID_ANNOTATION: sandbox.to_string() },
    });
    if deleting {
        metadata["deletionTimestamp"] = "2026-10-30T00:00:00Z".into();
    }
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": metadata,
        "spec": { "containers": [] },
        "status": { "phase": phase },
    }))
    .expect("valid pod");
    pod.into()
}

fn seeded(pods: impl IntoIterator<Item = SandboxPodView>) -> ClientLivePods {
    let index = ClientLivePods::default();
    index.apply(&Event::Init);
    for pod in pods {
        index.apply(&Event::InitApply(pod));
    }
    index.apply(&Event::InitDone);
    index
}

fn count(
    index: &ClientLivePods,
    ids: &CountedIds,
    activating: Option<SandboxId>,
) -> LiveSandboxCount {
    let pending = HashSet::new();
    index.count(
        ids,
        &LiveCountExclusions {
            activating,
            already_pending: &pending,
        },
        true,
    )
}

/// A client's count is its distinct sandboxes with a non-terminal pod
/// carrying its label: a pending or terminating pod still holds capacity and
/// counts, a terminal one doesn't, and two pods of one sandbox count once.
#[test]
fn counts_distinct_sandboxes_with_a_live_labeled_pod() {
    let (a, b, c, d) = (
        SandboxId::new(),
        SandboxId::new(),
        SandboxId::new(),
        SandboxId::new(),
    );
    let index = seeded([
        pod("a-1", a, Some("alpha"), "Running", false),
        // A restore pod beside the old one: still one sandbox.
        pod("a-2", a, Some("alpha"), "Pending", false),
        pod("b-1", b, Some("alpha"), "Running", true),
        pod("c-1", c, Some("alpha"), "Succeeded", false),
        pod("d-1", d, Some("beta"), "Running", false),
        pod("unlabeled", SandboxId::new(), None, "Running", false),
    ]);
    let alpha = CountedIds::Exactly(vec![id("alpha")]);
    assert_eq!(count(&index, &alpha, None), LiveSandboxCount::Known(2));
    assert_eq!(
        count(&index, &CountedIds::Exactly(vec![id("beta")]), None),
        LiveSandboxCount::Known(1)
    );
}

/// The sandbox being activated never counts against its own activation, so
/// reaching or replacing a sandbox the client already holds is never refused
/// (RFC Decision 5); nor does one the pending set already counts.
#[test]
fn the_activating_sandbox_and_pending_ones_never_count() {
    let (a, b) = (SandboxId::new(), SandboxId::new());
    let index = seeded([
        pod("a-1", a, Some("alpha"), "Running", false),
        pod("b-1", b, Some("alpha"), "Running", false),
    ]);
    let alpha = CountedIds::Exactly(vec![id("alpha")]);
    assert_eq!(count(&index, &alpha, Some(a)), LiveSandboxCount::Known(1));
    let pending = HashSet::from([b]);
    assert_eq!(
        index.count(
            &alpha,
            &LiveCountExclusions {
                activating: Some(a),
                already_pending: &pending,
            },
            true,
        ),
        LiveSandboxCount::Known(0)
    );
}

/// `unknown` counts every id outside the key set except `native`.
#[test]
fn unknown_counts_every_unlisted_id_but_native() {
    let index = seeded([
        pod("listed", SandboxId::new(), Some("alpha"), "Running", false),
        pod(
            "unlisted",
            SandboxId::new(),
            Some("gamma"),
            "Running",
            false,
        ),
        pod(
            "anonymous",
            SandboxId::new(),
            Some("unknown"),
            "Running",
            false,
        ),
        pod("native", SandboxId::new(), Some("native"), "Running", false),
    ]);
    assert_eq!(
        count(&index, &CountedIds::AllExcept(vec![id("alpha")]), None),
        LiveSandboxCount::Known(2)
    );
}

/// A sandbox leaves its client's set only when its last qualifying pod does,
/// and repeated or terminal events land idempotently.
#[test]
fn a_sandbox_leaves_with_its_last_live_pod() {
    let a = SandboxId::new();
    let index = seeded([
        pod("a-1", a, Some("alpha"), "Running", false),
        pod("a-2", a, Some("alpha"), "Pending", false),
    ]);
    let alpha = CountedIds::Exactly(vec![id("alpha")]);
    index.apply(&Event::Apply(pod(
        "a-1",
        a,
        Some("alpha"),
        "Running",
        false,
    )));
    index.apply(&Event::Delete(pod(
        "a-1",
        a,
        Some("alpha"),
        "Running",
        false,
    )));
    assert_eq!(count(&index, &alpha, None), LiveSandboxCount::Known(1));
    index.apply(&Event::Apply(pod("a-2", a, Some("alpha"), "Failed", false)));
    assert_eq!(count(&index, &alpha, None), LiveSandboxCount::Known(0));
    index.apply(&Event::Delete(pod(
        "a-2",
        a,
        Some("alpha"),
        "Failed",
        false,
    )));
    assert_eq!(count(&index, &alpha, None), LiveSandboxCount::Known(0));
}

/// A re-list swaps in whole at `InitDone`, so a pod deleted during a watch
/// gap stops counting; until the list completes the count isn't trusted.
#[test]
fn a_relist_replaces_the_index_when_it_completes() {
    let (a, b) = (SandboxId::new(), SandboxId::new());
    let index = seeded([pod("a-1", a, Some("alpha"), "Running", false)]);
    let alpha = CountedIds::Exactly(vec![id("alpha")]);
    index.apply(&Event::Init);
    index.apply(&Event::InitApply(pod(
        "b-1",
        b,
        Some("alpha"),
        "Running",
        false,
    )));
    assert_eq!(
        count(&index, &alpha, Some(b)),
        LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheStale)
    );
    index.apply(&Event::InitDone);
    assert_eq!(count(&index, &alpha, Some(b)), LiveSandboxCount::Known(0));
    assert_eq!(count(&index, &alpha, Some(a)), LiveSandboxCount::Known(1));
}

/// A cold reflector answers unknown, and so does one whose watch failed,
/// until the next event shows it's delivering again.
#[test]
fn a_cold_or_failing_reflector_answers_unknown() {
    let index = seeded([pod(
        "a-1",
        SandboxId::new(),
        Some("alpha"),
        "Running",
        false,
    )]);
    let alpha = CountedIds::Exactly(vec![id("alpha")]);
    let pending = HashSet::new();
    let exclusions = LiveCountExclusions {
        activating: None,
        already_pending: &pending,
    };
    assert_eq!(
        index.count(&alpha, &exclusions, false),
        LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheCold)
    );
    assert_eq!(
        index.count(&alpha, &exclusions, true),
        LiveSandboxCount::Known(1)
    );
    index.mark_watch_error();
    assert_eq!(
        index.count(&alpha, &exclusions, true),
        LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheStale)
    );
    index.apply(&Event::Apply(pod(
        "b-1",
        SandboxId::new(),
        Some("alpha"),
        "Pending",
        false,
    )));
    assert_eq!(
        index.count(&alpha, &exclusions, true),
        LiveSandboxCount::Known(2),
        "an event after the error shows the watch is delivering again"
    );
    // A re-list keeps the count untrusted until its rebuilt index is in.
    index.apply(&Event::Init);
    index.apply(&Event::InitApply(pod(
        "c-1",
        SandboxId::new(),
        Some("alpha"),
        "Running",
        false,
    )));
    assert_eq!(
        index.count(&alpha, &exclusions, true),
        LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheStale),
        "the old index serves until the re-list completes"
    );
    index.apply(&Event::InitDone);
    assert_eq!(
        index.count(&alpha, &exclusions, true),
        LiveSandboxCount::Known(1)
    );
}
