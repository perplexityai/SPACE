//! Unit tests for the pure safety logic of the warm-pool orphan sweep —
//! `partition`, `fresh_verdict`, and `WarmPodSnapshot::from_pod`/age. These
//! carry the load-bearing correctness (never delete a live/adopted pod) and run
//! with plain values, no kube client or Postgres.

use pplx_space_common_core::types::SandboxId;

use std::collections::HashSet;

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::api::core::v1::{
    ContainerState, ContainerStateTerminated, ContainerStatus, Pod, PodStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use pplx_space_common_core::kubernetes::{
    SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_ID_ANNOTATION, SANDBOX_POOL_ROLE_KEY,
    SANDBOX_POOL_ROLE_WARM, SANDBOX_POOL_SLOT_ID_KEY,
};
use uuid::Uuid;

use super::*;

const BASE_UNIX: i64 = 1_700_000_000;

fn base_now() -> Timestamp {
    Timestamp::from_second(BASE_UNIX).expect("valid base timestamp")
}

fn secs_before_now(secs: i64) -> Timestamp {
    base_now()
        .checked_sub(SignedDuration::from_secs(secs))
        .expect("timestamp in range")
}

fn snapshot(name: &str, slot_id: Uuid, sandbox_id: SandboxId) -> WarmPodSnapshot {
    WarmPodSnapshot {
        pod_name: name.to_string(),
        pod_uid: Some(format!("uid-{name}").parse().expect("non-empty uid")),
        resource_version: Some("100".to_string()),
        slot_id: Some(slot_id),
        sandbox_id: Some(sandbox_id),
        is_warm_pool: true,
        is_restore: false,
        terminating: false,
        phase_terminal: true,
        terminal_since: Some(secs_before_now(600)),
    }
}

fn candidate(name: &str, slot_id: Uuid, sandbox_id: SandboxId) -> Candidate {
    let snapshot = snapshot(name, slot_id, sandbox_id);
    Candidate {
        pod_uid: snapshot.pod_uid.clone().unwrap(),
        snapshot,
        slot_id,
        sandbox_id,
        age_secs: 600,
    }
}

// ---- partition ----

#[test]
fn partition_routes_orphan_bound_and_live_slot() {
    let orphan_slot = Uuid::now_v7();
    let bound_slot = Uuid::now_v7();
    let live_slot = Uuid::now_v7();
    let orphan = candidate("space-sbx-orphan-g1", orphan_slot, SandboxId::new());
    let bound = candidate("space-sbx-bound-g1", bound_slot, SandboxId::new());
    let live = candidate("space-sbx-live-g1", live_slot, SandboxId::new());

    let live_slots = HashSet::from([live_slot]);
    let bound_pods = HashSet::from(["space-sbx-bound-g1".to_string()]);
    let split = partition(vec![orphan, bound, live], &live_slots, &bound_pods);

    assert_eq!(split.orphans.len(), 1);
    assert_eq!(split.orphans[0].snapshot.pod_name, "space-sbx-orphan-g1");
    assert_eq!(split.sandbox_bound.len(), 1);
    assert_eq!(
        split.sandbox_bound[0].snapshot.pod_name,
        "space-sbx-bound-g1"
    );
    assert_eq!(split.slot_row_live.len(), 1);
    assert_eq!(
        split.slot_row_live[0].snapshot.pod_name,
        "space-sbx-live-g1"
    );
}

#[test]
fn partition_binding_wins_over_live_slot() {
    // A pod that is both slot-tracked and bound → classified sandbox_bound (the
    // load-bearing live-sandbox guard takes precedence).
    let slot = Uuid::now_v7();
    let c = candidate("space-sbx-both-g1", slot, SandboxId::new());
    let split = partition(
        vec![c],
        &HashSet::from([slot]),
        &HashSet::from(["space-sbx-both-g1".to_string()]),
    );
    assert!(split.orphans.is_empty());
    assert!(split.slot_row_live.is_empty());
    assert_eq!(split.sandbox_bound.len(), 1);
}

// ---- fresh_verdict ----

#[test]
fn fresh_verdict_covers_every_branch() {
    let slot = Uuid::now_v7();
    let sandbox = SandboxId::new();
    let snap = snapshot("space-sbx-orphan-g1", slot, sandbox);
    let uid = snap.pod_uid.clone().unwrap();

    // Proceed: same uid, still orphan-shaped.
    assert_eq!(fresh_verdict(Some(&snap), &uid), FreshVerdict::Proceed);

    // Gone.
    assert_eq!(fresh_verdict(None, &uid), FreshVerdict::AlreadyGone);

    // Different uid → stale identity.
    let mut other_uid = snap.clone();
    other_uid.pod_uid = Some("uid-different".parse().unwrap());
    assert_eq!(
        fresh_verdict(Some(&other_uid), &uid),
        FreshVerdict::StaleIdentity
    );

    // Recovered (no longer terminal) → no longer eligible.
    let mut recovered = snap.clone();
    recovered.phase_terminal = false;
    assert_eq!(
        fresh_verdict(Some(&recovered), &uid),
        FreshVerdict::NoLongerEligible
    );

    // Gained a restore annotation → no longer eligible.
    let mut restored = snap.clone();
    restored.is_restore = true;
    assert_eq!(
        fresh_verdict(Some(&restored), &uid),
        FreshVerdict::NoLongerEligible
    );
}

// ---- from_pod extraction ----

fn warm_pod_object(
    slot_id: Uuid,
    sandbox_id: SandboxId,
    phase: &str,
    finished: Option<Timestamp>,
) -> Pod {
    let terminated = finished.map(|ts| ContainerStatus {
        state: Some(ContainerState {
            terminated: Some(ContainerStateTerminated {
                finished_at: Some(Time(ts)),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    });
    Pod {
        metadata: ObjectMeta {
            name: Some("space-sbx-classify-g1".to_string()),
            uid: Some("pod-uid-classify".to_string()),
            resource_version: Some("42".to_string()),
            labels: Some(std::collections::BTreeMap::from([(
                SANDBOX_POOL_ROLE_KEY.to_string(),
                SANDBOX_POOL_ROLE_WARM.to_string(),
            )])),
            annotations: Some(std::collections::BTreeMap::from([
                (SANDBOX_POOL_SLOT_ID_KEY.to_string(), slot_id.to_string()),
                (SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string()),
            ])),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            container_statuses: terminated.map(|c| vec![c]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn from_pod_extracts_orphan_candidate() {
    let slot = Uuid::now_v7();
    let sandbox = SandboxId::new();
    let finished = secs_before_now(600);
    let snap = WarmPodSnapshot::from_pod(&SandboxPodView::from(warm_pod_object(
        slot,
        sandbox,
        "Succeeded",
        Some(finished),
    )));

    assert!(snap.is_warm_pool);
    assert!(snap.phase_terminal);
    assert!(!snap.is_restore);
    assert!(!snap.terminating);
    assert_eq!(snap.slot_id, Some(slot));
    assert_eq!(snap.sandbox_id, Some(sandbox));
    assert_eq!(snap.terminal_since, Some(finished));
    assert!(snap.is_orphan_candidate_shaped());
    assert_eq!(
        snap.terminal_age(base_now()),
        Some(SignedDuration::from_secs(600))
    );
}

#[test]
fn from_pod_flags_restore_and_running() {
    let mut restore = warm_pod_object(
        Uuid::now_v7(),
        SandboxId::new(),
        "Succeeded",
        Some(secs_before_now(600)),
    );
    restore.metadata.annotations.as_mut().unwrap().insert(
        SANDBOX_CHECKPOINT_ANNOTATION.to_string(),
        "snap".to_string(),
    );
    assert!(WarmPodSnapshot::from_pod(&SandboxPodView::from(restore.clone())).is_restore);
    assert!(
        !WarmPodSnapshot::from_pod(&SandboxPodView::from(restore.clone()))
            .is_orphan_candidate_shaped()
    );

    let running = warm_pod_object(Uuid::now_v7(), SandboxId::new(), "Running", None);
    assert!(!WarmPodSnapshot::from_pod(&SandboxPodView::from(running.clone())).phase_terminal);
    assert!(
        !WarmPodSnapshot::from_pod(&SandboxPodView::from(running.clone()))
            .is_orphan_candidate_shaped()
    );
}

#[test]
fn admission_failed_pod_without_containers_ages_from_creation() {
    // A Failed pod that never ran a container (e.g. kubelet admission failure)
    // has no `finishedAt` and no container status; creationTimestamp ≈ failure
    // time, so it ages from creation and is eventually collected — otherwise
    // this class leaks forever (codex).
    let mut pod = warm_pod_object(Uuid::now_v7(), SandboxId::new(), "Failed", None);
    let created = secs_before_now(600);
    pod.metadata.creation_timestamp = Some(Time(created));
    let snap = WarmPodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));

    assert!(snap.is_orphan_candidate_shaped());
    assert_eq!(snap.terminal_since, Some(created));
    assert_eq!(
        snap.terminal_age(base_now()),
        Some(SignedDuration::from_secs(600))
    );
}

#[test]
fn ran_pod_without_finished_at_is_unageable() {
    // A pod that ran a container but reports no `finishedAt` must NOT age from
    // creationTimestamp — that would make a long-lived pod that just went
    // terminal look aged and bypass min_terminal_age. It stays un-ageable.
    let mut pod = warm_pod_object(Uuid::now_v7(), SandboxId::new(), "Failed", None);
    pod.metadata.creation_timestamp = Some(Time(secs_before_now(86_400)));
    // A container status with a non-terminated (running) state: it ran, but no
    // finishedAt was recorded.
    pod.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
        state: Some(ContainerState {
            running: Some(Default::default()),
            ..Default::default()
        }),
        ..Default::default()
    }]);
    let snap = WarmPodSnapshot::from_pod(&SandboxPodView::from(pod.clone()));

    assert!(snap.is_orphan_candidate_shaped());
    assert_eq!(snap.terminal_since, None);
    assert_eq!(snap.terminal_age(base_now()), None);
}

#[test]
fn rotation_cursor_covers_whole_backlog() {
    // Mirrors the sweep's fairness rotation: each tick rotates the stable
    // orphan order by `cursor % total`, acts on up to `cap`, then advances the
    // cursor by the number acted. Over ceil(total/cap) ticks with nothing
    // removed (the dry-run case), the union of acted windows must cover every
    // candidate — otherwise later candidates starve (codex P2).
    let total = 5usize;
    let cap = 2usize;
    let order: Vec<usize> = (0..total).collect();
    let mut cursor = 0usize;
    let mut covered = HashSet::new();
    for _ in 0..total.div_ceil(cap) {
        let start = cursor % total;
        let mut rotated = order.clone();
        rotated.rotate_left(start);
        let acted: Vec<usize> = rotated.into_iter().take(cap).collect();
        covered.extend(acted.iter().copied());
        cursor = cursor.wrapping_add(acted.len());
    }
    let all: HashSet<usize> = (0..total).collect();
    assert_eq!(
        covered, all,
        "rotation must eventually cover the whole backlog"
    );
}

#[test]
fn future_finished_at_yields_zero_age() {
    let mut snap = snapshot("space-sbx-future-g1", Uuid::now_v7(), SandboxId::new());
    snap.terminal_since = Some(secs_before_now(-120));
    assert_eq!(snap.terminal_age(base_now()), Some(SignedDuration::ZERO));
}

// A 409 on DELETE means the UID+resourceVersion precondition failed, so nothing
// was deleted. It must map to `cleanup_failed`, not `cleanup_completed` — the
// latter would record a durable success for cleanup that never happened, and a
// later sweep re-evaluates the (possibly superseded) pod.
#[test]
fn superseded_identity_is_failed_not_completed() {
    assert_eq!(
        cleanup_event_type(&CleanupOutcome::SupersededIdentity),
        SandboxEventType::CleanupFailed,
    );
}

#[test]
fn cleanup_event_type_covers_every_variant() {
    assert_eq!(
        cleanup_event_type(&CleanupOutcome::Attempting),
        SandboxEventType::CleanupRequested,
    );
    assert_eq!(
        cleanup_event_type(&CleanupOutcome::Deleted),
        SandboxEventType::CleanupCompleted,
    );
    assert_eq!(
        cleanup_event_type(&CleanupOutcome::AlreadyGone),
        SandboxEventType::CleanupCompleted,
    );
    assert_eq!(
        cleanup_event_type(&CleanupOutcome::Failed("boom".to_string())),
        SandboxEventType::CleanupFailed,
    );
}
