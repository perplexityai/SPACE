use pplx_space_common_core::types::{LocalArtifactId, SnapshotId};
use rusqlite::params;

use super::*;

fn v7(secs: u64) -> Uuid {
    Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, secs, 0))
}

/// UUIDv7 byte order is creation-time order — the property the
/// generation comparison rests on.
#[test]
fn uuid_v7_ordering_tracks_time() {
    let older = v7(1_000);
    let newer = v7(2_000);
    assert!(newer > older);
}

fn pinned(marker: bool) -> LocalEvidence {
    LocalEvidence::Pinned(PinnedArtifact {
        artifact_id: LocalArtifactId::new(),
        artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
        backup_marker: marker,
    })
}

fn unpinned(job: Option<(Uuid, u64)>) -> LocalEvidence {
    LocalEvidence::Unpinned {
        latest_job: job.map(|(snapshot_id, created_at_secs)| UploadGeneration {
            snapshot_id: SnapshotId::from_uuid(snapshot_id),
            created_at_secs,
        }),
    }
}

#[test]
fn decide_skips_without_ready_backup() {
    assert_eq!(
        decide(None, &unpinned(Some((v7(1_000), 0))), 0),
        PreDecision::Skip(SkipReason::NoReadyBackup)
    );
}

/// Pinned + local marker: identity proof complete, no RPC needed.
#[test]
fn decide_evicts_pinned_with_backup_marker() {
    assert_eq!(
        decide(None, &pinned(true), u64::MAX),
        PreDecision::Evict(EvictReason::LatestBackedIdle)
    );
}

/// Pinned without a marker must demand exact-artifact proof from
/// the cluster — never fall back to job/generation comparison.
#[test]
fn decide_demands_exact_proof_for_unmarked_pin() {
    assert_eq!(decide(None, &pinned(false), 0), PreDecision::ProvePinned);
}

/// **Reviewer regression**: current artifact unbacked (pin present,
/// marker unset), an OLDER upload job exists, and the cluster's
/// latest ready snapshot is newer than that job. The previous
/// implementation compared cluster vs the stale job and evicted as
/// `stale_generation`, destroying the current (newest, unbacked)
/// local bytes. The decision must never consult the job journal
/// for pinned rows: it demands exact-artifact proof, and an
/// unprovable artifact skips.
#[test]
fn decide_never_uses_job_journal_for_pinned_rows() {
    // The old inputs that produced the unsafe verdict: an old job
    // (gen1) and a newer cluster generation (gen2) — irrelevant
    // now; only the pin matters.
    let evidence = pinned(false);
    assert_eq!(
        decide(Some(SnapshotId::from_uuid(v7(2_000))), &evidence, u64::MAX),
        PreDecision::ProvePinned,
        "pinned rows must require exact-artifact proof regardless of job/cluster comparison"
    );
}

/// Pin set but the artifact row is gone — inconsistent; never
/// evict.
#[test]
fn decide_skips_pinned_row_missing() {
    assert_eq!(
        decide(
            Some(SnapshotId::from_uuid(v7(2_000))),
            &LocalEvidence::PinnedRowMissing,
            0
        ),
        PreDecision::Skip(SkipReason::CurrentArtifactUnproven)
    );
}

#[test]
fn decide_skips_unknown_local_generation() {
    assert_eq!(
        decide(Some(SnapshotId::from_uuid(v7(1_000))), &unpinned(None), 0),
        PreDecision::Skip(SkipReason::UnknownLocalGeneration)
    );
}

/// The failed-upload trap: the newest local generation has no ready
/// backup (cluster's latest ready is older). Evicting would silently
/// roll the sandbox back — must always skip.
#[test]
fn decide_never_evicts_when_local_state_is_newer_than_backup() {
    assert_eq!(
        decide(
            Some(SnapshotId::from_uuid(v7(1_000))),
            &unpinned(Some((v7(2_000), 0))),
            0
        ),
        PreDecision::Skip(SkipReason::LocalNewerThanBackup)
    );
}

/// "A newer generation exists elsewhere" is not a proof about the
/// local bytes (a pre-tracking suspend may have no job at all) —
/// the old `stale_generation` eviction is gone.
#[test]
fn decide_skips_when_cluster_is_newer_than_local_jobs() {
    assert_eq!(
        decide(
            Some(SnapshotId::from_uuid(v7(2_000))),
            &unpinned(Some((v7(1_000), 0))),
            0
        ),
        PreDecision::Skip(SkipReason::GenerationUnproven)
    );
}

/// Legacy equality proof: newest local job == cluster latest ready,
/// and the recency witness holds (last sandbox transition not later
/// than the job's creation + slack).
#[test]
fn decide_evicts_unpinned_on_exact_generation_match_with_recency() {
    let generation = v7(1_500);
    let job_created = 10_000u64;
    assert_eq!(
        decide(
            Some(SnapshotId::from_uuid(generation)),
            &unpinned(Some((generation, job_created))),
            job_created + RECENCY_WITNESS_SLACK_SECS,
        ),
        PreDecision::Evict(EvictReason::LatestBackedIdle)
    );
}

/// Recency witness failure: the sandbox transitioned well after the
/// newest upload job was staged — a later suspend may exist with no
/// job at all; equality with the older generation proves nothing
/// about the current bytes.
#[test]
fn decide_skips_unpinned_when_sandbox_transitioned_after_last_job() {
    let generation = v7(1_500);
    let job_created = 10_000u64;
    assert_eq!(
        decide(
            Some(SnapshotId::from_uuid(generation)),
            &unpinned(Some((generation, job_created))),
            job_created + RECENCY_WITNESS_SLACK_SECS + 1,
        ),
        PreDecision::Skip(SkipReason::GenerationUnproven)
    );
}

// ── SQLite-backed candidate enumeration ─────────────────────────

fn open_test_db() -> Connection {
    crate::db::test_conn()
}

fn insert_sandbox(conn: &Connection, id: &str, state: &str, updated_at_secs: u64) {
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json
         ) VALUES (?1, ?2, ?3, 'seed', 'u', 'fc', '0', ?4, '{}', '{}', '{}')",
        params![
            id,
            id.get(..8.min(id.len())).unwrap_or(id),
            state,
            updated_at_secs.to_string()
        ],
    )
    .unwrap();
}

fn insert_upload_job(
    conn: &Connection,
    job_id: &str,
    sandbox_id: &str,
    status: &str,
    snapshot_id: SnapshotId,
    created_at: &str,
) {
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, payload, subject_id, created_at)
         VALUES (?1, 'snapshot_upload', ?2, ?3, ?4, ?5)",
        params![
            job_id,
            status,
            format!(r#"{{"snapshot_id":"{snapshot_id}","meta":{{}}}}"#),
            sandbox_id,
            created_at,
        ],
    )
    .unwrap();
}

fn candidate_ids(candidates: Vec<EvictionCandidate>) -> Vec<String> {
    candidates
        .into_iter()
        .map(|candidate| candidate.sandbox_id)
        .collect()
}

#[test]
fn defaults_use_disk_pressure_retention_policy() {
    assert!((EvictionConfig::DEFAULT_ENGAGE_FRACTION - 0.75).abs() < f64::EPSILON);
    assert!((EvictionConfig::DEFAULT_TARGET_FRACTION - 0.70).abs() < f64::EPSILON);
    assert_eq!(EvictionConfig::DEFAULT_MIN_IDLE_SECS, 90_000);
    assert_eq!(EvictionConfig::DEFAULT_MAX_PER_TICK, 100);
    // The 25h floor is advisory: sub-floor values are honored (only
    // warned about) rather than clamped up to 90_000.
    assert!(EvictionConfig::is_below_recommended_min_idle(0));
    assert!(EvictionConfig::is_below_recommended_min_idle(24 * 60 * 60));
    assert!(!EvictionConfig::is_below_recommended_min_idle(25 * 60 * 60));
    assert!(!EvictionConfig::is_below_recommended_min_idle(26 * 60 * 60));
}

#[test]
fn skip_reason_scan_policy_is_exhaustive() {
    for (reason, should_back_off, halts_scan) in [
        (SkipReason::NoReadyBackup, true, false),
        (SkipReason::LocalNewerThanBackup, true, false),
        (SkipReason::UnknownLocalGeneration, true, false),
        (SkipReason::CurrentArtifactUnproven, true, false),
        (SkipReason::GenerationUnproven, true, false),
        (SkipReason::RetryBackoff, false, false),
        (SkipReason::NotSuspendedAtExecute, false, false),
        (SkipReason::LocalReadError, true, false),
        (SkipReason::RpcError, true, false),
        (SkipReason::RpcTransportError, false, true),
    ] {
        assert_eq!(reason.should_back_off(), should_back_off, "{reason:?}");
        assert_eq!(reason.halts_scan(), halts_scan, "{reason:?}");
    }
}

#[test]
fn rpc_failure_classification_only_halts_for_scan_wide_codes() {
    for code in [
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::DeadlineExceeded,
        tonic::Code::ResourceExhausted,
        tonic::Code::Unavailable,
        tonic::Code::Internal,
        tonic::Code::Unauthenticated,
        tonic::Code::PermissionDenied,
    ] {
        assert_eq!(
            classify_rpc_failure(&tonic::Status::new(code, "scan-wide")),
            SkipReason::RpcTransportError,
            "{code:?}"
        );
    }
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::FailedPrecondition,
    ] {
        assert_eq!(
            classify_rpc_failure(&tonic::Status::new(code, "candidate")),
            SkipReason::RpcError,
            "{code:?}"
        );
    }
}

#[test]
fn candidate_skip_backoff_expires() {
    let mut state = CandidateScanState::default();
    let now = Instant::now();

    state.back_off("sbx-a", now);
    assert!(state.is_backed_off("sbx-a"));

    state.begin_tick(now + CANDIDATE_SKIP_BACKOFF);
    assert!(!state.is_backed_off("sbx-a"));
}

#[test]
fn candidate_scoped_failures_back_off_and_advance_cursor() {
    let previous = CandidateCursor {
        idle_witness_secs: 1_000,
        sandbox_id: "sbx-previous".to_string(),
    };
    let candidate = CandidateCursor {
        idle_witness_secs: 2_000,
        sandbox_id: "sbx-transient".to_string(),
    };
    let mut state = CandidateScanState {
        cursor: Some(previous.clone()),
        ..Default::default()
    };
    let mut report = TickReport::default();

    for reason in [SkipReason::RpcError, SkipReason::LocalReadError] {
        state.cursor = Some(previous.clone());
        let flow = record_candidate_skip(
            &mut report,
            &mut state,
            "sbx-transient",
            candidate.clone(),
            reason,
            Instant::now(),
        );

        assert_eq!(flow, CandidateFlow::Continue, "{reason:?}");
        assert_eq!(state.cursor, Some(candidate.clone()), "{reason:?}");
        assert!(state.is_backed_off("sbx-transient"), "{reason:?}");
    }
}

#[test]
fn transport_failure_halts_without_advancing_cursor() {
    let previous = CandidateCursor {
        idle_witness_secs: 1_000,
        sandbox_id: "sbx-previous".to_string(),
    };
    let candidate = CandidateCursor {
        idle_witness_secs: 2_000,
        sandbox_id: "sbx-unreached".to_string(),
    };
    let mut state = CandidateScanState {
        cursor: Some(previous.clone()),
        ..Default::default()
    };
    let mut report = TickReport::default();

    let flow = record_candidate_skip(
        &mut report,
        &mut state,
        "sbx-unreached",
        candidate,
        SkipReason::RpcTransportError,
        Instant::now(),
    );

    assert_eq!(flow, CandidateFlow::Stop);
    assert_eq!(state.cursor, Some(previous));
    assert!(!state.is_backed_off("sbx-unreached"));
}

#[test]
fn state_change_race_advances_without_backoff() {
    let candidate = CandidateCursor {
        idle_witness_secs: 2_000,
        sandbox_id: "sbx-resumed".to_string(),
    };
    let mut state = CandidateScanState::default();
    let mut report = TickReport::default();

    let flow = record_candidate_skip(
        &mut report,
        &mut state,
        "sbx-resumed",
        candidate.clone(),
        SkipReason::NotSuspendedAtExecute,
        Instant::now(),
    );

    assert_eq!(flow, CandidateFlow::Continue);
    assert_eq!(state.cursor, Some(candidate));
    assert!(!state.is_backed_off("sbx-resumed"));
}

#[test]
fn durable_skip_advances_cursor_and_backs_off_candidate() {
    let candidate = CandidateCursor {
        idle_witness_secs: 2_000,
        sandbox_id: "sbx-unproven".to_string(),
    };
    let mut state = CandidateScanState::default();
    let mut report = TickReport::default();

    let flow = record_candidate_skip(
        &mut report,
        &mut state,
        "sbx-unproven",
        candidate.clone(),
        SkipReason::CurrentArtifactUnproven,
        Instant::now(),
    );

    assert_eq!(flow, CandidateFlow::Continue);
    assert_eq!(state.cursor, Some(candidate));
    assert!(state.is_backed_off("sbx-unproven"));
}

#[test]
fn only_queued_repair_clears_proof_backoff() {
    for (outcome, expected_backoff) in [
        (Some(LegacyRepairOutcome::Queued), false),
        (Some(LegacyRepairOutcome::Skipped), true),
        (Some(LegacyRepairOutcome::Failed), true),
        (None, true),
    ] {
        let mut state = CandidateScanState::default();
        state.back_off("sbx-unproven", Instant::now());

        state.apply_repair_outcome("sbx-unproven", outcome);

        assert_eq!(state.is_backed_off("sbx-unproven"), expected_backoff);
    }
}

#[test]
fn failed_repair_backs_off_fresh_candidate() {
    // A repair attempt that could not queue an upload must back the
    // row off; otherwise the oldest permanently-failing rows (e.g.
    // sandboxes whose cluster row was hard-deleted) re-enter the
    // `updated_at ASC` repair scan every tick, exhaust the per-tick
    // attempt budget, and starve every repairable row behind them.
    for (outcome, expected_backoff) in [
        (Some(LegacyRepairOutcome::Failed), true),
        (Some(LegacyRepairOutcome::Skipped), true),
        (Some(LegacyRepairOutcome::Queued), false),
        (None, false),
    ] {
        let mut state = CandidateScanState::default();

        state.apply_repair_outcome("sbx-unrepairable", outcome);

        assert_eq!(
            state.is_backed_off("sbx-unrepairable"),
            expected_backoff,
            "{outcome:?}"
        );
    }
}

#[test]
fn repair_scan_reaches_rows_behind_a_fully_backed_off_head() {
    // Regression: a fetch window that does not scale with the
    // backed-off set degenerates to an all-backed-off page once
    // enough permanently-failing rows accumulate at the oldest end
    // of the scan, and the repairable rows behind them starve.
    let mut state = CandidateScanState::default();
    let now = Instant::now();
    let head: Vec<String> = (0..100)
        .map(|i| format!("sbx-unrepairable-{i:03}"))
        .collect();
    for sandbox_id in &head {
        state.back_off(sandbox_id, now);
    }
    let mut population = head;
    population.push("sbx-repairable".to_string());

    let window = repair_scan_window(&state, 5);
    let fetched: Vec<String> = population.into_iter().take(window).collect();
    let plan = plan_repair_attempts(fetched, &state, 5);

    assert_eq!(plan, vec!["sbx-repairable"]);
}

#[test]
fn repair_plan_passes_over_backed_off_head() {
    let mut state = CandidateScanState::default();
    let now = Instant::now();
    state.back_off("sbx-orphan-1", now);
    state.back_off("sbx-orphan-2", now);
    let candidates = vec![
        "sbx-orphan-1".to_string(),
        "sbx-orphan-2".to_string(),
        "sbx-repairable-1".to_string(),
        "sbx-repairable-2".to_string(),
        "sbx-repairable-3".to_string(),
    ];

    let plan = plan_repair_attempts(candidates, &state, 2);

    assert_eq!(plan, vec!["sbx-repairable-1", "sbx-repairable-2"]);
}

fn set_activity_clocks(
    conn: &Connection,
    id: &str,
    last_access_at: Option<u64>,
    last_command_at: Option<u64>,
) {
    let mut state = serde_json::Map::new();
    if let Some(secs) = last_access_at {
        state.insert("last_access_at".into(), secs.into());
    }
    if let Some(secs) = last_command_at {
        state.insert("last_command_at".into(), secs.into());
    }
    conn.execute(
        "UPDATE sandboxes SET auto_lifecycle_state_json = ?1 WHERE id = ?2",
        params![serde_json::Value::Object(state).to_string(), id],
    )
    .unwrap();
}

#[test]
fn activity_witness_overrides_churned_updated_at() {
    // The 60s facet reconcile keeps rewriting rows the cluster no
    // longer places here, so their updated_at never ages past the
    // idle floor while the real activity clocks in
    // auto_lifecycle_state_json sit weeks old. Idleness must follow
    // the activity witness, not the row-write clock.
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-evader", "suspended", 9_000);
    insert_pinned_artifact(&conn, "sbx-evader", &v7(1_000).to_string(), None);
    set_activity_clocks(&conn, "sbx-evader", Some(1_000), Some(900));

    let got = candidate_ids(list_candidates(&conn, 5_000, None, 10).unwrap());
    assert_eq!(
        got,
        vec!["sbx-evader".to_string()],
        "a churned updated_at must not hide a sandbox whose last real \
         activity is past the idle floor"
    );
}

#[test]
fn recent_activity_protects_despite_stale_updated_at() {
    // The inverse guard: a sandbox whose activity clocks are fresh
    // stays protected even if its row happens not to have been
    // written since — the witness governs in both directions.
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-active", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-active", &v7(1_000).to_string(), None);
    set_activity_clocks(&conn, "sbx-active", None, Some(9_000));

    let got = candidate_ids(list_candidates(&conn, 5_000, None, 10).unwrap());
    assert!(
        got.is_empty(),
        "fresh activity must protect the wake-cache regardless of \
         updated_at, got {got:?}"
    );
}

#[test]
fn non_u64_activity_clocks_fall_back_instead_of_poisoning_the_scan() {
    // Syntactically valid JSON can still carry values our writers
    // never produce: a negative or fractional clock decodes into the
    // SqliteU64 witness column and fails the whole row mapping.
    // Wrong-typed clocks must be treated like absent ones.
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-negative", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-negative", &v7(1_000).to_string(), None);
    conn.execute(
        "UPDATE sandboxes SET auto_lifecycle_state_json = \
         '{\"last_access_at\": -1}' WHERE id = 'sbx-negative'",
        [],
    )
    .unwrap();
    insert_sandbox(&conn, "sbx-fractional", "suspended", 2_000);
    insert_pinned_artifact(&conn, "sbx-fractional", &v7(2_000).to_string(), None);
    conn.execute(
        "UPDATE sandboxes SET auto_lifecycle_state_json = \
         '{\"last_command_at\": 1234.5}' WHERE id = 'sbx-fractional'",
        [],
    )
    .unwrap();

    let got = candidate_ids(
        list_candidates(&conn, 5_000, None, 10)
            .expect("wrong-typed activity clocks must not abort the candidate scan"),
    );
    assert_eq!(
        got,
        vec!["sbx-negative".to_string(), "sbx-fractional".to_string()],
        "both rows fall back to their (old) updated_at"
    );
}

#[test]
fn suspend_time_witness_dates_each_proof_kind() {
    // Pinned artifacts are UUIDv7s minted at suspend time.
    let artifact_id = LocalArtifactId::new();
    let now = now_secs();
    let (secs, proof) = suspend_time_witness(&LocalEvidence::Pinned(PinnedArtifact {
        artifact_id,
        artifact_kind: "full".parse().unwrap(),
        backup_marker: true,
    }))
    .expect("a v7 pin must date the suspend");
    assert_eq!(proof, SuspendAgeProof::Pinned);
    assert!(
        secs.abs_diff(now) < 5,
        "v7 pin timestamp must track mint time (got {secs}, now {now})"
    );

    // Legacy rows are dated by their newest upload job.
    let (secs, proof) = suspend_time_witness(&LocalEvidence::Unpinned {
        latest_job: Some(UploadGeneration {
            snapshot_id: SnapshotId::from_uuid(v7(1_000)),
            created_at_secs: 1_000,
        }),
    })
    .expect("a legacy upload job must date the suspend");
    assert_eq!((secs, proof), (1_000, SuspendAgeProof::LegacyUpload));

    // Undatable shapes yield None rather than a fake age.
    assert!(suspend_time_witness(&LocalEvidence::PinnedRowMissing).is_none());
    assert!(suspend_time_witness(&LocalEvidence::Unpinned { latest_job: None }).is_none());

    // Non-v7 ids carry no timestamp.
    assert!(uuid_v7_epoch_secs(Uuid::nil()).is_none());
}

#[test]
fn suspend_age_proof_display_matches_as_str() {
    for proof in [SuspendAgeProof::Pinned, SuspendAgeProof::LegacyUpload] {
        assert_eq!(proof.to_string(), proof.as_str());
    }
}

#[test]
fn malformed_lifecycle_state_json_falls_back_instead_of_poisoning_the_scan() {
    // json_extract aborts the whole statement on malformed JSON, so
    // without a json_valid guard one corrupt row would fail every
    // eviction tick and shield the valid cold rows around it. The
    // corrupt row must fall back to updated_at semantics and the scan
    // must keep working.
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-corrupt", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-corrupt", &v7(1_000).to_string(), None);
    conn.execute(
        "UPDATE sandboxes SET auto_lifecycle_state_json = 'not json' \
         WHERE id = 'sbx-corrupt'",
        [],
    )
    .unwrap();
    insert_sandbox(&conn, "sbx-valid", "suspended", 2_000);
    insert_pinned_artifact(&conn, "sbx-valid", &v7(2_000).to_string(), None);

    let got = candidate_ids(
        list_candidates(&conn, 5_000, None, 10)
            .expect("a corrupt lifecycle-state blob must not abort the candidate scan"),
    );
    assert_eq!(
        got,
        vec!["sbx-corrupt".to_string(), "sbx-valid".to_string()],
        "the corrupt row falls back to its (old) updated_at and the \
         valid row stays visible"
    );
}

#[test]
fn legacy_repair_scan_uses_the_activity_witness() {
    // The churned no-proof shape: no pin, no upload job, updated_at
    // rewritten every minute, activity clocks weeks old. Excluded
    // from the eviction scan by the missing proof seed AND from the
    // repair scan by the churned updated_at — unrepairable and
    // unevictable forever unless the repair scan reads the witness.
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-legacy-churned", "suspended", 9_000);
    set_activity_clocks(&conn, "sbx-legacy-churned", Some(1_000), None);

    let got = list_legacy_reupload_candidates(&conn, 5_000, 10).unwrap();
    assert_eq!(
        got,
        vec!["sbx-legacy-churned".to_string()],
        "a churned updated_at must not hide a proof-less row from the \
         repair scan"
    );
}

#[test]
fn candidates_filter_state_idle_and_inflight_uploads() {
    let conn = open_test_db();
    // Cold suspended but legacy-unprovable — excluded before it can
    // consume the per-tick candidate cap.
    insert_sandbox(&conn, "sbx-cold", "suspended", 1_000);
    // Suspended but recently touched — excluded by idle cutoff.
    insert_sandbox(&conn, "sbx-warm", "suspended", 9_000);
    // Cold but not suspended — excluded.
    insert_sandbox(&conn, "sbx-running", "started", 1_000);
    // Cold suspended with an in-flight upload — excluded.
    insert_sandbox(&conn, "sbx-uploading", "suspended", 1_000);
    insert_upload_job(
        &conn,
        "j-up",
        "sbx-uploading",
        "running",
        SnapshotId::from_uuid(v7(1_000)),
        "2026-01-01 00:00:00",
    );
    // Cold suspended with only a terminal upload — candidate.
    insert_sandbox(&conn, "sbx-done", "suspended", 2_000);
    insert_upload_job(
        &conn,
        "j-done",
        "sbx-done",
        "completed",
        SnapshotId::from_uuid(v7(2_000)),
        "2026-01-01 00:00:00",
    );
    // Cold suspended with a current artifact pin — candidate even
    // before a local marker exists; the tick can prove exact remote
    // durability with the cluster.
    insert_sandbox(&conn, "sbx-pinned", "suspended", 3_000);
    insert_pinned_artifact(&conn, "sbx-pinned", &v7(3_000).to_string(), None);

    let got = candidate_ids(list_candidates(&conn, 5_000, None, 10).unwrap());
    assert_eq!(got, vec!["sbx-done".to_string(), "sbx-pinned".to_string()]);

    // The cap applies oldest-first.
    let capped = candidate_ids(list_candidates(&conn, 5_000, None, 1).unwrap());
    assert_eq!(capped, vec!["sbx-done".to_string()]);
}

#[test]
fn candidates_do_not_starve_on_legacy_rows_without_upload_generation() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-unknown-a", "suspended", 1_000);
    insert_sandbox(&conn, "sbx-unknown-b", "suspended", 1_100);
    insert_sandbox(&conn, "sbx-done", "suspended", 1_200);
    insert_upload_job(
        &conn,
        "j-done",
        "sbx-done",
        "completed",
        SnapshotId::from_uuid(v7(1_200)),
        "2026-01-01 00:00:00",
    );
    insert_sandbox(&conn, "sbx-pinned", "suspended", 1_300);
    insert_pinned_artifact(&conn, "sbx-pinned", &v7(1_300).to_string(), None);

    let got = candidate_ids(list_candidates(&conn, 5_000, None, 2).unwrap());

    assert_eq!(got, vec!["sbx-done".to_string(), "sbx-pinned".to_string()]);
}

#[test]
fn candidates_exclude_malformed_updated_at() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-valid", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-valid", &v7(1_000).to_string(), Some("123"));
    insert_sandbox(&conn, "sbx-malformed", "suspended", 1_100);
    insert_pinned_artifact(&conn, "sbx-malformed", &v7(1_100).to_string(), Some("123"));
    conn.execute(
        "UPDATE sandboxes SET updated_at = 'not-an-epoch' WHERE id = 'sbx-malformed'",
        [],
    )
    .unwrap();

    let got = candidate_ids(list_candidates(&conn, 5_000, None, 10).unwrap());

    assert_eq!(got, vec!["sbx-valid".to_string()]);
    assert_eq!(count_malformed_suspended_updated_at(&conn).unwrap(), 1);
}

#[test]
fn candidate_cursor_advances_past_skips_and_wraps() {
    let conn = open_test_db();
    for id in ["sbx-a", "sbx-b", "sbx-c"] {
        insert_sandbox(&conn, id, "suspended", 1_000);
        insert_pinned_artifact(&conn, id, &Uuid::now_v7().to_string(), Some("123"));
    }

    let first = list_candidates(&conn, 5_000, None, 2).unwrap();
    assert_eq!(candidate_ids(first.clone()), vec!["sbx-a", "sbx-b"]);

    let second = list_candidates(&conn, 5_000, Some(&first[1].cursor), 2).unwrap();
    assert_eq!(candidate_ids(second.clone()), vec!["sbx-c"]);

    let wrapped = list_candidates(&conn, 5_000, Some(&second[0].cursor), 2).unwrap();
    assert_eq!(candidate_ids(wrapped), vec!["sbx-a", "sbx-b"]);
}

#[test]
fn legacy_reupload_candidates_are_unprovable_no_job_rows_only() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-legacy-a", "suspended", 1_000);
    insert_sandbox(&conn, "sbx-legacy-b", "suspended", 1_100);
    insert_sandbox(&conn, "sbx-warm", "suspended", 9_000);
    insert_sandbox(&conn, "sbx-malformed", "suspended", 1_150);
    conn.execute(
        "UPDATE sandboxes SET updated_at = 'not-an-epoch' WHERE id = 'sbx-malformed'",
        [],
    )
    .unwrap();
    insert_sandbox(&conn, "sbx-running", "started", 1_000);
    insert_sandbox(&conn, "sbx-uploading", "suspended", 1_000);
    insert_upload_job(
        &conn,
        "j-up",
        "sbx-uploading",
        "pending",
        SnapshotId::from_uuid(v7(1_000)),
        "2026-01-01 00:00:00",
    );
    insert_sandbox(&conn, "sbx-done", "suspended", 1_200);
    insert_upload_job(
        &conn,
        "j-done",
        "sbx-done",
        "completed",
        SnapshotId::from_uuid(v7(1_200)),
        "2026-01-01 00:00:00",
    );
    insert_sandbox(&conn, "sbx-pinned", "suspended", 1_300);
    insert_pinned_artifact(&conn, "sbx-pinned", &v7(1_300).to_string(), None);

    let repair = list_legacy_reupload_candidates(&conn, 5_000, 10).unwrap();
    assert_eq!(
        repair,
        vec!["sbx-legacy-a".to_string(), "sbx-legacy-b".to_string()]
    );

    let capped = list_legacy_reupload_candidates(&conn, 5_000, 1).unwrap();
    assert_eq!(capped, vec!["sbx-legacy-a".to_string()]);

    let evict = candidate_ids(list_candidates(&conn, 5_000, None, 10).unwrap());
    assert_eq!(
        evict,
        vec!["sbx-done".to_string(), "sbx-pinned".to_string()]
    );
}

#[tokio::test]
async fn evict_tick_below_engage_returns_before_candidate_scan() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-cold", "suspended", 1_000);
    insert_sandbox(&conn, "sbx-malformed", "suspended", 1_100);
    conn.execute(
        "UPDATE sandboxes SET updated_at = 'not-an-epoch' WHERE id = 'sbx-malformed'",
        [],
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);
    let cfg = EvictionConfig {
        enabled: true,
        engage_fraction: 0.50,
        target_fraction: 0.40,
        min_idle: std::time::Duration::from_secs(0),
        interval: std::time::Duration::from_secs(60),
        max_per_tick: 10,
        reupload_max_per_tick: 5,
    };
    let disk = crate::disk_usage::DiskUsage {
        used_bytes: 30,
        total_bytes: 100,
    };
    let mut scan_state = CandidateScanState {
        cursor: Some(CandidateCursor {
            idle_witness_secs: 1_000,
            sandbox_id: "sbx-previous".to_string(),
        }),
        ..Default::default()
    };

    let report = evict_tick(&service, cfg, Some(disk), &mut scan_state)
        .await
        .unwrap();

    assert_eq!(report.candidates, 0);
    assert_eq!(report.evicted, 0);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.malformed_updated_at, 1);
    assert_eq!(report.resident_rows, 2);
    assert_eq!(
        scan_state.cursor, None,
        "closed gate must restart oldest-first"
    );
}

#[test]
fn resident_rows_count_every_state() {
    let conn = open_test_db();
    insert_sandbox(&conn, "a-suspended", "suspended", 1_000);
    insert_sandbox(&conn, "b-started", "started", 1_000);
    insert_sandbox(&conn, "c-deleting", "deleting", 1_000);

    assert_eq!(count_resident_rows(&conn).unwrap(), 3);
}

#[tokio::test]
async fn evict_tick_without_disk_usage_fails_without_scanning() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-cold", "suspended", 1_000);
    let service = SandboxManagerService::for_test(conn);
    let cfg = EvictionConfig {
        enabled: true,
        engage_fraction: 0.0,
        target_fraction: 0.0,
        min_idle: std::time::Duration::from_secs(0),
        interval: std::time::Duration::from_secs(60),
        max_per_tick: 10,
        reupload_max_per_tick: 5,
    };
    let cursor = CandidateCursor {
        idle_witness_secs: 1_000,
        sandbox_id: "sbx-previous".to_string(),
    };
    let mut scan_state = CandidateScanState {
        cursor: Some(cursor.clone()),
        ..Default::default()
    };

    let error = evict_tick(&service, cfg, None, &mut scan_state)
        .await
        .unwrap_err();

    assert!(error.contains("statvfs"), "{error}");
    assert_eq!(scan_state.cursor, Some(cursor), "nothing was scanned");
}

#[tokio::test]
async fn evict_tick_does_not_connect_when_all_candidates_are_backed_off() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-backed-off", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-backed-off", &v7(1_000).to_string(), Some("123"));
    let service = SandboxManagerService::for_test(conn);
    let cfg = EvictionConfig {
        enabled: true,
        engage_fraction: 0.50,
        target_fraction: 0.40,
        min_idle: std::time::Duration::from_secs(0),
        interval: std::time::Duration::from_secs(60),
        max_per_tick: 10,
        reupload_max_per_tick: 5,
    };
    let disk = crate::disk_usage::DiskUsage {
        used_bytes: 80,
        total_bytes: 100,
    };
    let mut scan_state = CandidateScanState::default();
    scan_state.back_off("sbx-backed-off", Instant::now());

    let report = evict_tick(&service, cfg, Some(disk), &mut scan_state)
        .await
        .unwrap();

    assert_eq!(report.candidates, 1);
    assert_eq!(report.evicted, 0);
    assert_eq!(report.skipped, 1);
}

fn insert_pinned_artifact(
    conn: &Connection,
    sandbox_id: &str,
    artifact_id: &str,
    backup_completed_at: Option<&str>,
) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at
         ) VALUES (?1, ?2, 'full', 'suspend', NULL, ?1, '0', ?3)",
        params![artifact_id, sandbox_id, backup_completed_at],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?2",
        params![artifact_id, sandbox_id],
    )
    .unwrap();
}

/// Pinned sandboxes yield identity evidence (id + kind + marker)
/// and never the job journal; unpinned sandboxes yield the newest
/// job regardless of status (a failed newest upload must still
/// count so the failed-upload trap can fire), with its creation
/// time for the recency witness.
#[test]
fn gather_evidence_distinguishes_pinned_and_unpinned() {
    let conn = open_test_db();

    // Pinned, marker set — and a decoy newer job that must be
    // ignored for pinned rows.
    insert_sandbox(&conn, "sbx-pin", "suspended", 1_000);
    let art = "01939e3e-aaaa-7000-8000-000000000001";
    insert_pinned_artifact(&conn, "sbx-pin", art, Some("123"));
    insert_upload_job(
        &conn,
        "j-decoy",
        "sbx-pin",
        "completed",
        SnapshotId::from_uuid(v7(9_000)),
        "2026-01-03 00:00:00",
    );
    let (evidence, updated_at) = gather_local_evidence(&conn, "sbx-pin").unwrap().unwrap();
    assert_eq!(updated_at, 1_000);
    match evidence {
        LocalEvidence::Pinned(p) => {
            assert_eq!(p.artifact_id.to_string(), art);
            assert!(p.backup_marker);
        }
        other => panic!("expected pinned evidence, got {other:?}"),
    }

    // Pin pointing at a missing artifact row.
    insert_sandbox(&conn, "sbx-dangling", "suspended", 1_000);
    conn.execute(
        "UPDATE sandboxes SET current_local_artifact_id =
             '01939e3e-bbbb-7000-8000-000000000001',
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = 'sbx-dangling'",
        [],
    )
    .unwrap();
    let (evidence, _) = gather_local_evidence(&conn, "sbx-dangling")
        .unwrap()
        .unwrap();
    assert_eq!(evidence, LocalEvidence::PinnedRowMissing);

    // Unpinned: newest job wins regardless of status.
    insert_sandbox(&conn, "sbx-legacy", "suspended", 1_000);
    let older = v7(1_000);
    let newest_failed = v7(2_000);
    insert_upload_job(
        &conn,
        "j-1",
        "sbx-legacy",
        "completed",
        SnapshotId::from_uuid(older),
        "2026-01-01 00:00:00",
    );
    insert_upload_job(
        &conn,
        "j-2",
        "sbx-legacy",
        "failed",
        SnapshotId::from_uuid(newest_failed),
        "2026-01-02 00:00:00",
    );
    let (evidence, _) = gather_local_evidence(&conn, "sbx-legacy").unwrap().unwrap();
    match evidence {
        LocalEvidence::Unpinned {
            latest_job: Some(job),
        } => {
            assert_eq!(job.snapshot_id, SnapshotId::from_uuid(newest_failed));
            // 2026-01-02 00:00:00 UTC.
            assert_eq!(job.created_at_secs, 1_767_312_000);
        }
        other => panic!("expected unpinned evidence with a job, got {other:?}"),
    }

    // Vanished sandbox.
    assert!(gather_local_evidence(&conn, "sbx-none").unwrap().is_none());

    // A malformed upload timestamp is absent proof, not Unix epoch zero.
    // Low but valid sandbox timestamps must not make that fabricated age
    // satisfy the legacy recency witness for a matching cluster snapshot.
    insert_sandbox(&conn, "sbx-invalid-job-time", "suspended", 1_000);
    insert_upload_job(
        &conn,
        "j-invalid-time",
        "sbx-invalid-job-time",
        "completed",
        SnapshotId::from_uuid(older),
        "invalid-time",
    );
    for timestamp in ["invalid-time", "-1", "99999999999999junk", "1.5"] {
        conn.execute(
            "UPDATE jobs SET created_at = ?1 WHERE job_id = 'j-invalid-time'",
            [timestamp],
        )
        .unwrap();
        let (evidence, updated_at) = gather_local_evidence(&conn, "sbx-invalid-job-time")
            .unwrap()
            .unwrap();
        assert_eq!(
            decide(Some(SnapshotId::from_uuid(older)), &evidence, updated_at),
            PreDecision::Skip(SkipReason::UnknownLocalGeneration),
            "{timestamp}"
        );
        assert_eq!(suspend_time_witness(&evidence), None, "{timestamp}");
    }
}

#[derive(Clone)]
struct BackupProofServer {
    requests: std::sync::Arc<
        std::sync::Mutex<Vec<pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest>>,
    >,
    reply: std::sync::Arc<
        std::sync::Mutex<
            Result<
                pplx_space_node_api_client::proto::GetArtifactBackupLocationResponse,
                tonic::Code,
            >,
        >,
    >,
}

impl tonic::server::NamedService for BackupProofServer {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl
    tonic::server::UnaryService<pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest>
    for BackupProofServer
{
    type Response = pplx_space_node_api_client::proto::GetArtifactBackupLocationResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(
        &mut self,
        request: tonic::Request<
            pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest,
        >,
    ) -> Self::Future {
        self.requests.lock().unwrap().push(request.into_inner());
        std::future::ready(match self.reply.lock().unwrap().clone() {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(code) => Err(tonic::Status::new(code, "synthetic proof failure")),
        })
    }
}

impl<B> tower::Service<http::Request<B>> for BackupProofServer
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        // The suspend-location RPC is placement-scoped and off limits to a
        // former host; proofs go only through readiness queries.
        assert!(request.uri().path().ends_with("/GetArtifactBackupLocation"));
        let service = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(service, request).await)
        })
    }
}

#[tokio::test]
async fn eviction_proofs_use_known_identities_and_fail_closed() {
    use pplx_space_node_api_client::proto::{
        BackupReadinessQuery, GetArtifactBackupLocationRequest, GetArtifactBackupLocationResponse,
        LocalArtifactBackupIdentity, backup_readiness_query::Artifact,
    };
    use std::ffi::OsStr;

    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(OsStr::new("1")),
    )]) {
        return;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mock = BackupProofServer {
        requests: Default::default(),
        reply: std::sync::Arc::new(std::sync::Mutex::new(Err(tonic::Code::Internal))),
    };
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(mock.clone())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
    );
    let mut client =
        pplx_space_node_api_client::cluster::connect(endpoint, "synthetic-proof-token")
            .await
            .unwrap();
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();

    for marker in [true, false] {
        assert_eq!(
            ready_unpinned_suspend_generation(&mut client, &sandbox_id, &pinned(marker)).await,
            Ok(None)
        );
    }
    assert!(
        mock.requests.lock().unwrap().is_empty(),
        "pinned caches must skip the latest-suspend query"
    );

    // The identity rides only inside `readiness`, so a server that predates
    // it sees an empty artifact id. Only a bare acknowledgement is proof.
    let readiness = |artifact| GetArtifactBackupLocationRequest {
        sandbox_id: sandbox_id.clone(),
        readiness: Some(BackupReadinessQuery {
            artifact: Some(artifact),
        }),
        ..Default::default()
    };
    let bare = |ready| GetArtifactBackupLocationResponse {
        ready,
        ..Default::default()
    };
    let location = GetArtifactBackupLocationResponse {
        backup_attempt_id: SnapshotId::new().to_string(),
        storage_uri: "s3://synthetic-backups/attempt".to_string(),
        ..Default::default()
    };
    for (reply, expected) in [
        (Ok(bare(Some(true))), Ok(true)),
        (Ok(bare(Some(false))), Ok(false)),
        (Ok(bare(None)), Err(SkipReason::RpcError)),
        (Ok(location.clone()), Err(SkipReason::RpcError)),
        (
            Ok(GetArtifactBackupLocationResponse {
                ready: Some(true),
                ..location.clone()
            }),
            Err(SkipReason::RpcError),
        ),
        // An older server rejects the empty artifact id; an even older one
        // lacks the RPC. Neither retries as a location read.
        (Err(tonic::Code::InvalidArgument), Err(SkipReason::RpcError)),
        (Err(tonic::Code::Unimplemented), Err(SkipReason::RpcError)),
        (
            Err(tonic::Code::Unavailable),
            Err(SkipReason::RpcTransportError),
        ),
    ] {
        *mock.reply.lock().unwrap() = reply;
        let conn = open_test_db();
        let artifact_id = LocalArtifactId::new();
        insert_sandbox(&conn, &sandbox_id, "suspended", 0);
        insert_pinned_artifact(&conn, &sandbox_id, &artifact_id.to_string(), None);
        let service = SandboxManagerService::for_test(conn);
        let pin = PinnedArtifact {
            artifact_id,
            artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
            backup_marker: false,
        };
        let proof = prove_pinned_artifact(&mut client, &service, &sandbox_id, &pin).await;
        assert_eq!(proof, expected);
        let (evidence, _) = {
            let conn = service.db_conn().unwrap();
            gather_local_evidence(&conn, &sandbox_id).unwrap().unwrap()
        };
        assert!(
            matches!(evidence, LocalEvidence::Pinned(PinnedArtifact { backup_marker, .. }) if backup_marker == (expected == Ok(true)))
        );
        assert_eq!(
            mock.requests.lock().unwrap().pop().unwrap(),
            readiness(Artifact::LocalArtifact(LocalArtifactBackupIdentity {
                local_artifact_id: artifact_id.to_string(),
                artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
            }))
        );

        let generation = SnapshotId::from_uuid(v7(1_000));
        let unpinned_cache = unpinned(Some((generation.as_uuid(), 0)));
        assert_eq!(
            ready_unpinned_suspend_generation(&mut client, &sandbox_id, &unpinned_cache).await,
            expected.map(|ready| ready.then_some(generation))
        );
        assert_eq!(
            mock.requests.lock().unwrap().pop().unwrap(),
            readiness(Artifact::LatestReadySuspendSnapshotId(
                generation.to_string()
            ))
        );
    }
    drop(client);
    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("proof server did not stop")
        .unwrap()
        .unwrap();
}

#[test]
fn gather_evidence_rejects_malformed_updated_at() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-malformed", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-malformed", &v7(1_000).to_string(), Some("123"));
    conn.execute(
        "UPDATE sandboxes SET updated_at = 'not-an-epoch' WHERE id = 'sbx-malformed'",
        [],
    )
    .unwrap();

    let error = gather_local_evidence(&conn, "sbx-malformed").unwrap_err();

    assert!(matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, _)
    ));
}

/// The version-bound claim used by the cluster-deleted eviction RPC:
/// its owner/pool/quiescence proof was checked against a specific
/// row version, and any mutation in the gap (every write bumps
/// `updated_at`) must fail the claim rather than delete.
#[test]
fn claim_cas_binds_to_the_checked_updated_at() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-version", "suspended", 1_000);
    assert!(
        !crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-version", None, Some("999"),)
            .unwrap(),
        "a stale updated_at must fail the claim"
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = 'sbx-version'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "suspended", "failed claim must not mutate the row");
    assert!(
        crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-version", None, Some("1000"),)
            .unwrap(),
        "the checked version must claim"
    );
}

#[test]
fn claim_cas_rejects_non_suspended_rows() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-claim", "suspended", 1_000);
    assert!(
        crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-claim", None, None).unwrap()
    );
    // Second claim: state is now 'deleting' → CAS fails.
    assert!(
        !crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-claim", None, None).unwrap()
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = 'sbx-claim'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "deleting");
}

/// The claim must re-assert the proven identity: if the pin moved
/// to a different (unproven) artifact between the decision and the
/// claim — a fast resume + re-suspend — nothing may be deleted.
#[test]
fn claim_cas_binds_the_proven_pin() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-bind", "suspended", 1_000);
    let proven = "01939e3e-cccc-7000-8000-000000000001";
    let swapped = "01939e3e-cccc-7000-8000-000000000002";
    insert_pinned_artifact(&conn, "sbx-bind", proven, Some("123"));

    // Simulate resume + re-suspend swapping in a fresh, unproven
    // artifact after the decision proved `proven`.
    insert_pinned_artifact(&conn, "sbx-bind", swapped, None);

    assert!(
        !crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-bind", Some(proven), None)
            .unwrap(),
        "claim must fail when the pin no longer matches the proven artifact"
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = 'sbx-bind'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "suspended", "a failed claim must change nothing");

    // Claiming with the now-current pin succeeds.
    assert!(
        crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-bind", Some(swapped), None)
            .unwrap()
    );
}

/// The unpinned-class claim asserts the row is STILL unpinned: a
/// pin appearing between decision and claim (a modern suspend
/// happened) invalidates the legacy equality proof.
#[test]
fn claim_cas_rejects_unpinned_claim_when_pin_appeared() {
    let conn = open_test_db();
    insert_sandbox(&conn, "sbx-was-legacy", "suspended", 1_000);
    insert_pinned_artifact(
        &conn,
        "sbx-was-legacy",
        "01939e3e-dddd-7000-8000-000000000001",
        None,
    );
    assert!(
        !crate::db::claim_suspended_sandbox_for_eviction(&conn, "sbx-was-legacy", None, None)
            .unwrap(),
        "unpinned-class claim must fail once a pin exists"
    );
}

/// A pin naming an artifact row that belongs to a *different* sandbox is an
/// inconsistent state, and the borrower must not inherit the owner's proof
/// of durability.
///
/// Read by `local_artifact_id` alone this returns `Pinned` carrying the
/// owner's `backup_completed_at`, and `decide` then evicts the borrower's
/// bytes on somebody else's backup. Keyed on `(local_artifact_id,
/// sandbox_id)` the lookup misses, which is `PinnedRowMissing` — the state
/// `decide` already refuses to evict on.
///
/// The second assertion is what stops this passing vacuously: the owner,
/// whose pin does match its own row, still reads as `Pinned` with the
/// stamp, so the ownership conjunct is narrowing the lookup rather than
/// breaking it.
#[test]
fn a_pin_owned_by_another_sandbox_is_not_evidence_of_this_sandbox_backup() {
    let conn = open_test_db();
    let artifact = "01939e3e-cccc-7000-8000-000000000001";
    insert_sandbox(&conn, "sbx-owner", "suspended", 1_000);
    insert_sandbox(&conn, "sbx-borrower", "suspended", 1_000);
    insert_pinned_artifact(&conn, "sbx-owner", artifact, Some("2026-09-19T00:00:00Z"));
    conn.execute(
        "UPDATE sandboxes SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = 'sbx-borrower'",
        params![artifact],
    )
    .unwrap();

    let (borrower, _) = gather_local_evidence(&conn, "sbx-borrower")
        .unwrap()
        .unwrap();
    assert_eq!(
        borrower,
        LocalEvidence::PinnedRowMissing,
        "the borrower must not read the owner's backup marker"
    );

    let (owner, _) = gather_local_evidence(&conn, "sbx-owner").unwrap().unwrap();
    assert!(
        matches!(
            owner,
            LocalEvidence::Pinned(PinnedArtifact {
                backup_marker: true,
                ..
            })
        ),
        "the owner's own pin still resolves: {owner:?}"
    );
}

/// Environment of a node wired to a cluster API through the given endpoint and
/// static token, with the token file pointing at a missing test-owned path, so
/// any cluster-api connect stops at the token probe before dialing.
fn cluster_api_env<'a>(
    endpoint: &'a str,
    token: &'a str,
    token_path: &'a Path,
) -> Vec<(&'static str, Option<&'a std::ffi::OsStr>)> {
    vec![
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV,
            Some(std::ffi::OsStr::new(endpoint)),
        ),
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV_LEGACY,
            None,
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_INTERNAL_TOKEN_ENV,
            Some(std::ffi::OsStr::new(token)),
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_CLUSTER_API_TOKEN_PATH_ENV,
            Some(token_path.as_os_str()),
        ),
    ]
}

/// Refusals produced by the real reupload handler, read through the real
/// legacy-repair classifier: a sandbox that vanished or left `Suspended` is
/// skipped, while a node that cannot restore snapshots is a retryable failure.
#[tokio::test]
async fn legacy_reupload_refusals_classify_as_state_skips_or_retryable_failures() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        "http://127.0.0.1:9",
        "characterization-token",
        &token_path,
    )) {
        return;
    }
    use crate::state::SandboxState;
    use pplx_space_common_core::types::SandboxId;

    let service = SandboxManagerService::for_test(open_test_db());
    let unknown = service
        .reupload_legacy_sandbox_for_eviction(&SandboxId::new().to_string())
        .await
        .unwrap_err();
    assert_eq!(
        classify_reupload_failure(&unknown),
        Ok(LegacyRepairSkipReason::StateChanged),
        "{unknown}"
    );

    // Not covered: a `Suspended` row passes state admission, and its artifact
    // checks then read the fixed sandbox cache root, which this fixture cannot
    // isolate.
    for state in SandboxState::all() {
        if state == SandboxState::Suspended {
            continue;
        }
        let conn = open_test_db();
        let id = SandboxId::new().to_string();
        insert_sandbox(&conn, &id, state.as_str(), 0);
        let service = SandboxManagerService::for_test(conn);

        let refused = service
            .reupload_legacy_sandbox_for_eviction(&id)
            .await
            .unwrap_err();

        assert_eq!(
            classify_reupload_failure(&refused),
            Ok(LegacyRepairSkipReason::StateChanged),
            "{state}: {refused}"
        );
    }

    // The snapshot-restore support check precedes the state check, so even a
    // non-suspended row is retried rather than skipped on such a node.
    let conn = open_test_db();
    let id = SandboxId::new().to_string();
    insert_sandbox(&conn, &id, SandboxState::Restoring.as_str(), 0);
    let service = SandboxManagerService::new_with_config(
        conn,
        crate::service::SandboxManagerServiceOptions::for_test(),
        crate::config::SandboxConfig {
            hugepages: true,
            ..Default::default()
        },
    );
    let refused = service
        .reupload_legacy_sandbox_for_eviction(&id)
        .await
        .unwrap_err();
    assert_eq!(
        classify_reupload_failure(&refused),
        Err(LegacyRepairFailureReason::Submission),
        "{refused}"
    );
}

/// Empty cluster-api wiring is operator-fixable, so eviction retries the
/// reupload instead of skipping the sandbox.
async fn assert_missing_wiring_reupload_is_retried() {
    let service = SandboxManagerService::for_test(open_test_db());
    let refused = service
        .reupload_legacy_sandbox_for_eviction(
            &pplx_space_common_core::types::SandboxId::new().to_string(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        classify_reupload_failure(&refused),
        Err(LegacyRepairFailureReason::Submission),
        "{refused}"
    );
}

#[tokio::test]
async fn legacy_reupload_retries_when_the_cluster_api_token_is_empty() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        "http://127.0.0.1:9",
        "",
        &token_path,
    )) {
        return;
    }
    assert_missing_wiring_reupload_is_retried().await;
}

#[tokio::test]
async fn legacy_reupload_retries_when_the_cluster_api_endpoint_is_empty() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        "",
        "characterization-token",
        &token_path,
    )) {
        return;
    }
    assert_missing_wiring_reupload_is_retried().await;
}
