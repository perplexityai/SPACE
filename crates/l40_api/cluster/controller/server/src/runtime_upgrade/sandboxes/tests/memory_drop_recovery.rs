//! Policy-fence, mid-lease shift, and post-commit recovery tests for the
//! `sandbox_memory_drops` driver.

use pplx_space_runtime_upgrade_campaign_store::{CampaignKind, CampaignRecord, PhaseChange};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::suspended_environment_pins::{self, SandboxPinState};
use pplx_space_snapshot_model::ArtifactKind;

use super::super::memory_drops::POLICY_STOPPED_DROPPING_MEMORY;
use super::memory_drops::{
    FakeDropDispatcher, PinSideEffect, ScriptedDrop, drop_reconciler,
    insert_suspended_pinned_with_id, insert_suspended_pinned_with_published, memory_drop_seed,
    repin_to_snapshot, suspended_on_source,
};
use super::*;

#[derive(Clone)]
struct AllowSourceDuringRetirementRead {
    pool: PgPool,
    inner: FakeMemoryRetirement,
}

impl MemoryRetirementSource for AllowSourceDuringRetirementRead {
    async fn memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> Result<MemoryRetirement, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        pplx_space_runtime_environment_policy_store::set_environment_state(
            &mut tx,
            environment,
            RuntimeEnvironmentState::Allowed,
            &change(),
        )
        .await
        .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        self.inner.memory_retirement(environment).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn mid_lease_policy_change_releases_without_dropping_and_ineligible_retries_stay_bounded_while_failed_spends_budget()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;

    // Check 1: if memory retirement clears after leasing and before `upgrade`
    // calls the dispatcher, `upgrade` releases back to `pending` without
    // touching memory or spending an attempt, so re-retiring drives it on the
    // next tick without waiting for another scan pass.
    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDropDispatcher::new(&db.pool).with_retirement(retirement.clone());
    let driver =
        SandboxMemoryDropDriver::new(db.pool.clone(), K8S, dispatcher.clone(), retirement.clone());
    let mut seed = memory_drop_seed();
    seed.failure_budget = 1;
    let campaign_id = insert_campaign(&db.pool, seed).await?;
    let policy_flipped = suspended_on_source(&db.pool).await?;
    record(&db.pool, campaign_id, &[policy_flipped]).await?;
    let CampaignRecord { campaign, .. } = store::get_record(&db.pool, campaign_id)
        .await?
        .expect("campaign exists");
    let leased = store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    retirement.unretire(source());
    let outcome = driver.upgrade(&campaign, &leased[0]).await;
    assert_eq!(
        outcome,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_DROPPING_MEMORY))
    );
    assert!(dispatcher.calls().is_empty());
    store::finish_item(&db.pool, &leased[0], &outcome).await?;
    assert_eq!(
        item(&db.pool, campaign_id, policy_flipped).await?,
        (
            "pending".to_owned(),
            Some(POLICY_STOPPED_DROPPING_MEMORY.to_owned()),
            0
        ),
        "mid-lease policy hold releases back to pending with zero attempts spent"
    );
    retirement.retire(source());

    // Check 1b: if an operator flips policy (`source()` -> `Allowed`) while
    // `memory_retirement` is in flight (after `inspect_pin` already read the
    // deprecated policy), `upgrade` re-reads `Inputs` after `memory_retirement`
    // and releases without calling `drop_memory`.
    let leased_race =
        store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    let racing_driver = SandboxMemoryDropDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        AllowSourceDuringRetirementRead {
            pool: db.pool.clone(),
            inner: retirement.clone(),
        },
    );
    let race_outcome = racing_driver.upgrade(&campaign, &leased_race[0]).await;
    assert_eq!(
        race_outcome,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_DROPPING_MEMORY))
    );
    assert!(dispatcher.calls().is_empty());
    store::finish_item(&db.pool, &leased_race[0], &race_outcome).await?;
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_environment_state(
        &mut tx,
        source(),
        RuntimeEnvironmentState::Deprecated,
        &change(),
    )
    .await?;
    tx.commit().await?;

    // Check 1c: a commit-time policy/retirement refusal from `drop_memory`
    // (`SandboxMemoryDrop::Held`) also releases without spending an attempt.
    dispatcher.script(
        policy_flipped,
        vec![ScriptedDrop::Held("memory unretired at commit".to_owned())],
    );
    let leased_commit_hold =
        store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    let commit_hold_outcome = driver.upgrade(&campaign, &leased_commit_hold[0]).await;
    assert_eq!(
        commit_hold_outcome,
        ItemOutcome::Release(ItemError::new("memory unretired at commit"))
    );
    store::finish_item(&db.pool, &leased_commit_hold[0], &commit_hold_outcome).await?;
    assert_eq!(
        item(&db.pool, campaign_id, policy_flipped).await?,
        (
            "pending".to_owned(),
            Some("memory unretired at commit".to_owned()),
            0
        ),
        "commit-time Held outcome releases back to pending with zero attempts spent"
    );

    // Check 2: `Ineligible` skips without spending `failure_budget = 1`,
    // re-queues across passes up to `MAX_ATTEMPTS`, and then lets the campaign
    // complete with `policy_flipped` `done` (`attempts == 1`) and `ineligible`
    // terminally `skipped`.
    let ineligible = suspended_on_source(&db.pool).await?;
    dispatcher.script(
        ineligible,
        (0..10)
            .map(|_| ScriptedDrop::Ineligible("encrypted checkpoint".to_owned()))
            .collect(),
    );
    let reconciler = drop_reconciler(&db.pool, dispatcher.clone(), retirement.clone(), 100);
    tick_until_finished(
        &reconciler,
        &db.pool,
        campaign_id,
        2 * MAX_ATTEMPTS as usize,
    )
    .await?;
    assert_eq!(campaign_phase(&db.pool, campaign_id).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign_id, policy_flipped).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign_id, ineligible).await?,
        (
            "skipped".to_owned(),
            Some("encrypted checkpoint".to_owned()),
            MAX_ATTEMPTS as i64
        )
    );
    repin_to_snapshot(&db.pool, ineligible, "full", target()).await?;

    // Check 3: an unbound pre-metadata checkpoint that was `skipped` AND scanned
    // while `legacy_unbound_allowed = true` (`attribute` -> `None`) isn't
    // retired into `RETIRED_ATTEMPTS` via `elsewhere`, so flipping
    // `legacy_unbound_allowed = false` reopens it and drops its memory.
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            true,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }
    let unbound_reopened = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Nothing,
        None,
    )
    .await?;
    let keep_open = suspended_on_source(&db.pool).await?;
    dispatcher.script(
        keep_open,
        vec![ScriptedDrop::Sealed("hold open".to_owned())],
    );
    let reopen_campaign = insert_campaign(&db.pool, memory_drop_seed()).await?;
    record(&db.pool, reopen_campaign, &[unbound_reopened]).await?;

    // Tick 1 leases `unbound_reopened`, skips it (`legacy_unbound_allowed = true`
    // -> `attribute` returns `None`), and scans `unbound_reopened` while
    // non-matching; `keep_open` releases on `Sealed` so `reopen_campaign` stays
    // `rolling`.
    tick(&reconciler).await;
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, reopen_campaign, unbound_reopened).await?,
        (
            "skipped".to_owned(),
            Some(NO_LONGER_ON_SOURCE.to_owned()),
            1
        ),
        "scanning a skipped unbound checkpoint while unattributed leaves `attempts = 1` instead of retiring it"
    );

    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            false,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }
    tick_until_finished(&reconciler, &db.pool, reopen_campaign, 5).await?;
    assert_eq!(
        campaign_phase(&db.pool, reopen_campaign).await?,
        "completed"
    );
    assert_eq!(
        item(&db.pool, reopen_campaign, unbound_reopened).await?,
        ("done".to_owned(), None, 2),
        "flipping `legacy_unbound_allowed = false` reopens the skipped unbound item and drops it"
    );

    // Check 4: `DispatchError::Failed` marks the item `failed` immediately and
    // an uncommitted `DispatchError::Retryable` still fails after `MAX_ATTEMPTS`,
    // auto-pausing a campaign once `failure_budget` is reached.
    let mut fail_seed = memory_drop_seed();
    fail_seed.failure_budget = 2;
    let fail_campaign = insert_campaign(&db.pool, fail_seed).await?;
    let broken = suspended_on_source(&db.pool).await?;
    let exhausted_retryable = suspended_on_source(&db.pool).await?;
    let retryable_moved_off_source = suspended_on_source(&db.pool).await?;
    let retryable_into_restoring = suspended_on_source(&db.pool).await?;
    dispatcher.script(
        broken,
        vec![ScriptedDrop::Fail(DispatchError::Failed(
            "corrupt disk manifest".to_owned(),
        ))],
    );
    dispatcher.script(
        exhausted_retryable,
        (0..MAX_ATTEMPTS)
            .map(|_| ScriptedDrop::Fail(DispatchError::Retryable("s3 timeout".to_owned())))
            .collect(),
    );
    dispatcher.script(
        retryable_moved_off_source,
        vec![ScriptedDrop::Fail(DispatchError::Retryable(
            "lost reply during wake".to_owned(),
        ))],
    );
    dispatcher.before_answer(
        retryable_moved_off_source,
        PinSideEffect::RotateFullPin(target()),
    );
    dispatcher.script(
        retryable_into_restoring,
        vec![ScriptedDrop::Fail(DispatchError::Retryable(
            "lost reply during restore".to_owned(),
        ))],
    );
    dispatcher.before_answer(
        retryable_into_restoring,
        PinSideEffect::SetLifecycle("restoring"),
    );
    for _ in 0..=MAX_ATTEMPTS {
        tick(&reconciler).await;
    }
    assert_eq!(
        item(&db.pool, fail_campaign, broken).await?,
        (
            "failed".to_owned(),
            Some("corrupt disk manifest".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, fail_campaign, exhausted_retryable).await?,
        (
            "failed".to_owned(),
            Some(format!(
                "s3 timeout (gave up after {MAX_ATTEMPTS} attempts)"
            )),
            MAX_ATTEMPTS as i64
        )
    );
    assert_eq!(
        item(&db.pool, fail_campaign, retryable_moved_off_source).await?,
        (
            "skipped".to_owned(),
            Some(NO_LONGER_ON_SOURCE.to_owned()),
            1
        ),
        "a retryable dispatch error after moving off source skips instead of burning retries or budget"
    );
    assert_eq!(
        item(&db.pool, fail_campaign, retryable_into_restoring).await?,
        (
            "pending".to_owned(),
            Some("the sandbox is restoring; retrying once it settles".to_owned()),
            0
        ),
        "a retryable dispatch error while mid-transition releases with zero attempts instead of failing"
    );
    assert_eq!(campaign_phase(&db.pool, fail_campaign).await?, "paused");
    store::abort_campaign(&db.pool, fail_campaign, &PhaseChange::default()).await?;

    Ok(())
}

async fn certify_second_target(pool: &PgPool) -> TestResult<EnvironmentReference> {
    let second_target = environment(2);
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, upgrade_edges
         )
         VALUES ($1, 'firecracker', '1.18.0', '{2}', '6.1.155', 'firecracker-snapshot', '13.0.0', '{}')"
    ))
    .bind(second_target.to_string())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments
         SET upgrade_edges = $2
         WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(vec![target().to_string(), second_target.to_string()])
    .execute(pool)
    .await?;
    Ok(second_target)
}

#[tokio::test(flavor = "current_thread")]
async fn commit_time_source_and_target_fence_blocks_default_flip_and_unbound_attribution_shift()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let second_target = certify_second_target(&db.pool).await?;
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            false,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDropDispatcher::new(&db.pool).with_retirement(retirement.clone());
    let driver =
        SandboxMemoryDropDriver::new(db.pool.clone(), K8S, dispatcher.clone(), retirement.clone());
    let campaign_id = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let CampaignRecord { campaign, .. } = store::get_record(&db.pool, campaign_id)
        .await?
        .expect("campaign exists");

    // Case 1: `source()` has certified upgrade edges to both `target()` and
    // `second_target`. If `default_environment` flips from `target()` to
    // `second_target` after `upgrade`'s pre-dispatch check and before
    // `drop_memory`'s final re-pin, sealing with `(source(), target())` sees
    // `ColdBootUpgrade { to: second_target } != target()`, rolls back without
    // projecting, and returns `Held` (`Release`, zero attempts spent).
    let target_flipped = suspended_on_source(&db.pool).await?;
    record(&db.pool, campaign_id, &[target_flipped]).await?;
    dispatcher.before_answer(
        target_flipped,
        PinSideEffect::SetDefaultEnvironment(second_target),
    );
    let leased = store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    let held_outcome = driver.upgrade(&campaign, &leased[0]).await;
    assert_eq!(
        held_outcome,
        ItemOutcome::Release(ItemError::new(
            "policy no longer permits dropping memory for campaign target"
        ))
    );
    store::finish_item(&db.pool, &leased[0], &held_outcome).await?;
    assert_eq!(
        item(&db.pool, campaign_id, target_flipped).await?,
        (
            "pending".to_owned(),
            Some("policy no longer permits dropping memory for campaign target".to_owned()),
            0
        )
    );
    assert!(
        matches!(
            suspended_environment_pins::pin_state(
                &db.pool,
                K8S,
                SandboxId::from_uuid(target_flipped),
            )
            .await?,
            SandboxPinState::SuspendedPinned {
                kind: ArtifactKind::Full,
                ..
            }
        ),
        "commit-time target fence leaves the full checkpoint untouched when default_environment flipped to T2"
    );

    // Restore `default_environment` back to `target()`.
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_default(&mut tx, target(), &change())
            .await?;
        tx.commit().await?;
    }

    // Case 2: a pre-metadata unbound checkpoint attributed to `source()` under
    // `legacy_unbound_allowed = Some(false)` loses source attribution if
    // `legacy_unbound_allowed` flips to `true` before `drop_memory` seals;
    // sealing with `source()` sees `attributed_checkpoint != Some(source())`,
    // rolls back without projecting, and returns `Moved` (`Skip`).
    let unbound_flipped = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Nothing,
        None,
    )
    .await?;
    record(&db.pool, campaign_id, &[unbound_flipped]).await?;
    dispatcher.before_answer(
        unbound_flipped,
        PinSideEffect::SetLegacyUnboundAllowed(true),
    );
    let leased_two = store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    for leased_item in &leased_two {
        let outcome = driver.upgrade(&campaign, leased_item).await;
        store::finish_item(&db.pool, leased_item, &outcome).await?;
    }
    assert_eq!(
        item(&db.pool, campaign_id, target_flipped).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign_id, unbound_flipped).await?,
        (
            "skipped".to_owned(),
            Some("checkpoint no longer attributes to campaign source".to_owned()),
            1
        )
    );
    assert!(
        matches!(
            suspended_environment_pins::pin_state(
                &db.pool,
                K8S,
                SandboxId::from_uuid(unbound_flipped),
            )
            .await?,
            SandboxPinState::SuspendedPinned {
                kind: ArtifactKind::Full,
                ..
            }
        ),
        "commit-time source attribution fence leaves the unbound full checkpoint untouched"
    );
    for (call_campaign, _, _, call_source, call_target) in dispatcher.full_calls() {
        assert_eq!(
            (call_campaign, call_source, call_target),
            (campaign_id, source(), target()),
            "driver forwards the campaign's id, source, and target to drop_memory"
        );
    }

    // Post-page retirement/policy read ordering: if `legacy_unbound_allowed`
    // flips from `true` to `false` while the post-`candidates_page`
    // `memory_retirement` call is awaiting, `read_retired_memory_plan` reads
    // `Inputs` (`policy` + `catalog`) after `memory_retirement` settles and
    // returns `PlanChanged` rather than letting a short page complete the
    // campaign with the newly attributed pre-metadata unbound sandbox omitted.
    store::abort_campaign(&db.pool, campaign_id, &PhaseChange::default()).await?;
    sqlx::query(sql!("DELETE FROM snapshot_storage_locations"))
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!("DELETE FROM sandboxes"))
        .execute(&db.pool)
        .await?;
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            true,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }
    let unbound_post_page = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Nothing,
        None,
    )
    .await?;
    let post_page_campaign = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let CampaignRecord {
        campaign: post_page_campaign_row,
        ..
    } = store::get_record(&db.pool, post_page_campaign)
        .await?
        .expect("post_page_campaign exists");

    // Direct driver assertion: Call 1 computes `initial_plan`; Call 2 is
    // `find_subjects`'s pre-page `read_plan`; Call 3 flips
    // `legacy_unbound_allowed` to `false` inside `find_subjects`'s post-page
    // `memory_retirement` read, returning `SubjectScan::PlanChanged` directly.
    let post_page_retirement = FakeMemoryRetirement::retired(source());
    let post_page_driver = SandboxMemoryDropDriver::new(
        db.pool.clone(),
        K8S,
        FakeDropDispatcher::new(&db.pool),
        post_page_retirement.clone(),
    );
    let Plan::Scan(initial_plan) = post_page_driver.plan(&post_page_campaign_row).await? else {
        panic!("expected initial Scan plan");
    };
    post_page_retirement.set_legacy_unbound_on_read(3, db.pool.clone(), false);
    assert_eq!(
        post_page_driver
            .find_subjects(&post_page_campaign_row, initial_plan, None, 100)
            .await?,
        SubjectScan::PlanChanged,
        "policy flip during post-page memory_retirement read yields SubjectScan::PlanChanged directly"
    );

    // Separate reconciler pre-completion recheck: reset `legacy_unbound_allowed`
    // to `true` and flip it on Call 4 (`drive` -> `driver.plan`, `find_subjects`
    // pre-page + post-page, then `drive` pre-completion `driver.plan` re-check).
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            true,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }
    let pre_complete_retirement = FakeMemoryRetirement::retired(source());
    pre_complete_retirement.set_legacy_unbound_on_read(4, db.pool.clone(), false);
    let post_page_dispatcher = FakeDropDispatcher::new(&db.pool);
    let post_page_reconciler = drop_reconciler(
        &db.pool,
        post_page_dispatcher.clone(),
        pre_complete_retirement,
        100,
    );
    tick(&post_page_reconciler).await;
    assert_eq!(
        campaign_phase(&db.pool, post_page_campaign).await?,
        "rolling",
        "policy flip on reconciler pre-completion plan recheck restarts the pass instead of completing"
    );
    tick_until_finished(&post_page_reconciler, &db.pool, post_page_campaign, 5).await?;
    assert_eq!(
        campaign_phase(&db.pool, post_page_campaign).await?,
        "completed"
    );
    assert_eq!(
        item(&db.pool, post_page_campaign, unbound_post_page).await?,
        ("done".to_owned(), None, 1),
        "restarted pass scans under legacy_unbound_allowed = false and drops the unbound sandbox"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn durable_campaign_completion_prevents_second_drop_after_lost_reply_or_lapsed_lease()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let second_target = certify_second_target(&db.pool).await?;

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDropDispatcher::new(&db.pool).with_retirement(retirement.clone());
    let driver =
        SandboxMemoryDropDriver::new(db.pool.clone(), K8S, dispatcher.clone(), retirement.clone());
    let campaign_id = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let CampaignRecord { campaign, .. } = store::get_record(&db.pool, campaign_id)
        .await?
        .expect("campaign exists");

    // Subject 1: `drop_memory` commits the `disk` re-pin of checkpoint A and
    // durable `(campaign_id, sandbox_id)` completion, then before its lost
    // reply returns `Err(DispatchError::Retryable)`, the sandbox wakes and
    // re-suspends onto a brand-new `full` checkpoint B on `source()`.
    // `upgrade` consults `memory_drop_completed` before `inspect_pin`, finishes
    // `Done` on attempt 1, never calls `drop_memory` a second time, and leaves
    // checkpoint B pinned as `Full`.
    let lost_reply_resuspended = suspended_on_source(&db.pool).await?;
    // Subject 2: lease 1 commits `drop_memory` (`Dropped`), then crashes before
    // `finish_item`; while the lease expires, the sandbox wakes and re-suspends
    // onto a brand-new `full` checkpoint C on `source()`.
    let lapsed_resuspended = suspended_on_source(&db.pool).await?;
    // Subject 3: lease 1 commits `drop_memory` (`Dropped`), then crashes before
    // `finish_item`; while the lease expires, the sandbox is deleted.
    let lapsed_deleted = suspended_on_source(&db.pool).await?;

    record(
        &db.pool,
        campaign_id,
        &[lost_reply_resuspended, lapsed_resuspended, lapsed_deleted],
    )
    .await?;
    dispatcher.script(
        lost_reply_resuspended,
        vec![ScriptedDrop::CommitDropThenLoseReply(
            "lost reply after commit".to_owned(),
        )],
    );
    dispatcher.after_commit(
        lost_reply_resuspended,
        PinSideEffect::RotateFullPin(source()),
    );

    let leased = store::lease_items(&db.pool, campaign_id, Duration::from_secs(60), 10).await?;
    assert_eq!(leased.len(), 3);
    for leased_item in &leased {
        let outcome = driver.upgrade(&campaign, leased_item).await;
        assert_eq!(outcome, ItemOutcome::Done);
        if leased_item.subject_id == lost_reply_resuspended {
            store::finish_item(&db.pool, leased_item, &outcome).await?;
        }
    }
    assert_eq!(
        item(&db.pool, campaign_id, lost_reply_resuspended).await?,
        ("done".to_owned(), None, 1),
        "committed drop followed by lost reply + new full pin on source finishes done on attempt 1"
    );
    let checkpoint_b = match suspended_environment_pins::pin_state(
        &db.pool,
        K8S,
        SandboxId::from_uuid(lost_reply_resuspended),
    )
    .await?
    {
        SandboxPinState::SuspendedPinned {
            kind: ArtifactKind::Full,
            artifact_id,
            ..
        } => artifact_id,
        other => panic!("expected full checkpoint B on lost_reply_resuspended, got {other:?}"),
    };

    // Simulate `finish_item` never running for `lapsed_resuspended` and
    // `lapsed_deleted`: expire their leases while `lapsed_resuspended` rotates
    // onto a brand-new `full` checkpoint C on `source()` and `lapsed_deleted`
    // is deleted.
    let checkpoint_c = repin_to_snapshot(&db.pool, lapsed_resuspended, "full", source()).await?;
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations
         WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
    ))
    .bind(lapsed_deleted)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(lapsed_deleted)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND phase = 'in_flight'"
    ))
    .bind(campaign_id)
    .execute(&db.pool)
    .await?;

    // Inject one transient `memory_drop_completed` read error on
    // `lapsed_resuspended` to verify a completion-lookup failure retries
    // without calling `drop_memory`.
    dispatcher.script_completed_errors(
        lapsed_resuspended,
        vec![Some(DispatchError::Retryable(
            "completion store timeout".to_owned(),
        ))],
    );

    let replacement_reconciler =
        drop_reconciler(&db.pool, dispatcher.clone(), retirement.clone(), 100);
    tick(&replacement_reconciler).await;
    assert_eq!(
        item(&db.pool, campaign_id, lapsed_resuspended).await?,
        (
            "pending".to_owned(),
            Some("completion store timeout".to_owned()),
            2
        ),
        "transient memory_drop_completed error retries without dispatching a second drop"
    );
    assert_eq!(
        item(&db.pool, campaign_id, lapsed_deleted).await?,
        ("done".to_owned(), None, 2),
        "durable campaign completion finishes done even after the sandbox was deleted"
    );

    tick_until_finished(&replacement_reconciler, &db.pool, campaign_id, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign_id).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign_id, lapsed_resuspended).await?,
        ("done".to_owned(), None, 3)
    );
    for single_drop in [lost_reply_resuspended, lapsed_resuspended, lapsed_deleted] {
        assert_eq!(
            dispatcher
                .calls()
                .into_iter()
                .filter(|(id, _)| *id == single_drop)
                .count(),
            1,
            "campaign {campaign_id} dropped {single_drop} at most once across lost replies and lapsed leases"
        );
    }
    for (sandbox, expected_pin) in [
        (lost_reply_resuspended, checkpoint_b),
        (lapsed_resuspended, checkpoint_c),
    ] {
        assert!(
            matches!(
                suspended_environment_pins::pin_state(
                    &db.pool,
                    K8S,
                    SandboxId::from_uuid(sandbox),
                )
                .await?,
                SandboxPinState::SuspendedPinned {
                    kind: ArtifactKind::Full,
                    artifact_id,
                    ..
                } if artifact_id == expected_pin
            ),
            "subsequent full checkpoint {expected_pin} on {sandbox} was not dropped a second time by the same campaign"
        );
    }

    // `Reconciler::tick` calls `retain_campaigns` with unfinished campaign IDs:
    // once `campaign_id` completed, one more tick evicts its in-memory tracking.
    tick(&replacement_reconciler).await;
    assert!(
        dispatcher.tracked_completed_campaigns().is_empty(),
        "retain_campaigns prunes finished campaign completion state"
    );

    // Per-campaign scoping: a later campaign (`source() -> second_target`) sees
    // `memory_drop_completed(second_campaign, ...) == false` and drops the new
    // `full` checkpoints B and C.
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_default(&mut tx, second_target, &change())
            .await?;
        tx.commit().await?;
    }
    let mut second_seed = memory_drop_seed();
    second_seed.to = second_target;
    let second_campaign = insert_campaign(&db.pool, second_seed).await?;
    tick_until_finished(&replacement_reconciler, &db.pool, second_campaign, 5).await?;
    assert_eq!(
        campaign_phase(&db.pool, second_campaign).await?,
        "completed"
    );
    for sandbox in [lost_reply_resuspended, lapsed_resuspended] {
        assert_eq!(
            item(&db.pool, second_campaign, sandbox).await?,
            ("done".to_owned(), None, 1),
            "completion is scoped to (campaign_id, sandbox_id), so a subsequent campaign drops new full pins"
        );
    }

    // Commit-fence completion check + final-attempt disk fallback on a third
    // campaign (`source() -> target()`):
    // 1) Replacement Worker 2 reads `memory_drop_completed == false` before
    //    stale Worker 1 commits checkpoint D0 -> `disk` + completion and the
    //    sandbox wakes and suspends a new `full` checkpoint D1 on `source()`.
    //    Worker 2 then inspects D1 and calls `drop_memory`, which checks
    //    `(campaign_id, sandbox_id)` completion under the `sandboxes`
    //    `FOR UPDATE` row lock and returns `AlreadyDropped` without mutating D1.
    // 2) On attempt `MAX_ATTEMPTS`, `CommitDropThenLoseReply` commits the `disk`
    //    re-pin and the post-dispatch `memory_drop_completed` lookup fails with
    //    `DispatchError::Retryable`; `inspect_pin` sees `AlreadyDiskOnSource`
    //    and records `Done` rather than failing the item on `MAX_ATTEMPTS`.
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_default(&mut tx, target(), &change())
            .await?;
        tx.commit().await?;
    }
    let third_campaign_id = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let CampaignRecord {
        campaign: third_campaign,
        ..
    } = store::get_record(&db.pool, third_campaign_id)
        .await?
        .expect("third campaign exists");

    let concurrent_repin_race = suspended_on_source(&db.pool).await?;
    let checkpoint_d0 = match suspended_environment_pins::pin_state(
        &db.pool,
        K8S,
        SandboxId::from_uuid(concurrent_repin_race),
    )
    .await?
    {
        SandboxPinState::SuspendedPinned {
            kind: ArtifactKind::Full,
            artifact_id,
            ..
        } => artifact_id,
        other => panic!("expected initial full checkpoint D0, got {other:?}"),
    };
    let final_attempt_disk = suspended_on_source(&db.pool).await?;
    record(
        &db.pool,
        third_campaign_id,
        &[concurrent_repin_race, final_attempt_disk],
    )
    .await?;

    let (reached_false, resume_after_commit) =
        dispatcher.pause_completed_false_once(concurrent_repin_race);
    let leased_third =
        store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 10).await?;
    let race_lease = leased_third
        .iter()
        .find(|i| i.subject_id == concurrent_repin_race)
        .expect("race item leased");
    let final_disk_lease_1 = leased_third
        .iter()
        .find(|i| i.subject_id == final_attempt_disk)
        .expect("final_attempt_disk item leased");

    let (worker2_outcome, checkpoint_d1) =
        tokio::join!(driver.upgrade(&third_campaign, race_lease), async {
            tokio::time::timeout(Duration::from_secs(10), reached_false.notified())
                .await
                .expect("worker 2 reached pre-dispatch memory_drop_completed == false");
            assert_eq!(
                dispatcher
                    .drop_memory(
                        third_campaign_id,
                        SandboxId::from_uuid(concurrent_repin_race),
                        checkpoint_d0,
                        source(),
                        target(),
                    )
                    .await,
                Ok(SandboxMemoryDrop::Dropped),
                "stale Worker 1 commits checkpoint D0 -> disk and records campaign completion"
            );
            let d1 = repin_to_snapshot(&db.pool, concurrent_repin_race, "full", source())
                .await
                .expect("re-suspending full checkpoint D1");
            resume_after_commit.notify_one();
            d1
        });
    assert_eq!(worker2_outcome, ItemOutcome::Done);
    store::finish_item(&db.pool, race_lease, &worker2_outcome).await?;
    assert_eq!(
        dispatcher.committed_count(concurrent_repin_race),
        1,
        "FOR UPDATE completion check returns AlreadyDropped without committing a second projection"
    );
    assert!(
        matches!(
            suspended_environment_pins::pin_state(
                &db.pool,
                K8S,
                SandboxId::from_uuid(concurrent_repin_race),
            )
            .await?,
            SandboxPinState::SuspendedPinned {
                kind: ArtifactKind::Full,
                artifact_id,
                ..
            } if artifact_id == checkpoint_d1
        ),
        "new full checkpoint D1 stays intact when replacement Worker 2 hits FOR UPDATE completion check"
    );

    // Concurrent commit + delete after `memory_drop_completed == false`: when
    // replacement Worker 2 later sees `PinInspection::OffSource`,
    // `skip_unless_completed` rechecks durable completion and records `Done`
    // rather than `Skipped(NO_LONGER_ON_SOURCE)`.
    let concurrent_delete_race = suspended_on_source(&db.pool).await?;
    let checkpoint_e0 = match suspended_environment_pins::pin_state(
        &db.pool,
        K8S,
        SandboxId::from_uuid(concurrent_delete_race),
    )
    .await?
    {
        SandboxPinState::SuspendedPinned {
            kind: ArtifactKind::Full,
            artifact_id,
            ..
        } => artifact_id,
        other => panic!("expected initial full checkpoint E0, got {other:?}"),
    };
    let rolled_back_drop = suspended_on_source(&db.pool).await?;
    record(
        &db.pool,
        third_campaign_id,
        &[concurrent_delete_race, rolled_back_drop],
    )
    .await?;
    let (delete_reached_false, delete_resume) =
        dispatcher.pause_completed_false_once(concurrent_delete_race);
    let extra_leases =
        store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 10).await?;
    let delete_race_lease = extra_leases
        .iter()
        .find(|i| i.subject_id == concurrent_delete_race)
        .expect("delete_race item leased");
    let rollback_lease_1 = extra_leases
        .iter()
        .find(|i| i.subject_id == rolled_back_drop)
        .expect("rolled_back_drop item leased");
    let (delete_race_outcome, ()) =
        tokio::join!(driver.upgrade(&third_campaign, delete_race_lease), async {
            tokio::time::timeout(Duration::from_secs(10), delete_reached_false.notified())
                .await
                .expect("worker 2 reached pre-dispatch memory_drop_completed == false");
            assert_eq!(
                dispatcher
                    .drop_memory(
                        third_campaign_id,
                        SandboxId::from_uuid(concurrent_delete_race),
                        checkpoint_e0,
                        source(),
                        target(),
                    )
                    .await,
                Ok(SandboxMemoryDrop::Dropped),
                "stale Worker 1 commits E0 -> disk + receipt before sandbox deletion"
            );
            sqlx::query(sql!(
                "DELETE FROM snapshot_storage_locations
                 WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
            ))
            .bind(concurrent_delete_race)
            .execute(&db.pool)
            .await
            .expect("deleting snapshot storage locations before sandbox deletion");
            sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
                .bind(concurrent_delete_race)
                .execute(&db.pool)
                .await
                .expect("deleting sandbox after stale commit");
            delete_resume.notify_one();
        });
    assert_eq!(
        delete_race_outcome,
        ItemOutcome::Done,
        "OffSource rechecks memory_drop_completed and finishes Done instead of Skipped"
    );
    store::finish_item(&db.pool, delete_race_lease, &delete_race_outcome).await?;

    // Transactional receipt rollback: staging both the `disk` re-pin and the
    // completion receipt inside a transaction that rolls back leaves neither
    // visible to `memory_drop_completed` nor `inspect_pin`.
    dispatcher.script(
        rolled_back_drop,
        vec![
            ScriptedDrop::ProjectThenRollback(DispatchError::Retryable(
                "rollback before commit".to_owned(),
            )),
            ScriptedDrop::CommitDrop,
        ],
    );
    let rollback_outcome = driver.upgrade(&third_campaign, rollback_lease_1).await;
    assert!(
        matches!(rollback_outcome, ItemOutcome::Retry(_)),
        "rolled-back projection + receipt records Retry rather than Done: {rollback_outcome:?}"
    );
    assert_eq!(
        dispatcher
            .memory_drop_completed(third_campaign_id, SandboxId::from_uuid(rolled_back_drop))
            .await,
        Ok(false),
        "rolled-back transaction leaves no durable completion receipt"
    );
    store::finish_item(&db.pool, rollback_lease_1, &rollback_outcome).await?;
    let rollback_lease_2 =
        &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
    let committed_after_rollback = driver.upgrade(&third_campaign, rollback_lease_2).await;
    assert_eq!(committed_after_rollback, ItemOutcome::Done);
    store::finish_item(&db.pool, rollback_lease_2, &committed_after_rollback).await?;

    let mut scripted = (1..MAX_ATTEMPTS)
        .map(|attempt| {
            ScriptedDrop::Fail(DispatchError::Retryable(format!(
                "attempt {attempt} timeout"
            )))
        })
        .collect::<Vec<_>>();
    scripted.push(ScriptedDrop::CommitDropThenLoseReply(
        "final attempt lost reply".to_owned(),
    ));
    dispatcher.script(final_attempt_disk, scripted);
    let retry1 = driver.upgrade(&third_campaign, final_disk_lease_1).await;
    store::finish_item(&db.pool, final_disk_lease_1, &retry1).await?;
    for _ in 2..MAX_ATTEMPTS {
        let retry_lease =
            &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
        let retry = driver.upgrade(&third_campaign, retry_lease).await;
        store::finish_item(&db.pool, retry_lease, &retry).await?;
    }
    let final_disk_lease =
        &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
    assert_eq!(final_disk_lease.attempts, MAX_ATTEMPTS);
    // Final attempt: pre-dispatch `memory_drop_completed` succeeds (`None`), then
    // post-commit `memory_drop_completed` fails with `Retryable` (`Some(...)`).
    dispatcher.script_completed_errors(
        final_attempt_disk,
        vec![
            None,
            Some(DispatchError::Retryable(
                "completion store timeout".to_owned(),
            )),
        ],
    );
    let final_outcome = crate::runtime_upgrade::worker::recorded_outcome(
        driver.upgrade(&third_campaign, final_disk_lease).await,
        final_disk_lease.attempts,
    );
    assert_eq!(
        final_outcome,
        ItemOutcome::Done,
        "AlreadyDiskOnSource salvages Done on MAX_ATTEMPTS even if post-commit completion read fails"
    );
    store::finish_item(&db.pool, final_disk_lease, &final_outcome).await?;

    // Symmetric pre-dispatch salvage: if an earlier attempt (`MAX_ATTEMPTS - 1`)
    // committed the `disk` re-pin before its lease lapsed and the pre-dispatch
    // `memory_drop_completed` call fails transiently on `MAX_ATTEMPTS`,
    // `check_completed_or_salvage_disk` sees `AlreadyDiskOnSource` and finishes
    // `Done` without dispatching a second time or failing the item.
    let pre_dispatch_disk = suspended_on_source(&db.pool).await?;
    record(&db.pool, third_campaign_id, &[pre_dispatch_disk]).await?;
    let mut pre_scripted = (1..(MAX_ATTEMPTS - 1))
        .map(|attempt| {
            ScriptedDrop::Fail(DispatchError::Retryable(format!(
                "pre attempt {attempt} timeout"
            )))
        })
        .collect::<Vec<_>>();
    pre_scripted.push(ScriptedDrop::CommitDrop);
    dispatcher.script(pre_dispatch_disk, pre_scripted);
    for _ in 1..(MAX_ATTEMPTS - 1) {
        let retry_lease =
            &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
        let retry = driver.upgrade(&third_campaign, retry_lease).await;
        store::finish_item(&db.pool, retry_lease, &retry).await?;
    }
    let committed_lease_lapsed =
        &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
    assert_eq!(committed_lease_lapsed.attempts, MAX_ATTEMPTS - 1);
    assert_eq!(
        driver
            .upgrade(&third_campaign, committed_lease_lapsed)
            .await,
        ItemOutcome::Done
    );
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(third_campaign_id)
    .bind(pre_dispatch_disk)
    .execute(&db.pool)
    .await?;
    store::redrive_expired(&db.pool, third_campaign_id, MAX_ATTEMPTS).await?;
    let redriven_final =
        &store::lease_items(&db.pool, third_campaign_id, Duration::from_secs(60), 1).await?[0];
    assert_eq!(redriven_final.attempts, MAX_ATTEMPTS);
    dispatcher.script_completed_errors(
        pre_dispatch_disk,
        vec![Some(DispatchError::Retryable(
            "pre-dispatch completion read timeout".to_owned(),
        ))],
    );
    let calls_before_redrive = dispatcher.calls().len();
    let pre_salvaged = crate::runtime_upgrade::worker::recorded_outcome(
        driver.upgrade(&third_campaign, redriven_final).await,
        redriven_final.attempts,
    );
    assert_eq!(
        pre_salvaged,
        ItemOutcome::Done,
        "AlreadyDiskOnSource salvages Done on MAX_ATTEMPTS when pre-dispatch completion read fails"
    );
    assert_eq!(
        dispatcher.calls().len(),
        calls_before_redrive,
        "pre-dispatch AlreadyDiskOnSource salvage never invokes drop_memory"
    );
    store::finish_item(&db.pool, redriven_final, &pre_salvaged).await?;
    store::abort_campaign(&db.pool, third_campaign_id, &PhaseChange::default()).await?;

    // Expired final lease (`attempts == MAX_ATTEMPTS`) after a committed memory
    // drop: `Reconciler::redrive` consults `completed_before_redrive` (deferring
    // across a transient receipt-read failure and salvaging `Done` on the next
    // tick without calling `drop_memory` again or spending `failure_budget = 1`),
    // while an uncommitted expired final lease still marks `Failed` and pauses
    // the campaign at `failure_budget = 1`.
    sqlx::query(sql!("DELETE FROM snapshot_storage_locations"))
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!("DELETE FROM sandboxes"))
        .execute(&db.pool)
        .await?;
    let mut tight_budget_seed = memory_drop_seed();
    tight_budget_seed.failure_budget = 1;
    let fourth_campaign_id = insert_campaign(&db.pool, tight_budget_seed.clone()).await?;
    let CampaignRecord {
        campaign: fourth_campaign,
        ..
    } = store::get_record(&db.pool, fourth_campaign_id)
        .await?
        .expect("fourth campaign exists");
    let expired_final_committed = suspended_on_source(&db.pool).await?;
    record(&db.pool, fourth_campaign_id, &[expired_final_committed]).await?;
    let mut final_lapsed_script = (1..MAX_ATTEMPTS)
        .map(|attempt| {
            ScriptedDrop::Fail(DispatchError::Retryable(format!(
                "final-lapse attempt {attempt} timeout"
            )))
        })
        .collect::<Vec<_>>();
    final_lapsed_script.push(ScriptedDrop::CommitDrop);
    dispatcher.script(expired_final_committed, final_lapsed_script);
    for _ in 1..MAX_ATTEMPTS {
        let retry_lease =
            &store::lease_items(&db.pool, fourth_campaign_id, Duration::from_secs(60), 1).await?[0];
        let retry = driver.upgrade(&fourth_campaign, retry_lease).await;
        store::finish_item(&db.pool, retry_lease, &retry).await?;
    }
    let lapsed_final_lease =
        &store::lease_items(&db.pool, fourth_campaign_id, Duration::from_secs(60), 1).await?[0];
    assert_eq!(lapsed_final_lease.attempts, MAX_ATTEMPTS);
    assert_eq!(
        driver.upgrade(&fourth_campaign, lapsed_final_lease).await,
        ItemOutcome::Done
    );
    // Rotate to a new `full` checkpoint on `source()` before the final lease
    // lapses so redrive salvage proves durable receipt recovery (and does not
    // touch the new `full` checkpoint).
    let post_lapse_full =
        repin_to_snapshot(&db.pool, expired_final_committed, "full", source()).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(fourth_campaign_id)
    .bind(expired_final_committed)
    .execute(&db.pool)
    .await?;
    let calls_before_final_redrive = dispatcher.calls().len();
    dispatcher.script_completed_errors(
        expired_final_committed,
        vec![Some(DispatchError::Retryable(
            "transient redrive receipt timeout".to_owned(),
        ))],
    );
    tick(&replacement_reconciler).await;
    assert_eq!(
        item(&db.pool, fourth_campaign_id, expired_final_committed).await?,
        (
            "in_flight".to_owned(),
            Some(format!("final-lapse attempt {} timeout", MAX_ATTEMPTS - 1)),
            (MAX_ATTEMPTS + 1) as i64,
        ),
        "transient completion-read error during redrive defers failing the expired final lease"
    );
    assert_eq!(
        campaign_phase(&db.pool, fourth_campaign_id).await?,
        "rolling"
    );
    tick_until_finished(&replacement_reconciler, &db.pool, fourth_campaign_id, 5).await?;
    assert_eq!(
        campaign_phase(&db.pool, fourth_campaign_id).await?,
        "completed",
        "salvaging a committed expired final lease completes the campaign without spending failure_budget = 1"
    );
    assert_eq!(
        item(&db.pool, fourth_campaign_id, expired_final_committed).await?,
        ("done".to_owned(), None, (MAX_ATTEMPTS + 1) as i64)
    );
    assert_eq!(
        dispatcher.calls().len(),
        calls_before_final_redrive,
        "salvaging an expired final lease never invokes drop_memory again"
    );
    assert!(
        matches!(
            suspended_environment_pins::pin_state(
                &db.pool,
                K8S,
                SandboxId::from_uuid(expired_final_committed),
            )
            .await?,
            SandboxPinState::SuspendedPinned {
                kind: ArtifactKind::Full,
                artifact_id,
                ..
            } if artifact_id == post_lapse_full
        ),
        "post-lapse full checkpoint stays untouched after final-lease salvage"
    );

    let fifth_campaign_id = insert_campaign(&db.pool, tight_budget_seed).await?;
    let CampaignRecord {
        campaign: fifth_campaign,
        ..
    } = store::get_record(&db.pool, fifth_campaign_id)
        .await?
        .expect("fifth campaign exists");
    let expired_final_uncommitted = suspended_on_source(&db.pool).await?;
    record(&db.pool, fifth_campaign_id, &[expired_final_uncommitted]).await?;
    dispatcher.script(
        expired_final_uncommitted,
        (1..MAX_ATTEMPTS)
            .map(|attempt| {
                ScriptedDrop::Fail(DispatchError::Retryable(format!(
                    "uncommitted attempt {attempt} timeout"
                )))
            })
            .collect(),
    );
    for _ in 1..MAX_ATTEMPTS {
        let retry_lease =
            &store::lease_items(&db.pool, fifth_campaign_id, Duration::from_secs(60), 1).await?[0];
        let retry = driver.upgrade(&fifth_campaign, retry_lease).await;
        store::finish_item(&db.pool, retry_lease, &retry).await?;
    }
    let uncommitted_final =
        &store::lease_items(&db.pool, fifth_campaign_id, Duration::from_secs(60), 1).await?[0];
    assert_eq!(uncommitted_final.attempts, MAX_ATTEMPTS);
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(fifth_campaign_id)
    .bind(expired_final_uncommitted)
    .execute(&db.pool)
    .await?;
    tick(&replacement_reconciler).await;
    assert_eq!(
        item(&db.pool, fifth_campaign_id, expired_final_uncommitted).await?,
        (
            "failed".to_owned(),
            Some("the lease expired before an outcome was recorded".to_owned()),
            MAX_ATTEMPTS as i64,
        ),
        "uncommitted expired final lease still fails at MAX_ATTEMPTS"
    );
    assert_eq!(
        campaign_phase(&db.pool, fifth_campaign_id).await?,
        "paused",
        "uncommitted expired final lease counts against failure_budget = 1 and auto-pauses"
    );

    Ok(())
}

// Nodeless pacing (`!paces_per_node`) and cross-kind subject-lock matrices live
// in `pplx-space-runtime-upgrade-campaign-store`; here we assert only the
// controller delta that releasing a coexisting `sandbox_force_restarts` lease
// lets the next reconciler pass discover and dispatch `drop_memory` on that
// sandbox.
#[tokio::test(flavor = "current_thread")]
async fn coexisting_force_restart_lease_holds_handoff_until_released_then_dispatches_drop()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;

    let live_node = node(&db.pool).await?;
    let free = suspended_on_source(&db.pool).await?;
    let locked_by_force_restart = insert_suspended_pinned_with_id(
        &db.pool,
        Uuid::now_v7(),
        "full",
        source(),
        Some(live_node),
    )
    .await?;

    let force_campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            from: source(),
            to: target(),
            ..CampaignSeed::default()
        },
    )
    .await?;
    record(&db.pool, force_campaign, &[locked_by_force_restart]).await?;
    let force_leased =
        store::lease_items(&db.pool, force_campaign, Duration::from_secs(300), 10).await?;
    assert_eq!(force_leased.len(), 1);

    let drop_campaign = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let dispatcher = FakeDropDispatcher::new(&db.pool);
    let retirement = FakeMemoryRetirement::retired(source());
    let reconciler = drop_reconciler(&db.pool, dispatcher.clone(), retirement, 100);

    tick(&reconciler).await;
    assert_eq!(dispatcher.dropped_sandboxes(), vec![free]);
    assert_eq!(campaign_phase(&db.pool, drop_campaign).await?, "rolling");
    assert_eq!(
        item(&db.pool, drop_campaign, locked_by_force_restart)
            .await?
            .0,
        "pending"
    );

    store::finish_item(
        &db.pool,
        &force_leased[0],
        &ItemOutcome::Release(ItemError::new("deferred")),
    )
    .await?;
    store::abort_campaign(&db.pool, force_campaign, &PhaseChange::default()).await?;

    tick_until_finished(&reconciler, &db.pool, drop_campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, drop_campaign).await?, "completed");
    let mut expected = vec![free, locked_by_force_restart];
    expected.sort();
    assert_eq!(dispatcher.dropped_sandboxes(), expected);
    assert_eq!(
        item(&db.pool, drop_campaign, locked_by_force_restart).await?,
        ("done".to_owned(), None, 1)
    );
    Ok(())
}
