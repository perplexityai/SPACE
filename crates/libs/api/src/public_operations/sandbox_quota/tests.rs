use std::time::{Duration, Instant};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{IdempotencyKey, OrgId, SandboxId, UserId};
use pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections;
use pplx_space_public_api::{
    OperationKind, OperationTargetType, PublicSandboxId, RequestFingerprint,
};
use pplx_space_test_env::TestResult;
use sqlx::{PgConnection, PgPool};

use super::*;
use crate::public_operations::{
    CreatePublicOperation, CreatePublicOperationIdempotency, CreatePublicOperationResult,
    HttpOperationMethod, OperationPathTemplate, PublicOperationStore, create_operation_tx,
};

#[test]
fn creates_lock_shared_well_under_a_limit_and_exclusively_near_it() {
    use LockMode::{Exclusive, Shared};
    for (held, limit, planned) in [
        // Within twice the minimum headroom, always exclusive.
        (0, 1, Ok(Exclusive)),
        (2, 3, Ok(Exclusive)),
        (3, 3, Err(())),
        (0, 128, Ok(Exclusive)),
        (0, 129, Ok(Shared)),
        // Larger limits keep a tenth of themselves as headroom.
        (799, 1000, Ok(Shared)),
        (800, 1000, Ok(Exclusive)),
        (999, 1000, Ok(Exclusive)),
        (1000, 1000, Err(())),
    ] {
        assert_eq!(plan_limit(held, limit), planned, "{held} held of {limit}");
    }
    for (held, mode, verdict) in [
        (999, Exclusive, Verdict::Admit),
        (1000, Exclusive, Verdict::Refuse),
        (899, Shared, Verdict::Admit),
        // A shared count that grew into the headroom cannot be decided.
        (900, Shared, Verdict::Contended),
        (999, Shared, Verdict::Contended),
        (1000, Shared, Verdict::Refuse),
    ] {
        assert_eq!(enforce_limit(held, 1000, mode), verdict, "{held} {mode:?}");
    }
}

/// The org limit counts every live sandbox of the org and its in-flight
/// creates, whoever owns them, so a federated caller cannot escape it by
/// arriving as a new subject; the subject limit counts the subject's own.
#[tokio::test]
async fn the_org_limit_counts_every_owner_and_a_subject_limit_its_own() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(2).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let org_id = seed_org(pool).await?;
    let local = seed_user(pool, org_id, None).await?;
    let subject_a = seed_user(pool, org_id, Some("subject-a")).await?;
    let subject_b = seed_user(pool, org_id, Some("subject-b")).await?;
    let new_subject = seed_user(pool, org_id, Some("subject-new")).await?;
    let other_org_id = seed_org(pool).await?;
    let other_org_user = seed_user(pool, other_org_id, None).await?;

    let materialized = SandboxId::new();
    for (owner, owner_org, sandbox_id, lifecycle) in [
        (local, org_id, materialized, "started"),
        (local, org_id, SandboxId::new(), "deleting"),
        (local, org_id, SandboxId::new(), "deleted"),
        (local, org_id, SandboxId::new(), "failed"),
        (local, org_id, SandboxId::new(), "quarantined"),
        (local, org_id, SandboxId::new(), "cleanup_required"),
        (subject_a, org_id, SandboxId::new(), "started"),
        (subject_b, org_id, SandboxId::new(), "suspended"),
        (other_org_user, other_org_id, SandboxId::new(), "started"),
    ] {
        seed_sandbox(pool, sandbox_id, owner, owner_org, lifecycle).await?;
    }
    let store = PublicOperationStore::new(pool.clone());
    // In flight past its row insert, so already counted by its row.
    record_create(&store, org_id, local, materialized).await?;
    // In flight while its row is terminal (a drain sweep failed it): it can
    // still retry and succeed, so it counts.
    let abandoned = SandboxId::new();
    seed_sandbox(pool, abandoned, local, org_id, "failed").await?;
    record_create(&store, org_id, local, abandoned).await?;
    // In flight before their row inserts.
    record_create(&store, org_id, local, SandboxId::new()).await?;
    record_create(&store, org_id, subject_a, SandboxId::new()).await?;
    for terminal in ["succeeded", "failed"] {
        let operation_id = record_create(&store, org_id, local, SandboxId::new()).await?;
        sqlx::query(sql!(
            "UPDATE public_operations SET state = $2 WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .bind(terminal)
        .execute(pool)
        .await?;
    }

    // The org holds four live rows and three creates; subject A one of each.
    let subject = |user_id, limit| Some(SubjectSandboxQuota { user_id, limit });
    for (quota, expected) in [
        (
            org_quota(org_id, 7),
            SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Org { limit: 7 }),
        ),
        (org_quota(org_id, 8), SandboxQuotaOutcome::Admit),
        (
            SandboxQuota {
                org_id,
                org_limit: Some(7),
                subject: subject(new_subject, 10),
            },
            SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Org { limit: 7 }),
        ),
        (
            SandboxQuota {
                org_id,
                org_limit: None,
                subject: subject(subject_a, 2),
            },
            SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Subject { limit: 2 }),
        ),
        (
            SandboxQuota {
                org_id,
                org_limit: Some(8),
                subject: subject(subject_a, 3),
            },
            SandboxQuotaOutcome::Admit,
        ),
        (
            SandboxQuota {
                org_id,
                org_limit: None,
                subject: subject(new_subject, 1),
            },
            SandboxQuotaOutcome::Admit,
        ),
    ] {
        let mut admission = begin(&store, org_id, "count").await?;
        assert_eq!(
            admission.admit_new_create(quota).await?,
            expected,
            "{quota:?}"
        );
        admission.finish().await?;
    }
    Ok(())
}

/// A create near either limit takes that limit's lock exclusively: it waits
/// for a racing create holding the lock and, once that one commits, counts
/// it.
#[tokio::test]
async fn an_exclusive_create_waits_for_a_racing_create_and_counts_it() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let store = PublicOperationStore::new(db.pool.clone());
    for per_subject in [false, true] {
        let org_id = seed_org(&db.pool).await?;
        let actor_id = seed_user(&db.pool, org_id, Some("racing-subject")).await?;
        let (lock_id, seed, quota, refusal) = if per_subject {
            (
                actor_id.as_uuid(),
                SUBJECT_LOCK_SEED,
                SandboxQuota {
                    org_id,
                    org_limit: None,
                    subject: Some(SubjectSandboxQuota {
                        user_id: actor_id,
                        limit: 1,
                    }),
                },
                SandboxQuotaRefusal::Subject { limit: 1 },
            )
        } else {
            (
                org_id.as_uuid(),
                ORG_LOCK_SEED,
                org_quota(org_id, 1),
                SandboxQuotaRefusal::Org { limit: 1 },
            )
        };

        let mut racing = db.pool.begin().await?;
        lock_tx(&mut racing, lock_id, seed, LockMode::Exclusive).await?;
        let racing_target = PublicSandboxId::new().to_string();
        create_operation_tx(
            &mut racing,
            native_create(org_id, actor_id, &racing_target, None),
        )
        .await?;

        let mut waiting = begin(&store, org_id, "exclusive-waiting").await?;
        let plan = planned(&mut waiting, quota).await?;
        let waiting = tokio::spawn(async move {
            let outcome = waiting.enforce_sandbox_quota(plan).await;
            waiting.finish().await.map(|()| outcome)
        });
        wait_until_blocked_by(&mut racing).await?;

        racing.commit().await?;
        assert_eq!(
            waiting.await???,
            SandboxQuotaOutcome::Refuse(refusal),
            "per subject: {per_subject}"
        );
    }
    Ok(())
}

/// A create well under the limit holds the lock shared through its commit,
/// so a create near the limit waits for it and counts it. Counting only
/// committed creates without that wait would admit the near one too and put
/// the org over its limit.
#[tokio::test]
async fn a_near_limit_create_waits_for_in_flight_shared_creates() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, org_id, None).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let quota = org_quota(org_id, 1000);
    seed_sandboxes(&db.pool, org_id, actor_id, 799).await?;

    // Well under the limit: admitted under the shared lock, not yet recorded.
    let mut shared = begin(&store, org_id, "shared-in-flight").await?;
    assert_eq!(
        shared.admit_new_create(quota).await?,
        SandboxQuotaOutcome::Admit
    );
    // Others fill the org to one under its limit.
    seed_sandboxes(&db.pool, org_id, actor_id, 200).await?;

    let mut near = begin(&store, org_id, "near-limit").await?;
    let plan = planned(&mut near, quota).await?;
    let near = tokio::spawn(async move {
        let outcome = near.enforce_sandbox_quota(plan).await;
        near.finish().await.map(|()| outcome)
    });
    wait_until_blocked_by(&mut shared.tx).await?;

    let target_id = PublicSandboxId::new().to_string();
    let key = IdempotencyKey::parse("shared-in-flight")?;
    let fingerprint = fingerprint();
    let recorded = shared
        .create(native_create(
            org_id,
            actor_id,
            &target_id,
            Some(idempotency(&key, &fingerprint)),
        ))
        .await?;
    assert!(matches!(recorded, CreatePublicOperationResult::Created(_)));
    assert_eq!(
        near.await???,
        SandboxQuotaOutcome::Refuse(SandboxQuotaRefusal::Org { limit: 1000 })
    );
    Ok(())
}

/// A create that planned a shared lock but counts near the limit once it has
/// it (the count grew while it waited) is not admitted on that count.
#[tokio::test]
async fn a_shared_count_that_moved_near_the_limit_is_contended() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, org_id, None).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let quota = org_quota(org_id, 1000);
    seed_sandboxes(&db.pool, org_id, actor_id, 799).await?;

    let mut shared = begin(&store, org_id, "planned-shared").await?;
    let plan = planned(&mut shared, quota).await?;

    let mut racing = db.pool.begin().await?;
    lock_tx(
        &mut racing,
        org_id.as_uuid(),
        ORG_LOCK_SEED,
        LockMode::Exclusive,
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), $1, $2, 'k8s', 'started', 1 FROM generate_series(1, 150)"
    ))
    .bind(actor_id)
    .bind(org_id)
    .execute(&mut *racing)
    .await?;

    let shared = tokio::spawn(async move {
        let outcome = shared.enforce_sandbox_quota(plan).await;
        shared.finish().await.map(|()| outcome)
    });
    wait_until_blocked_by(&mut racing).await?;
    racing.commit().await?;
    assert_eq!(shared.await???, SandboxQuotaOutcome::Contended);
    Ok(())
}

/// Creates well under the limit do not wait for one another, and a create
/// at the limit is refused without taking a lock at all.
#[tokio::test]
async fn shared_creates_do_not_wait_for_each_other_and_refusals_take_no_lock() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, org_id, None).await?;
    seed_sandboxes(&db.pool, org_id, actor_id, 2).await?;
    let store = PublicOperationStore::new(db.pool.clone());

    for (held_mode, limit, expected) in [
        (LockMode::Shared, 1000, Ok(SandboxQuotaOutcome::Admit)),
        (
            LockMode::Exclusive,
            2,
            Err(SandboxQuotaRefusal::Org { limit: 2 }),
        ),
    ] {
        let mut holder = db.pool.begin().await?;
        lock_tx(&mut holder, org_id.as_uuid(), ORG_LOCK_SEED, held_mode).await?;
        let quota = org_quota(org_id, limit);
        let check = async {
            let mut admission = begin(&store, org_id, "no-wait").await?;
            let outcome = match admission.plan_sandbox_quota(quota).await? {
                Ok(plan) => Ok(admission.enforce_sandbox_quota(plan).await?),
                Err(refusal) => Err(refusal),
            };
            admission.finish().await?;
            TestResult::Ok(outcome)
        };
        let outcome = tokio::time::timeout(Duration::from_secs(2), check)
            .await
            .map_err(|_| format!("limit {limit}: the create waited on the quota lock"))??;
        assert_eq!(outcome, expected, "limit {limit}");
        holder.rollback().await?;
    }
    Ok(())
}

/// A create waiting for its quota lock gives up after
/// [`QUOTA_LOCK_TIMEOUT`], or sooner under a shorter `statement_timeout` or
/// `lock_timeout`, as contended rather than holding its connection until the
/// holder commits.
/// The bound covers the lock statements only: the rest of an admitted
/// create's transaction keeps the session's own `lock_timeout`.
#[tokio::test]
async fn a_long_quota_lock_wait_ends_contended() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let quota = org_quota(org_id, 1);

    let mut holder = db.pool.begin().await?;
    lock_tx(
        &mut holder,
        org_id.as_uuid(),
        ORG_LOCK_SEED,
        LockMode::Exclusive,
    )
    .await?;

    let mut waiting = begin(&store, org_id, "lock-timeout").await?;
    let started = Instant::now();
    let outcome = tokio::time::timeout(3 * QUOTA_LOCK_TIMEOUT, waiting.admit_new_create(quota))
        .await
        .map_err(|_| "the lock wait was not bounded")??;
    assert_eq!(outcome, SandboxQuotaOutcome::Contended);
    assert!(started.elapsed() >= QUOTA_LOCK_TIMEOUT);
    drop(waiting);

    let mut waiting = begin(&store, org_id, "statement-timeout").await?;
    sqlx::query(sql!("SET LOCAL statement_timeout = '100ms'"))
        .execute(&mut *waiting.tx)
        .await?;
    let outcome = tokio::time::timeout(QUOTA_LOCK_TIMEOUT / 2, waiting.admit_new_create(quota))
        .await
        .map_err(|_| "the statement timeout did not end the wait")??;
    assert_eq!(outcome, SandboxQuotaOutcome::Contended);
    drop(waiting);

    // A shorter bound already in force is kept, not raised.
    let mut waiting = begin(&store, org_id, "shorter-lock-timeout").await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '100ms'"))
        .execute(&mut *waiting.tx)
        .await?;
    let outcome = tokio::time::timeout(QUOTA_LOCK_TIMEOUT / 2, waiting.admit_new_create(quota))
        .await
        .map_err(|_| "the shorter lock timeout was raised")??;
    assert_eq!(outcome, SandboxQuotaOutcome::Contended);
    drop(waiting);
    holder.rollback().await?;

    let mut admitted = begin(&store, org_id, "admitted").await?;
    let show = sql!("SHOW lock_timeout");
    let session_default: String = sqlx::query_scalar(show)
        .fetch_one(&mut *admitted.tx)
        .await?;
    assert_eq!(
        admitted.admit_new_create(quota).await?,
        SandboxQuotaOutcome::Admit
    );
    let after: String = sqlx::query_scalar(show)
        .fetch_one(&mut *admitted.tx)
        .await?;
    assert_eq!(after, session_default);
    drop(admitted);

    // A bound the caller set earlier in the transaction, longer or shorter
    // than ours, is back in force once the locks are held.
    for earlier in ["7s", "100ms"] {
        let mut admitted = begin(&store, org_id, "earlier-lock-timeout").await?;
        sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
            .bind(earlier)
            .execute(&mut *admitted.tx)
            .await?;
        assert_eq!(
            admitted.admit_new_create(quota).await?,
            SandboxQuotaOutcome::Admit
        );
        let after: String = sqlx::query_scalar(show)
            .fetch_one(&mut *admitted.tx)
            .await?;
        assert_eq!(after, earlier);
    }
    Ok(())
}

fn org_quota(org_id: OrgId, limit: u32) -> SandboxQuota {
    SandboxQuota {
        org_id,
        org_limit: Some(limit),
        subject: None,
    }
}

async fn planned(
    admission: &mut PublicOperationAdmission,
    quota: SandboxQuota,
) -> TestResult<SandboxQuotaPlan> {
    admission
        .plan_sandbox_quota(quota)
        .await?
        .map_err(|refusal| format!("refused at the lock-free count: {refusal:?}").into())
}

/// Wait until some backend is blocked on a lock `holder` holds: proof that
/// the create under test queued behind it.
async fn wait_until_blocked_by(holder: &mut PgConnection) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let blocked: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND pg_backend_pid() = ANY(pg_blocking_pids(pid))
             )"
        ))
        .fetch_one(&mut *holder)
        .await?;
        if blocked {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("the create never waited for the holder's lock".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn begin(
    store: &PublicOperationStore,
    org_id: OrgId,
    key: &str,
) -> TestResult<PublicOperationAdmission> {
    let key = IdempotencyKey::parse(key)?;
    let fingerprint = fingerprint();
    Ok(store
        .begin_operation_admission(org_id, ApiProduct::Native, &idempotency(&key, &fingerprint))
        .await?)
}

fn fingerprint() -> RequestFingerprint {
    RequestFingerprint::parse("d".repeat(64)).expect("valid fingerprint")
}

fn idempotency<'a>(
    key: &'a IdempotencyKey,
    fingerprint: &'a RequestFingerprint,
) -> CreatePublicOperationIdempotency<'a> {
    CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new("/sandboxes"),
        key,
        request_fingerprint: fingerprint,
    }
}

fn native_create<'a>(
    org_id: OrgId,
    actor_id: UserId,
    target_id: &'a str,
    idempotency: Option<CreatePublicOperationIdempotency<'a>>,
) -> CreatePublicOperation<'a> {
    CreatePublicOperation {
        org_id,
        actor_user_id: Some(actor_id),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(target_id),
        payload_json: serde_json::json!({}),
        idempotency,
        traceparent: None,
    }
}

async fn record_create(
    store: &PublicOperationStore,
    org_id: OrgId,
    actor_id: UserId,
    target: SandboxId,
) -> TestResult<pplx_space_public_api::OperationId> {
    let target_id = PublicSandboxId::from_uuid(target.as_uuid()).to_string();
    match store
        .create_operation(native_create(org_id, actor_id, &target_id, None))
        .await?
    {
        CreatePublicOperationResult::Created(operation_id) => Ok(operation_id),
        other => Err(format!("create was not recorded: {other:?}").into()),
    }
}

async fn seed_org(pool: &PgPool) -> Result<OrgId, sqlx::Error> {
    let id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(id)
    .bind(format!("quota-{id}"))
    .execute(pool)
    .await?;
    Ok(id)
}

/// A local user, or a federated-identity one for `subject`.
async fn seed_user(
    pool: &PgPool,
    org_id: OrgId,
    subject: Option<&str>,
) -> Result<UserId, sqlx::Error> {
    let id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES ($1, $2, 'hash', 'Quota User', $3, 'member',
                 CASE WHEN $4::TEXT IS NULL THEN 'local' ELSE 'federated_identity' END,
                 CASE WHEN $4::TEXT IS NULL THEN NULL ELSE 'example-issuer' END,
                 $4)"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .bind(subject)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_sandbox(
    pool: &PgPool,
    id: SandboxId,
    user_id: UserId,
    org_id: OrgId,
    lifecycle: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 1)"
    ))
    .bind(id)
    .bind(user_id)
    .bind(org_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_sandboxes(
    pool: &PgPool,
    org_id: OrgId,
    user_id: UserId,
    count: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         SELECT gen_random_uuid(), $1, $2, 'k8s', 'started', 1 FROM generate_series(1, $3)"
    ))
    .bind(user_id)
    .bind(org_id)
    .bind(count)
    .execute(pool)
    .await?;
    Ok(())
}
