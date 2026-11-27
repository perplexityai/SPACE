use pplx_space_cluster_trigger_queue::{QueuedRefConflict, queue_node_reconcile_refs};

use super::*;

const FACET: &str = "network-policy";

/// Where a queued ref stands after a completion.
#[derive(Debug, PartialEq, Eq)]
struct QueuedRef {
    attempts: i32,
    last_error: String,
    claim_token: String,
    /// Its last write put its next attempt later than the write: a retry's
    /// backoff, never a claim or a release.
    backing_off: bool,
}

/// `element`'s ref on `node_id`, `None` once completed.
async fn queued_ref(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    element: &str,
) -> TestResult<Option<QueuedRef>> {
    let row: Option<(i32, String, String, bool)> = sqlx::query_as(sql!(
        "SELECT attempts, last_error, claim_token, next_attempt_at > updated_at
         FROM node_reconcile_queue
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET)
    .bind(element)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(attempts, last_error, claim_token, backing_off)| QueuedRef {
            attempts,
            last_error,
            claim_token,
            backing_off,
        },
    ))
}

/// Whether `node_id` has an `ApplyFailed` condition for `element`.
async fn apply_failed(pool: &sqlx::PgPool, node_id: NodeId, element: &str) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM node_facet_conditions
             WHERE node_id = $1 AND facet = $2 AND element = $3
               AND condition_type = 'ApplyFailed'
         )"
    ))
    .bind(node_id)
    .bind(FACET)
    .bind(element)
    .fetch_one(pool)
    .await?)
}

/// A node claims its refs, reports one applied and one failed, reports two
/// that an enqueue refreshed after the claim, one applied and one failed,
/// and leaves one unreported. The applied ref is deleted and its
/// `ApplyFailed` condition cleared; the failed one counts an attempt, keeps
/// its error, records the condition and backs off; the refreshed ones are
/// not completed but released, due at once, the applied one keeping its
/// earlier `ApplyFailed` condition and the failed one recording none; the
/// unreported one stays claimed. A completion under another claim token
/// changes nothing.
#[tokio::test(flavor = "current_thread")]
async fn completion_settles_each_claimed_ref_by_its_outcome() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let [applied, failed, refreshed, refreshed_failed, unreported] =
        std::array::from_fn(|_| format!("sandbox:{}", SandboxId::new()));
    let elements = [
        &applied,
        &failed,
        &refreshed,
        &refreshed_failed,
        &unreported,
    ];
    for element in elements {
        queue_node_reconcile_refs(
            pool,
            &[node_id],
            &[(FACET.to_string(), element.clone())],
            QueuedRefConflict::KeepProgress,
        )
        .await?;
    }
    // Failures the applied refs left from an earlier pass.
    for element in [&applied, &refreshed] {
        pplx_space_sandbox_teardown::observed_status::touch(pool, node_id, FACET, Some(element))
            .await?;
        pplx_space_sandbox_teardown::conditions::record_apply_failed(
            pool,
            node_id,
            FACET,
            Some(element),
            "synthetic earlier failure",
        )
        .await?;
    }

    let claim = fixture
        .service
        .claim_node_reconcile_refs(request_from_node(
            ClaimNodeReconcileRefsRequest {
                node_id: node_id.to_string(),
                limit: 0,
                role: None,
            },
            TEST_WORKER_IP,
        ))
        .await?
        .into_inner();
    assert_eq!(claim.refs.len(), 5);
    let claimed_ref = |element: &str| -> TestResult<FacetRef> {
        for reference in &claim.refs {
            if proto_facet_ref_to_storage(reference.clone())?
                .element
                .as_deref()
                == Some(element)
            {
                return Ok(reference.clone());
            }
        }
        Err(format!("{element} was not claimed").into())
    };
    let (applied_ref, failed_ref, refreshed_ref, refreshed_failed_ref) = (
        claimed_ref(&applied)?,
        claimed_ref(&failed)?,
        claimed_ref(&refreshed)?,
        claimed_ref(&refreshed_failed)?,
    );
    for element in [&refreshed, &refreshed_failed] {
        queue_node_reconcile_refs(
            pool,
            &[node_id],
            &[(FACET.to_string(), element.clone())],
            QueuedRefConflict::KeepProgress,
        )
        .await?;
    }

    let complete = |claim_token: &str| {
        request_from_node(
            CompleteNodeReconcileRefsRequest {
                node_id: node_id.to_string(),
                claim_token: claim_token.to_string(),
                succeeded: vec![applied_ref.clone(), refreshed_ref.clone()],
                failed: [&failed_ref, &refreshed_failed_ref]
                    .map(|reference| crate::proto::FailedNodeReconcileRef {
                        r#ref: Some(reference.clone()),
                        error: "synthetic apply failure".to_string(),
                    })
                    .to_vec(),
                role: None,
            },
            TEST_WORKER_IP,
        )
    };
    let stale = fixture
        .service
        .complete_node_reconcile_refs(complete("another-claim"))
        .await?
        .into_inner();
    assert_eq!((stale.succeeded, stale.failed), (0, 0));
    let claimed = |attempts, last_error: &str| QueuedRef {
        attempts,
        last_error: last_error.to_string(),
        claim_token: claim.claim_token.clone(),
        backing_off: false,
    };
    for element in elements {
        assert_eq!(
            queued_ref(pool, node_id, element).await?,
            Some(claimed(0, "")),
            "{element} after the stale completion"
        );
    }
    assert!(apply_failed(pool, node_id, &applied).await?);
    assert!(!apply_failed(pool, node_id, &failed).await?);
    assert!(!apply_failed(pool, node_id, &refreshed_failed).await?);

    let completed = fixture
        .service
        .complete_node_reconcile_refs(complete(&claim.claim_token))
        .await?
        .into_inner();
    assert_eq!((completed.succeeded, completed.failed), (1, 1));

    assert_eq!(queued_ref(pool, node_id, &applied).await?, None);
    assert!(!apply_failed(pool, node_id, &applied).await?);
    assert_eq!(
        queued_ref(pool, node_id, &failed).await?,
        Some(QueuedRef {
            claim_token: String::new(),
            backing_off: true,
            ..claimed(1, "synthetic apply failure")
        })
    );
    assert!(apply_failed(pool, node_id, &failed).await?);
    assert_eq!(
        queued_ref(pool, node_id, &refreshed).await?,
        Some(QueuedRef {
            claim_token: String::new(),
            ..claimed(0, "")
        })
    );
    assert!(apply_failed(pool, node_id, &refreshed).await?);
    assert_eq!(
        queued_ref(pool, node_id, &refreshed_failed).await?,
        Some(QueuedRef {
            claim_token: String::new(),
            ..claimed(0, "")
        })
    );
    assert!(!apply_failed(pool, node_id, &refreshed_failed).await?);
    assert_eq!(
        queued_ref(pool, node_id, &unreported).await?,
        Some(claimed(0, ""))
    );
    Ok(())
}
