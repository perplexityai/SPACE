//! Postgres differential check between the two in-flight-upload shapes.
//!
//! [`super::inflight_suspend_upload_exists`] nests its artifact-identity and
//! legacy branches inside one correlated `EXISTS`. It used to compose them
//! through [`super::artifact_identity_or_legacy`] as two sibling `EXISTS`
//! expressions, which a negating caller could not get planned as an anti-join:
//! the legacy branch correlates through an inequality, so it re-executed once
//! per outer row and rescanned every `state='creating'` full snapshot in the
//! cluster each time, potentially exhausting the node-drain statement timeout.
//!
//! Seven statements read this fragment — the node-drain reupload queue and the
//! admin `:force-upload-local-snapshots` enumeration, plus five guarded writes
//! (`mark_sandbox_lost`, its gone-node variant, `mark_sandbox_failed_on_node`,
//! `fail_inflight_snapshots_on_gone_node`, and the transient-lifecycle
//! sweeper's unrestorable-`restoring` guard). Reshaping something that many
//! terminal writes negate is only allowed if the shapes agree for every row,
//! so the agreement is asserted against a live database rather than argued
//! from the distribution. The distribution *is*
//! sound — the branch key `current_local_artifact_id IS [NOT] NULL` reads only
//! the outer row, so pushing it inside the `EXISTS` cannot change which
//! snapshot rows satisfy it — but "sound argument" is what the sibling form's
//! own comment offered about its planning, and it was wrong.
//!
//! The fixture enumerates the axes that can separate the two forms rather than
//! a list of remembered cases: artifact identity (absent / matching /
//! mismatched id / mismatched kind), the `creating` row's freshness against
//! `placement_changed_at` (fresh / stale), and presence of the `creating` row
//! at all. A row that is TRUE under both forms and a row that is FALSE under
//! both are each required to exist, so the comparison cannot pass by finding
//! nothing.

use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    artifact_identity_or_legacy, inflight_suspend_upload_exists,
    inflight_suspend_upload_from_node_exists, inflight_suspend_upload_from_other_node_exists,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The shape as it was before the merge: the same two branches, composed as
/// siblings. Kept verbatim so the differential runs against the real prior
/// statement instead of a paraphrase of it.
fn sibling_inflight_suspend_upload_exists(sandbox_alias: &str) -> String {
    let exact = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.local_artifact_id = {sandbox_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = {sandbox_alias}.current_local_artifact_kind\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
         )"
    );
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND (\n\
                    {sandbox_alias}.placement_changed_at IS NULL\n\
                    OR sn.timestamp >= {sandbox_alias}.placement_changed_at\n\
                )\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// The node-scoped siblings, in their pre-merge form. `node_scope` is the whole
/// `source_node_id` conjunct so the two variants differ only by that string —
/// `= $1` for the from-node form, `IS DISTINCT FROM $1` for the from-other-node
/// form — which is exactly how the merged versions differ too.
fn sibling_inflight_scoped(sandbox_alias: &str, node_scope: &str) -> String {
    let exact = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.source_node_id {node_scope}\n\
                AND sn.local_artifact_id = {sandbox_alias}.current_local_artifact_id\n\
                AND sn.artifact_kind = {sandbox_alias}.current_local_artifact_kind\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
         )"
    );
    let legacy = format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM snapshots sn\n\
              WHERE sn.sandbox_id = {sandbox_alias}.id\n\
                AND sn.source_node_id {node_scope}\n\
                AND sn.type = 'full'\n\
                AND sn.state = 'creating'\n\
                AND (\n\
                    {sandbox_alias}.placement_changed_at IS NULL\n\
                    OR sn.timestamp >= {sandbox_alias}.placement_changed_at\n\
                )\n\
         )"
    );
    artifact_identity_or_legacy(sandbox_alias, &exact, &legacy)
}

/// How a fixture row carries (or fails to carry) artifact identity.
#[derive(Clone, Copy, Debug)]
enum Identity {
    /// Rollout-skew row: no `current_local_artifact_id`, so the legacy
    /// freshness branch decides.
    Legacy,
    /// The `creating` row is the sandbox's tracked current artifact.
    Matching,
    /// A `creating` row exists but for a different artifact id — a prior
    /// cycle's upload, which must not protect the current one.
    MismatchedId,
    /// Same artifact id, different kind. The predicate matches on
    /// `(local_artifact_id, artifact_kind)` together, so this row is what
    /// separates a two-column identity test from a one-column one: it is the
    /// only case a form that compared ids alone would get wrong.
    MismatchedKind,
}

/// When the sandbox's `creating` full snapshot was stamped, relative to its
/// `placement_changed_at`.
#[derive(Clone, Copy, Debug)]
enum Upload {
    /// No `creating` row at all.
    None,
    /// Stamped after the last placement — a live upload.
    Fresh,
    /// Stamped before the last placement — a dead prior-cycle remnant.
    Stale,
    /// **Two** `creating` rows: one stale carrying the case's own artifact
    /// identity, one fresh carrying an unrelated one. This is the axis that
    /// separates "quantify twice" from "quantify once": the sibling form
    /// evaluated two independent `EXISTS` over the same relation, the merged
    /// form evaluates one, and this is the row shape where a different snapshot
    /// satisfies each branch. The forms are still equivalent — the branch key
    /// reads only the outer row, so exactly one branch is live either way — but
    /// that is an argument, and this makes it a measurement.
    StaleOwnAndFreshForeign,
}

/// Every combination of the axes that can separate the two shapes. Enumerated
/// rather than hand-listed so a later axis is added here once instead of being
/// forgotten in one of the two forms.
const IDENTITIES: [Identity; 4] = [
    Identity::Legacy,
    Identity::Matching,
    Identity::MismatchedId,
    Identity::MismatchedKind,
];
const UPLOADS: [Upload; 4] = [
    Upload::None,
    Upload::Fresh,
    Upload::Stale,
    Upload::StaleOwnAndFreshForeign,
];

#[tokio::test(flavor = "current_thread")]
async fn merged_inflight_shape_matches_the_sibling_shape_for_every_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;

    let mut cases = Vec::new();
    for identity in IDENTITIES {
        for upload in UPLOADS {
            let sandbox = insert_case(pool, node, identity, upload).await?;
            cases.push((sandbox, identity, upload));
        }
    }

    // All three variants of the fragment were reshaped, so all three are
    // compared. The node-scoped pair is where the shape mattered most — the
    // `draining -> drained` blocker count negates the from-node form — and
    // `IS DISTINCT FROM` is the one node scope whose NULL handling a merge could
    // plausibly change, so the fixture's snapshots are seeded from `node` and
    // each form is evaluated against both `node` and an unrelated node.
    let other_node = NodeId::new();
    let variants: [(&str, String, String); 6] = [
        (
            "node-agnostic",
            inflight_suspend_upload_exists("s"),
            sibling_inflight_suspend_upload_exists("s"),
        ),
        (
            "from this node",
            inflight_suspend_upload_from_node_exists("s", &quote(node)),
            sibling_inflight_scoped("s", &format!("= {}", quote(node))),
        ),
        (
            "from another node",
            inflight_suspend_upload_from_node_exists("s", &quote(other_node)),
            sibling_inflight_scoped("s", &format!("= {}", quote(other_node))),
        ),
        (
            "from other-than this node",
            inflight_suspend_upload_from_other_node_exists("s", &quote(node)),
            sibling_inflight_scoped("s", &format!("IS DISTINCT FROM {}", quote(node))),
        ),
        (
            "from other-than another node",
            inflight_suspend_upload_from_other_node_exists("s", &quote(other_node)),
            sibling_inflight_scoped("s", &format!("IS DISTINCT FROM {}", quote(other_node))),
        ),
        (
            "from other-than NULL",
            inflight_suspend_upload_from_other_node_exists("s", "NULL::uuid"),
            sibling_inflight_scoped("s", "IS DISTINCT FROM NULL::uuid"),
        ),
    ];

    let mut mismatches = Vec::new();
    let mut merged_agnostic = Vec::new();
    for (label, merged_sql, sibling_sql) in &variants {
        let merged = evaluate(pool, merged_sql).await?;
        let sibling = evaluate(pool, sibling_sql).await?;
        if *label == "node-agnostic" {
            merged_agnostic = merged.clone();
        }
        // Every variant must be non-degenerate on its own, or its comparison
        // passes while reading nothing. Scoping to a node the fixture never
        // uploaded from is legitimately all-FALSE, so the requirement is that
        // *some* variant separates the rows and that each variant's pair agrees
        // row by row — the per-variant TRUE counts are reported on failure.
        let merged_true = merged.iter().filter(|(_, v)| *v).count();
        let sibling_true = sibling.iter().filter(|(_, v)| *v).count();
        for (sandbox, identity, upload) in &cases {
            let m = lookup(&merged, *sandbox);
            let sb = lookup(&sibling, *sandbox);
            if m != sb {
                mismatches.push(format!(
                    "[{label}] {identity:?}/{upload:?}: merged={m:?} sibling={sb:?} \
                     (variant TRUE counts: merged={merged_true}, sibling={sibling_true})"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "merged and sibling in-flight shapes disagree:\n{}",
        mismatches.join("\n")
    );
    let merged = merged_agnostic;

    // Non-degeneracy: the comparison above holds trivially if the predicate is
    // FALSE (or TRUE) for the whole fixture, which is exactly how an
    // equivalence check passes while reading nothing. `Legacy`/`Fresh` and
    // `Matching`/`Fresh` are in-flight; every `None` row is not.
    let true_rows = cases
        .iter()
        .filter(|(sandbox, _, _)| lookup(&merged, *sandbox) == Some(true))
        .count();
    let false_rows = cases.len() - true_rows;
    assert!(
        true_rows > 0 && false_rows > 0,
        "fixture must contain rows on both sides of the predicate; got \
         {true_rows} true and {false_rows} false out of {}",
        cases.len()
    );

    // And the answers themselves, so this file also pins the semantics the
    // merge had to preserve rather than only that two statements agree — two
    // identically-wrong statements agree too.
    for (sandbox, identity, upload) in &cases {
        let expected = match (identity, upload) {
            // A live upload of the sandbox's current artifact protects it.
            (Identity::Matching, Upload::Fresh) => true,
            // Identity beats freshness for a marker-carrying row: the exact
            // branch never consults `placement_changed_at`, so a `creating`
            // row for the current artifact counts however it is stamped.
            (Identity::Matching, Upload::Stale) => true,
            // A marker-carrying row whose `creating` snapshot is for another
            // artifact (or another kind) is not protected by it.
            (Identity::MismatchedId | Identity::MismatchedKind, _) => false,
            // A rollout-skew row has no identity to match, so freshness is the
            // only test available.
            (Identity::Legacy, Upload::Fresh) => true,
            (Identity::Legacy, Upload::Stale) => false,
            // Two rows, one satisfying the live branch. A markerless row is
            // protected by the fresh one; a marker-carrying row by the stale one
            // that bears its artifact — and the mismatch cases by neither, since
            // both of their rows carry the wrong identity.
            (Identity::Legacy, Upload::StaleOwnAndFreshForeign) => true,
            (Identity::Matching, Upload::StaleOwnAndFreshForeign) => true,
            (_, Upload::None) => false,
        };
        assert_eq!(
            lookup(&merged, *sandbox),
            Some(expected),
            "{identity:?}/{upload:?} must evaluate to {expected}"
        );
    }
    Ok(())
}

/// A uuid as a SQL literal, for use as a fragment's node expression. These
/// fragments take a node *expression*, not a bind slot, so a test that wants
/// several node scopes in one statement supplies literals.
fn quote(node: NodeId) -> String {
    format!("'{node}'::uuid")
}

fn lookup(rows: &[(Uuid, bool)], sandbox: Uuid) -> Option<bool> {
    rows.iter()
        .find(|(id, _)| *id == sandbox)
        .map(|(_, value)| *value)
}

/// Evaluate `fragment` (aliased `s`) for every sandbox in the fixture.
async fn evaluate(pool: &PgPool, fragment: &str) -> TestResult<Vec<(Uuid, bool)>> {
    let statement = format!("SELECT s.id, {fragment} AS in_flight FROM sandboxes s");
    Ok(
        sqlx::query_as::<_, (Uuid, bool)>(sqlx::AssertSqlSafe(sql!(statement.as_str())))
            .fetch_all(pool)
            .await?,
    )
}

/// Seed one sandbox for `(identity, upload)`.
///
/// `placement_changed_at` is backdated an hour so `Upload::Fresh` (stamped
/// `NOW()`) lands after it and `Upload::Stale` (stamped two hours ago) lands
/// before it.
async fn insert_case(
    pool: &PgPool,
    node: NodeId,
    identity: Identity,
    upload: Upload,
) -> TestResult<Uuid> {
    let sandbox = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    let current_artifact = match identity {
        Identity::Legacy => None,
        _ => Some(artifact),
    };
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, network_policy_generation
         )
         VALUES (
             $1, $2, $3, 'suspended', 'unplaced', 'k8s',
             $4, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
             $5, CASE WHEN $5::uuid IS NULL THEN NULL ELSE 'full' END,
             CASE WHEN $5::uuid IS NULL THEN NULL ELSE 'suspend' END, 1
         )"
    ))
    .bind(sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node)
    .bind(current_artifact)
    .execute(pool)
    .await?;

    // The `creating` row's own identity: matching for `Matching`, deliberately
    // divergent for the mismatch cases, and absent for `Legacy` (which is what
    // a rollout-skew upload looks like).
    let (snapshot_artifact, snapshot_kind) = match identity {
        Identity::Legacy => (None, None),
        Identity::Matching => (Some(artifact), Some("full")),
        Identity::MismatchedId => (Some(Uuid::now_v7()), Some("full")),
        Identity::MismatchedKind => (Some(artifact), Some("disk")),
    };
    // `(timestamp_expr, artifact_id, artifact_kind)` per `creating` row to seed.
    let snapshots: Vec<(&str, Option<Uuid>, Option<&str>)> = match upload {
        Upload::None => return Ok(sandbox),
        Upload::Fresh => vec![("NOW()", snapshot_artifact, snapshot_kind)],
        Upload::Stale => vec![(
            "NOW() - INTERVAL '2 hours'",
            snapshot_artifact,
            snapshot_kind,
        )],
        Upload::StaleOwnAndFreshForeign => vec![
            (
                "NOW() - INTERVAL '2 hours'",
                snapshot_artifact,
                snapshot_kind,
            ),
            // Fresh, but for an artifact this sandbox does not track. A
            // marker-carrying row must not be protected by it; a markerless one
            // must, because freshness is the only test it has.
            (
                "NOW()",
                snapshot_artifact.map(|_| Uuid::now_v7()),
                snapshot_kind.map(|_| "full"),
            ),
        ],
    };
    for (timestamp, snapshot_artifact, snapshot_kind) in snapshots {
        let statement = format!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, source_node_id, timestamp,
                 local_artifact_id, artifact_kind, artifact_origin
             )
             VALUES (
                 $1, $2, 'full', 'creating', $3, {timestamp},
                 $4, $5, CASE WHEN $4::uuid IS NULL THEN NULL ELSE 'suspend' END
             )"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql!(statement.as_str())))
            .bind(Uuid::now_v7())
            .bind(sandbox)
            .bind(node)
            .bind(snapshot_artifact)
            .bind(snapshot_kind)
            .execute(pool)
            .await?;
    }
    Ok(sandbox)
}
