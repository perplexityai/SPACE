//! `adopt_live_execution` against the real Postgres fixture: the guard that
//! places a row with no active node back on the node still running its
//! execution, one conjunct at a time.
//!
//! The wake-level tests in `idle_suspended` reach this adoption through a
//! worker's record; these call it directly, so they can also set up a
//! missing route or provenance row, another substrate's row and a row that
//! already has an active node.

use super::*;

/// The Postgres `sandboxes` columns the adoption writes or fences on.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    restore_lease_token: Option<Uuid>,
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id, restore_lease_token
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(pool)
    .await?)
}

/// The row as the adoption reads it, before the call.
#[derive(Debug, Clone, Copy)]
enum Seed {
    /// `suspended`, no active node, no lease.
    Suspended,
    /// `restoring` with a token on an expired lease.
    ExpiredToken,
    /// `restoring` under a live lease.
    LiveLease,
    /// `suspended`, already placed on the other worker.
    ActiveOnOther,
    /// `suspended`, but Kubernetes' row.
    Kubernetes,
    /// Read `suspended`, now `deleting`.
    MovedToDeleting,
}

impl Seed {
    fn read_lifecycle(self) -> SandboxLifecycle {
        match self {
            Self::ExpiredToken | Self::LiveLease => SandboxLifecycle::Restoring,
            Self::Suspended | Self::ActiveOnOther | Self::Kubernetes | Self::MovedToDeleting => {
                SandboxLifecycle::Suspended
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RouteOn {
    Source,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// Placed on the source node, in this lifecycle.
    Adopted(&'static str),
    Refused,
}

#[derive(Debug, Clone, Copy)]
struct Case {
    label: &'static str,
    seed: Seed,
    /// The route identity's node and execution generation; `None` for no row.
    route: Option<(RouteOn, i64)>,
    /// The last reserved execution generation; `None` for no row.
    provenance: Option<i64>,
    started: bool,
    expect: Expect,
}

/// The execution the node reports in every case.
const GENERATION: i64 = 2;

const CASES: &[Case] = &[
    Case {
        label: "every conjunct holds",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Adopted("suspended"),
    },
    Case {
        label: "a started record at the route identity",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 2)),
        provenance: Some(2),
        started: true,
        expect: Expect::Adopted("started"),
    },
    Case {
        label: "a started record ahead of the route identity",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: true,
        expect: Expect::Adopted("suspended"),
    },
    Case {
        label: "route identity on the other node",
        seed: Seed::Suspended,
        route: Some((RouteOn::Other, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "route identity newer than the record",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 3)),
        provenance: Some(3),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "last reservation below the record",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(1),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "no route identity",
        seed: Seed::Suspended,
        route: None,
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "no provenance",
        seed: Seed::Suspended,
        route: Some((RouteOn::Source, 1)),
        provenance: None,
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "another substrate's row",
        seed: Seed::Kubernetes,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "row already has an active node",
        seed: Seed::ActiveOnOther,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "row left the lifecycle the caller read",
        seed: Seed::MovedToDeleting,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "live restore lease, one generation",
        seed: Seed::LiveLease,
        route: Some((RouteOn::Source, 2)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "token held, route identity behind the record",
        seed: Seed::ExpiredToken,
        route: Some((RouteOn::Source, 1)),
        provenance: Some(2),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "token held, reservation past the record",
        seed: Seed::ExpiredToken,
        route: Some((RouteOn::Source, 2)),
        provenance: Some(3),
        started: false,
        expect: Expect::Refused,
    },
    Case {
        label: "token held, one generation",
        seed: Seed::ExpiredToken,
        route: Some((RouteOn::Source, 2)),
        provenance: Some(2),
        started: false,
        expect: Expect::Adopted("restoring"),
    },
    Case {
        label: "token held, one generation, started",
        seed: Seed::ExpiredToken,
        route: Some((RouteOn::Source, 2)),
        provenance: Some(2),
        started: true,
        expect: Expect::Adopted("started"),
    },
];

async fn insert_route_identity(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    execution_generation: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities
         (sandbox_id, route_generation, current_node_id, substrate_kind,
          execution_generation, activation_attempt_id)
         VALUES ($1, $3, $2, 'space', $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(execution_generation)
    .bind(SandboxActivationAttemptId::new())
    .execute(pool)
    .await?;
    Ok(())
}

/// Seeds `case`'s row on `source` and returns it as seeded.
async fn seed(
    pool: &PgPool,
    case: &Case,
    sandbox_id: SandboxId,
    source: NodeId,
    other: NodeId,
) -> TestResult<Row> {
    insert_sandbox(pool, sandbox_id, Some(source)).await?;
    if let Some((route_on, generation)) = case.route {
        let node = match route_on {
            RouteOn::Source => source,
            RouteOn::Other => other,
        };
        insert_route_identity(pool, sandbox_id, node, generation).await?;
    }
    if let Some(generation) = case.provenance {
        insert_direct_route_provenance(pool, sandbox_id, generation).await?;
    }
    let lease_expires = match case.seed {
        Seed::ExpiredToken => Some("-1 minute"),
        Seed::LiveLease => Some("1 hour"),
        Seed::Suspended | Seed::ActiveOnOther | Seed::Kubernetes | Seed::MovedToDeleting => None,
    };
    if let Some(lease_expires) = lease_expires {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    restore_lease_expires_at = NOW() + $3::interval,
                    restore_lease_token = $2
              WHERE id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(Uuid::now_v7())
        .bind(lease_expires)
        .execute(pool)
        .await?;
    }
    match case.seed {
        Seed::ActiveOnOther => {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET active_node_id = $2, target_node_id = $2, placement = 'on_node'
                  WHERE id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(other)
            .execute(pool)
            .await?;
        }
        Seed::Kubernetes => {
            sqlx::query(sql!("UPDATE sandboxes SET managed_by = $2 WHERE id = $1"))
                .bind(sandbox_id.as_uuid())
                .bind(SubstrateKind::Kubernetes)
                .execute(pool)
                .await?;
        }
        Seed::MovedToDeleting => {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .execute(pool)
            .await?;
        }
        Seed::Suspended | Seed::ExpiredToken | Seed::LiveLease => {}
    }
    row(pool, sandbox_id).await
}

/// Each conjunct falsified alone refuses the adoption and leaves the row as
/// it was; with all of them true the row is placed on the node, `started`
/// only when the record is started at the route identity's execution. A row
/// that holds a restore-lease token is adopted only when the route identity,
/// the record and the last reservation are one generation.
#[tokio::test(flavor = "current_thread")]
async fn adoption_places_the_row_only_when_every_conjunct_holds() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let source_id = NodeId::new();
    let other_id = NodeId::new();
    insert_ready_worker(&fixture.pool, source_id, "http://127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(&fixture.pool, other_id, "http://127.0.0.1:2", "9.0.0").await?;
    let source = fixture.substrate.direct_node_by_id(source_id).await?;

    let mut mismatches = Vec::new();
    for case in CASES {
        let sandbox_id = SandboxId::new();
        let seeded = seed(&fixture.pool, case, sandbox_id, source_id, other_id).await?;
        let placed_on = fixture
            .substrate
            .adopt_live_execution(
                sandbox_id,
                case.seed.read_lifecycle(),
                &source,
                GENERATION,
                case.started,
            )
            .await?
            .map(|node| node.id);
        let after = row(&fixture.pool, sandbox_id).await?;
        let (expected_row, expected_node) = match case.expect {
            Expect::Adopted(lifecycle) => (
                Row {
                    lifecycle: lifecycle.to_owned(),
                    placement: "on_node".to_owned(),
                    active_node_id: Some(source_id),
                    target_node_id: Some(source_id),
                    // Leaving `restoring` clears the token.
                    restore_lease_token: seeded
                        .restore_lease_token
                        .filter(|_| lifecycle == "restoring"),
                },
                Some(source_id),
            ),
            Expect::Refused => (seeded.clone(), seeded.active_node_id),
        };
        if (after.clone(), placed_on) != (expected_row.clone(), expected_node) {
            mismatches.push(format!(
                "{}: expected {expected_row:?} on {expected_node:?}, got {after:?} on {placed_on:?}",
                case.label
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The adoption reads the route identity and provenance rows without locking
/// them, as the single statement it replaced did: a writer holding both rows
/// in an open transaction does not delay it, and it decides on their
/// committed versions.
#[tokio::test(flavor = "current_thread")]
async fn adoption_does_not_wait_on_route_or_provenance_writers() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let source_id = NodeId::new();
    let other_id = NodeId::new();
    insert_ready_worker(&fixture.pool, source_id, "http://127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(&fixture.pool, other_id, "http://127.0.0.1:2", "9.0.0").await?;
    let source = fixture.substrate.direct_node_by_id(source_id).await?;
    let sandbox_id = SandboxId::new();
    let case = &CASES[0];
    seed(&fixture.pool, case, sandbox_id, source_id, other_id).await?;

    // Uncommitted writes that would refuse the adoption if it saw them.
    let mut holder = fixture.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_direct_route_identities SET current_node_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(other_id)
    .execute(&mut *holder)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_direct_route_provenance
            SET last_execution_generation = 1
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&mut *holder)
    .await?;

    let adopted = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.substrate.adopt_live_execution(
            sandbox_id,
            case.seed.read_lifecycle(),
            &source,
            GENERATION,
            case.started,
        ),
    )
    .await;
    holder.rollback().await?;
    let placed_on = adopted
        .expect("the adoption must not wait on the holder's row locks")?
        .map(|node| node.id);
    assert_eq!(placed_on, Some(source_id));
    assert_eq!(
        row(&fixture.pool, sandbox_id).await?.active_node_id,
        Some(source_id)
    );
    Ok(())
}
