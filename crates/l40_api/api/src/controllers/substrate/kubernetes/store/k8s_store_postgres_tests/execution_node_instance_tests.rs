//! The EC2 instance an execution records for its pod's Node
//! (`sandbox_k8s_executions.node_instance_id`), through each store statement
//! that sets or clears the execution's node, and beside the statements an api
//! from before the column still issues.

use pplx_space_db_test_fixtures::SchemaVariant;

use super::*;

const NODE_A: &str = "space-test-instance-node-a";
const NODE_B: &str = "space-test-instance-node-b";

/// The `updated_at` every case starts from, so a write that dates the row
/// shows.
const UNDATED: &str = "2000-01-01T00:00:00Z";

/// An execution's node, its instance, and whether `updated_at` moved from
/// [`UNDATED`].
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct ExecutionNode {
    k8s_node_name: Option<String>,
    node_instance_id: Option<String>,
    dated: bool,
}

impl ExecutionNode {
    fn new(node: Option<&str>, instance: Option<&str>, dated: bool) -> Self {
        Self {
            k8s_node_name: node.map(str::to_string),
            node_instance_id: instance.map(str::to_string),
            dated,
        }
    }
}

async fn set_execution_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    node: Option<&str>,
    instance: Option<&str>,
) -> TestResult {
    let updated = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET k8s_node_name = $3, node_instance_id = $4, updated_at = $5::TIMESTAMPTZ
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(node)
    .bind(instance)
    .bind(UNDATED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1);
    Ok(())
}

async fn execution_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<ExecutionNode> {
    Ok(sqlx::query_as(sql!(
        "SELECT k8s_node_name, node_instance_id, updated_at <> $3::TIMESTAMPTZ AS dated
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(UNDATED)
    .fetch_one(pool)
    .await?)
}

/// Pod placement on `node_name`, whose Node names `instance_id`.
async fn place(
    store: &K8sStore,
    sandbox_id: SandboxId,
    node_name: &str,
    instance_id: Option<&str>,
) -> Result<(), SubstrateError> {
    let address = if node_name == NODE_A {
        "10.0.0.71"
    } else {
        "10.0.0.72"
    };
    store
        .persist_pod_node_placement(
            sandbox_id,
            node_name,
            "uid-placement",
            space_k8s_node_id(node_name, NodeRole::Worker.as_str()),
            NodeRole::Worker,
            Some(address),
            instance_id,
        )
        .await
}

struct PlacementCase {
    name: &'static str,
    before: (Option<&'static str>, Option<&'static str>),
    placed: (&'static str, Option<&'static str>),
    after: (&'static str, Option<&'static str>),
    /// The base dates the row only when its node changes.
    dated: bool,
}

const PLACEMENT_CASES: [PlacementCase; 8] = [
    PlacementCase {
        name: "a first placement records its Node's instance",
        before: (None, None),
        placed: (NODE_A, Some("i-0a")),
        after: (NODE_A, Some("i-0a")),
        dated: true,
    },
    PlacementCase {
        name: "a first placement on a Node that names no instance records none",
        before: (None, None),
        placed: (NODE_A, None),
        after: (NODE_A, None),
        dated: true,
    },
    PlacementCase {
        name: "the same node without an instance keeps the recorded one",
        before: (Some(NODE_A), Some("i-0a")),
        placed: (NODE_A, None),
        after: (NODE_A, Some("i-0a")),
        dated: false,
    },
    PlacementCase {
        name: "the same node on another instance keeps the first",
        before: (Some(NODE_A), Some("i-0a")),
        placed: (NODE_A, Some("i-0b")),
        after: (NODE_A, Some("i-0a")),
        dated: false,
    },
    PlacementCase {
        name: "the same node and instance again changes nothing",
        before: (Some(NODE_A), Some("i-0a")),
        placed: (NODE_A, Some("i-0a")),
        after: (NODE_A, Some("i-0a")),
        dated: false,
    },
    PlacementCase {
        name: "a node recorded without an instance gains it, undated",
        before: (Some(NODE_A), None),
        placed: (NODE_A, Some("i-0b")),
        after: (NODE_A, Some("i-0b")),
        dated: false,
    },
    PlacementCase {
        name: "another node replaces the instance",
        before: (Some(NODE_A), Some("i-0a")),
        placed: (NODE_B, Some("i-0b")),
        after: (NODE_B, Some("i-0b")),
        dated: true,
    },
    PlacementCase {
        name: "another node that names no instance clears it",
        before: (Some(NODE_A), Some("i-0a")),
        placed: (NODE_B, None),
        after: (NODE_B, None),
        dated: true,
    },
];

#[tokio::test]
async fn pod_placement_records_the_instance_once_per_node() -> TestResult {
    let fixture = postgres_store().await?;
    for case in PLACEMENT_CASES {
        let sandbox_id = insert_placeable_sandbox(&fixture).await?;
        set_execution_node(&fixture.pool, sandbox_id, 1, case.before.0, case.before.1).await?;

        place(&fixture.store, sandbox_id, case.placed.0, case.placed.1).await?;

        assert_eq!(
            execution_node(&fixture.pool, sandbox_id, 1).await?,
            ExecutionNode::new(Some(case.after.0), case.after.1, case.dated),
            "{}",
            case.name
        );
    }
    Ok(())
}

/// Placement writes only the active execution: a retired one keeps what its
/// own pod recorded, both beside a new execution and when none is active.
#[tokio::test]
async fn pod_placement_never_records_an_instance_on_a_retired_execution() -> TestResult {
    let fixture = postgres_store().await?;
    let retire = |sandbox_id| {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE, state = 'replaced', terminated_at = NOW()
             WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&fixture.pool)
    };

    let replaced = insert_placeable_sandbox(&fixture).await?;
    set_execution_node(&fixture.pool, replaced, 1, Some(NODE_A), None).await?;
    retire(replaced).await?;
    fixture.store.ensure_active_execution_row(replaced).await?;
    set_execution_node(&fixture.pool, replaced, 2, None, None).await?;
    place(&fixture.store, replaced, NODE_A, Some("i-0b")).await?;
    assert_eq!(
        execution_node(&fixture.pool, replaced, 1).await?,
        ExecutionNode::new(Some(NODE_A), None, false),
        "the retired execution"
    );
    assert_eq!(
        execution_node(&fixture.pool, replaced, 2).await?,
        ExecutionNode::new(Some(NODE_A), Some("i-0b"), true),
        "its successor"
    );

    let retired = insert_placeable_sandbox(&fixture).await?;
    set_execution_node(&fixture.pool, retired, 1, Some(NODE_A), None).await?;
    retire(retired).await?;
    place(&fixture.store, retired, NODE_A, Some("i-0b")).await?;
    assert_eq!(
        execution_node(&fixture.pool, retired, 1).await?,
        ExecutionNode::new(Some(NODE_A), None, false),
        "no active execution"
    );
    Ok(())
}

/// The statements that take an execution's pod away.
#[derive(Debug, Clone, Copy)]
enum PodUnbinding {
    /// `claim_active_execution_pod_materialization`, before a new pod is
    /// created for the execution. It keeps the node name.
    MaterializationClaim,
    /// `converge_failed_recovery_after_pod_absence_if_generation`, once a
    /// failed recovery's pod is gone. It clears the node name.
    FailedRecoveryPodAbsence,
}

impl PodUnbinding {
    const ALL: [Self; 2] = [Self::MaterializationClaim, Self::FailedRecoveryPodAbsence];

    async fn unbind(self, fixture: &PostgresStore, sandbox_id: SandboxId) -> TestResult {
        match self {
            Self::MaterializationClaim => {
                assert!(
                    fixture
                        .store
                        .claim_active_execution_pod_materialization(
                            sandbox_id,
                            1,
                            SandboxLifecycle::Started,
                        )
                        .await?
                );
            }
            Self::FailedRecoveryPodAbsence => {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions SET state = 'recovery_failed'
                     WHERE sandbox_id = $1 AND generation = 1"
                ))
                .bind(sandbox_id)
                .execute(&fixture.pool)
                .await?;
                let outcome = fixture
                    .store
                    .converge_failed_recovery_after_pod_absence_if_generation(
                        sandbox_id,
                        1,
                        Some("pod-uid-1"),
                        Some("10.90.0.1"),
                        Some(NODE_A),
                    )
                    .await?;
                assert_ne!(outcome, FailedRecoveryPodAbsenceOutcome::NotMatched);
            }
        }
        Ok(())
    }

    fn node_after(self) -> Option<&'static str> {
        match self {
            Self::MaterializationClaim => Some(NODE_A),
            Self::FailedRecoveryPodAbsence => None,
        }
    }
}

/// The instance belongs to the pod: a statement that takes the pod away
/// clears it, so the execution's next pod records its own, here a
/// replacement Node under the same name.
#[tokio::test]
async fn statements_that_take_the_pod_away_clear_its_instance() -> TestResult {
    let fixture = postgres_store().await?;
    for unbinding in PodUnbinding::ALL {
        let sandbox_id = insert_placeable_sandbox(&fixture).await?;
        set_execution_node(&fixture.pool, sandbox_id, 1, Some(NODE_A), Some("i-0a")).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = 'pod-uid-1', pod_ip = '10.90.0.1'::INET, state = 'running'
             WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .execute(&fixture.pool)
        .await?;

        unbinding.unbind(&fixture, sandbox_id).await?;
        let after = execution_node(&fixture.pool, sandbox_id, 1).await?;
        assert_eq!(
            (after.k8s_node_name.as_deref(), after.node_instance_id),
            (unbinding.node_after(), None),
            "{unbinding:?}"
        );

        place(&fixture.store, sandbox_id, NODE_A, Some("i-0b")).await?;
        assert_eq!(
            execution_node(&fixture.pool, sandbox_id, 1)
                .await?
                .node_instance_id
                .as_deref(),
            Some("i-0b"),
            "{unbinding:?}: the next pod's placement"
        );
    }
    Ok(())
}

/// `update_active_execution_node_name` as the release before the column
/// issues it.
const PRE_COLUMN_PLACEMENT: &str = sql!(
    "WITH updated AS (
        UPDATE sandbox_k8s_executions
        SET k8s_node_name = $1::TEXT,
            updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
        WHERE sandbox_id = $2 AND active
          AND k8s_node_name IS DISTINCT FROM $1::TEXT
        RETURNING 1
     )
     SELECT EXISTS (
        SELECT 1
        FROM sandbox_k8s_executions
        WHERE sandbox_id = $2 AND active
     )"
);

/// `persist_pooled_execution_claim`'s insert as the release before the column
/// issues it.
const PRE_COLUMN_POOL_BIND: &str = sql!(
    "INSERT INTO sandbox_k8s_executions (
         sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
         k8s_node_name, state, active, route_activation_id, pod_materialized_at
     )
     VALUES (
         $1, 1, $2, $3, $4, $5::inet, $6, 'running', TRUE, $7,
         CASE WHEN NULLIF(BTRIM($4::TEXT), '') IS NOT NULL THEN NOW() END
     )"
);

const ADD_COLUMN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../db/postgres/20261107040432_execution_node_instance_id.up.sql"
));
const DROP_COLUMN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../db/postgres/20261107040432_execution_node_instance_id.down.sql"
));

async fn migrate(pool: &PgPool, migration: &'static str) -> TestResult {
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(migration).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// An api from before the column and one that writes it, on one execution,
/// over one connection whose statements were prepared before the column
/// existed. The older api's statements keep working once it exists and never
/// touch it, and this one fills an instance the older api left unknown
/// without clearing one it recorded.
#[tokio::test]
async fn an_api_from_before_the_column_and_this_one_share_an_execution() -> TestResult {
    if SchemaVariant::from_env()?.is_some() {
        // The migration names the table a variant turns into a view; the
        // variant's own pins prepare statements before its rename.
        return Ok(());
    }
    let fixture = postgres_store_with_max_connections(1).await?;
    let backend_pid = || sqlx::query_scalar::<_, i32>(sql!("SELECT pg_backend_pid()"));
    let pid = backend_pid().fetch_one(&fixture.pool).await?;
    migrate(&fixture.pool, DROP_COLUMN).await?;

    // The older api binds a pooled sandbox's execution to its slot's pod on
    // node A, and places it there again.
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    sqlx::query(PRE_COLUMN_POOL_BIND)
        .bind(sandbox_id)
        .bind(K8sStore::execution_pod_name(sandbox_id, 1))
        .bind("space")
        .bind("pod-uid-1")
        .bind("10.90.0.1")
        .bind(NODE_A)
        .bind(Uuid::now_v7())
        .execute(&fixture.pool)
        .await?;
    let pre_column_placement = || {
        sqlx::query_scalar::<_, bool>(PRE_COLUMN_PLACEMENT)
            .bind(NODE_A)
            .bind(sandbox_id)
            .fetch_one(&fixture.pool)
    };
    assert!(pre_column_placement().await?);
    // A reader with an explicit column list, and the `SELECT *` none of them
    // uses, both prepared before the column.
    let select_all = sql!("SELECT * FROM sandbox_k8s_executions WHERE sandbox_id = $1");
    sqlx::query(select_all)
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    let mut conn = fixture.pool.acquire().await?;
    let row = fixture
        .store
        .active_execution_row(&mut conn, sandbox_id)
        .await?
        .expect("the bound execution is active");
    drop(conn);

    migrate(&fixture.pool, ADD_COLUMN).await?;
    assert_eq!(
        backend_pid().fetch_one(&fixture.pool).await?,
        pid,
        "every statement ran on the one connection"
    );
    let error = sqlx::query(select_all)
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await
        .expect_err("a cached SELECT * changes its row type");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("0A000")
    );
    let mut conn = fixture.pool.acquire().await?;
    assert_eq!(
        fixture
            .store
            .active_execution_row(&mut conn, sandbox_id)
            .await?
            .map(|again| again.generation),
        Some(row.generation)
    );
    drop(conn);
    assert!(pre_column_placement().await?);
    set_execution_node(&fixture.pool, sandbox_id, 1, Some(NODE_A), None).await?;

    // This api places the execution with no instance, then with one: it never
    // invents an instance and fills the one the older api left unknown.
    place(&fixture.store, sandbox_id, NODE_A, None).await?;
    assert_eq!(
        execution_node(&fixture.pool, sandbox_id, 1).await?,
        ExecutionNode::new(Some(NODE_A), None, false)
    );
    place(&fixture.store, sandbox_id, NODE_A, Some("i-0a")).await?;
    // The older api places it on the same node again: no write, so the
    // instance stays.
    assert!(pre_column_placement().await?);
    assert_eq!(
        execution_node(&fixture.pool, sandbox_id, 1).await?,
        ExecutionNode::new(Some(NODE_A), Some("i-0a"), false)
    );
    Ok(())
}
