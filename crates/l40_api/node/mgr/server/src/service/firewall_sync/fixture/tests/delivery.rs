//! The whole delivery path: a round opened in Postgres is fetched by node-ctl
//! through the reviewed cluster API, answered by this binding over its socket,
//! and reported; node-ctl's fault cases are its own tests, and the server's
//! the Report's.

use std::ffi::OsStr;

use pplx_space_firewall_rules_store::test_support::{rollout, save_targets};
use pplx_space_firewall_rules_store::{
    Issuance, Opening, Wakeup, fetch_instructions, issue_first_assignment, lease_targets,
    open_round,
};
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};
use pplx_space_rpc_auth::test_support::{pod_bound_service_account_status, projected_token};
use pplx_space_sql_locator::sql;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::proto::node_manager_server::NodeManagerServer;

const TOKEN_PATH: &str = "SPACE_CLUSTER_API_TOKEN_PATH";

/// Queues nothing: node-ctl's startup Fetch finds the round.
struct Unqueued;

impl Wakeup for Unqueued {
    async fn wake(
        &mut self,
        _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        _node: NodeId,
    ) -> Result<u64, sqlx::Error> {
        Ok(1)
    }
}

/// `NODE`'s observation row and its rollout item, as JSON.
async fn rows(pool: &sqlx::PgPool) -> (serde_json::Value, String) {
    let observation = sqlx::query_scalar::<_, String>(sql!(
        "SELECT row_to_json(o)::text FROM firewall_rules_node_observations o WHERE node_id = $1"
    ))
    .bind(NODE)
    .fetch_one(pool)
    .await
    .unwrap();
    let item = sqlx::query_scalar::<_, String>(sql!(
        "SELECT row_to_json(i)::text FROM firewall_rules_rollout_items i WHERE node_id = $1"
    ))
    .bind(NODE)
    .fetch_one(pool)
    .await
    .unwrap();
    (serde_json::from_str(&observation).unwrap(), item)
}

// Catches a pending round left for a wake, evidence lost or made favorable on
// the way, an answer not stored for its round, or a report that touches the
// rollout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_round_is_answered_with_the_managers_unfavorable_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let token = dir.path().join("cluster-api.token");
    // node-ctl's real client reads both from its process.
    let child = pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        (TOKEN_PATH, Some(token.as_os_str())),
    ]);
    if !child {
        return;
    }
    let token = PathBuf::from(std::env::var_os(TOKEN_PATH).unwrap());
    let db = pplx_space_db_test_fixtures::pg_test_db().await.unwrap();
    let pool = &db.pool;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '192.0.2.10'::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(NODE)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'worker-a', 'uid-a')"
    ))
    .bind(NODE)
    .execute(pool)
    .await
    .unwrap();
    upsert_snapshot(pool, NODE, SnapshotSeed::default())
        .await
        .unwrap();
    let rollout = rollout(pool, "rolling").await.unwrap();
    save_targets(pool, rollout, &[NODE]).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let lease = Duration::from_secs(600);
    let target = lease_targets(&mut conn, rollout, 1, lease, 1)
        .await
        .unwrap()
        .remove(0);
    let check_in = Duration::from_secs(90);
    let issued = issue_first_assignment(&mut conn, &target, check_in, &mut Unqueued).await;
    assert!(matches!(issued, Ok(Issuance::Issued(_))), "{issued:?}");
    let Ok(Opening::Opened(round)) = open_round(&mut conn, &target, EPOCH, &mut Unqueued).await
    else {
        panic!("no round was opened");
    };
    let (_, item) = rows(pool).await;

    // Slot 1 is bound; leased slot 0 and idle slot 2 keep another ruleset, and
    // one managed namespace is no slot's.
    use SlotState::{Leased, Ready};
    let states = [Leased, Leased, Ready];
    let bound = bind_slots(BoundSlots::One(SLOT), None, None, WAIT, &states).await;
    for slot in [0, 2] {
        let mut tables = bound.host.tables.lock().unwrap();
        tables.insert(bound.namespaces[slot], installed(11));
    }
    bound.host.name(7, "mnt");
    let manager_socket = dir.path().join("node-mgr.sock");
    let manager = tokio::net::UnixListener::bind(&manager_socket).unwrap();
    let stamp = |mut request: Request<()>| {
        request.extensions_mut().insert(node_ctl());
        Ok(request)
    };
    let service = NodeManagerServer::with_interceptor(bound.service.clone(), stamp);
    let _manager = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(manager)),
    ));
    let bearer = projected_token("node-ctl-a");
    std::fs::write(&token, &bearer).unwrap();
    let review = move |spec: k8s_openapi::api::authentication::v1::TokenReviewSpec| {
        assert_eq!(spec.token.as_deref(), Some(bearer.as_str()));
        let node_ctl = "system:serviceaccount:space:space-node-ctl";
        pod_bound_service_account_status("space-cluster-api", node_ctl, "worker-a", "uid-a")
    };
    let cluster = pplx_space_internal_api_server::test_support::serve_firewall_rules(
        pool.clone(),
        vec![NODE],
        review,
    )
    .await;
    let shutdown = CancellationToken::new();
    let ctl = tokio::net::UnixListener::bind(dir.path().join("node-ctl.sock")).unwrap();
    let node_ctl = pplx_space_node_ctl_server::serve_firewall_wake_for_test(
        ctl,
        cluster.endpoint.clone(),
        &manager_socket,
        shutdown.clone(),
    );
    // No wake is sent: the round was pending before node-ctl started.
    let answered = async {
        loop {
            let instructions = fetch_instructions(&mut conn, NODE).await.unwrap();
            if instructions.pending_round.is_none() {
                shutdown.cancel();
                return instructions;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let (served, instructions) = tokio::join!(node_ctl, bounded(answered));
    served.unwrap();
    let assignment = instructions.assignment.unwrap();
    let (observation, item_after) = rows(pool).await;
    assert_eq!(observation["accepted_round"], round.round);
    let slot = |slot_id, outcome: &str, reason: Option<&str>| {
        let mut slot =
            serde_json::json!({"slot_id": slot_id, "slot_generation": 1, "outcome": outcome});
        if let Some(reason) = reason {
            slot["reason"] = reason.into();
        }
        slot
    };
    assert_eq!(
        observation["accepted_report"],
        serde_json::json!({
            "schema_version": 1,
            "round": round.round,
            "assignment_generation": round.assignment_generation,
            "revision_id": assignment.revision_id.to_string(),
            "manager_epoch": EPOCH,
            "census": "incomplete",
            "census_gaps": [],
            "extra_namespaces": 1,
            "slots": [
                slot(0, "failed", Some("rules_mismatch")),
                slot(1, "verified", None),
                slot(2, "failed", Some("rules_mismatch")),
            ],
        })
    );
    assert_eq!(item_after, item, "the report moved the rollout");
}
