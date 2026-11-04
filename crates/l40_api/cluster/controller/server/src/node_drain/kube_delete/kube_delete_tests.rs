//! The identity guards around the kube Node delete, against a fake API server
//! that records every request. A terminator that deletes the wrong Node takes
//! out a healthy replacement, and one that refuses a Node already gone leaves
//! the row stuck in `terminating`, so each branch is pinned by what reached the
//! API server as well as by what `terminate` returned.

use pplx_space_common_core::types::NodeId;

use std::sync::{Arc, Mutex};

use kube::client::{Body as KubeBody, ClientBuilder};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use serde_json::{Value, json};
use tonic::codegen::http::{Method, Request, Response, StatusCode};

use super::*;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

const NODE_NAME: &str = "worker-a";
const NODE_PATH: &str = "/api/v1/nodes/worker-a";

/// Every request the fake API server answered: method, path, and JSON body.
type Seen = Arc<Mutex<Vec<(Method, String, Option<Value>)>>>;

/// A kube client whose Node GET and DELETE answer with the given replies.
fn fake_apiserver(get: (StatusCode, Value), delete: (StatusCode, Value)) -> (kube::Client, Seen) {
    let seen = Seen::default();
    let recorded = Arc::clone(&seen);
    let service = tower::service_fn(move |request: Request<KubeBody>| {
        let recorded = Arc::clone(&recorded);
        let (get, delete) = (get.clone(), delete.clone());
        async move {
            let method = request.method().clone();
            let path = request.uri().path().to_string();
            let body = http_body_util::BodyExt::collect(request.into_body())
                .await?
                .to_bytes();
            let body = (!body.is_empty())
                .then(|| serde_json::from_slice::<Value>(&body))
                .transpose()?;
            recorded
                .lock()
                .map_err(|_| "request log poisoned")?
                .push((method.clone(), path, body));
            let (status, reply) = match method {
                Method::GET => get,
                Method::DELETE => delete,
                _ => failure(StatusCode::METHOD_NOT_ALLOWED),
            };
            Ok::<_, BoxError>(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(KubeBody::from(serde_json::to_vec(&reply)?))?,
            )
        }
    });
    (ClientBuilder::new(service, "default").build(), seen)
}

fn node(uid: &str) -> (StatusCode, Value) {
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": NODE_NAME, "uid": uid },
    });
    (StatusCode::OK, node)
}

fn deleted() -> (StatusCode, Value) {
    let status = json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Success",
        "code": 200,
    });
    (StatusCode::OK, status)
}

fn failure(code: StatusCode) -> (StatusCode, Value) {
    let status = json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Failure",
        "message": format!("injected {code}"),
        "reason": code.canonical_reason().unwrap_or_default(),
        "code": code.as_u16(),
    });
    (code, status)
}

fn requests(seen: &Seen) -> Vec<(Method, String)> {
    seen.lock()
        .expect("request log poisoned")
        .iter()
        .map(|(method, path, _)| (method.clone(), path.clone()))
        .collect()
}

fn get_only() -> Vec<(Method, String)> {
    vec![(Method::GET, NODE_PATH.to_string())]
}

fn get_then_delete() -> Vec<(Method, String)> {
    vec![
        (Method::GET, NODE_PATH.to_string()),
        (Method::DELETE, NODE_PATH.to_string()),
    ]
}

async fn seed_node(pool: &PgPool, phase: Option<&str>) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.50'::inet, 'worker', 'k8s', 'registered', $2, NOW())"
    ))
    .bind(node_id)
    .bind(phase)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn seed_node_k8s(pool: &PgPool, node_id: NodeId, uid: Option<&str>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(NODE_NAME)
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `terminating` node whose `node_k8s` row names [`NODE_NAME`] under `uid`.
async fn terminating_node(pool: &PgPool, uid: Option<&str>) -> TestResult<NodeId> {
    let node_id = seed_node(pool, Some("terminating")).await?;
    seed_node_k8s(pool, node_id, uid).await?;
    Ok(node_id)
}

#[tokio::test(flavor = "current_thread")]
async fn deletes_the_node_it_fetched_under_that_nodes_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = terminating_node(&db.pool, Some("uid-a")).await?;
    let (kube, seen) = fake_apiserver(node("uid-a"), deleted());

    KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
        .terminate(node_id)
        .await?;

    assert_eq!(requests(&seen), get_then_delete());
    // Without the precondition, a same-named Node that kubelet registers
    // between the GET and the DELETE is the one the API server deletes.
    let delete_body = seen.lock().expect("request log poisoned")[1].2.clone();
    assert_eq!(
        delete_body
            .as_ref()
            .map(|body| &body["preconditions"]["uid"]),
        Some(&json!("uid-a")),
        "DELETE must carry the fetched Node's UID as a precondition; body was {delete_body:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_row_with_no_stored_uid_is_deleted_under_the_live_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = terminating_node(&db.pool, None).await?;
    let (kube, seen) = fake_apiserver(node("uid-live"), deleted());

    KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
        .terminate(node_id)
        .await?;

    assert_eq!(requests(&seen), get_then_delete());
    let delete_body = seen.lock().expect("request log poisoned")[1].2.clone();
    assert_eq!(
        delete_body
            .as_ref()
            .map(|body| &body["preconditions"]["uid"]),
        Some(&json!("uid-live")),
        "with nothing stored to compare, the live UID still guards the delete; body was {delete_body:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_already_gone_counts_as_terminated() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = terminating_node(&db.pool, Some("uid-a")).await?;
    let (kube, seen) = fake_apiserver(failure(StatusCode::NOT_FOUND), deleted());

    KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
        .terminate(node_id)
        .await?;

    assert_eq!(requests(&seen), get_only());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_same_named_replacement_is_left_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = terminating_node(&db.pool, Some("uid-a")).await?;
    let (kube, seen) = fake_apiserver(node("uid-replacement"), deleted());

    let result = KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
        .terminate(node_id)
        .await;

    assert!(
        matches!(result, Err(TerminateError::StaleIdentity)),
        "a Node whose UID differs from the stored one is a replacement; got {result:?}"
    );
    assert_eq!(requests(&seen), get_only());
    Ok(())
}

/// What the DELETE's own answer means, once the GET has matched: the UID
/// precondition failing is a replacement, a Node gone in between is done, and
/// anything else (the missing-RBAC 403 the module doc warns about) is the
/// backend's failure to record.
#[tokio::test(flavor = "current_thread")]
async fn the_delete_answer_decides_the_outcome() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (answer, expected) in [
        (StatusCode::CONFLICT, "StaleIdentity"),
        (StatusCode::NOT_FOUND, "Ok"),
        (StatusCode::FORBIDDEN, "Provider"),
    ] {
        let node_id = terminating_node(&db.pool, Some("uid-a")).await?;
        let (kube, seen) = fake_apiserver(node("uid-a"), failure(answer));

        let result = KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
            .terminate(node_id)
            .await;

        let outcome = match &result {
            Ok(()) => "Ok",
            Err(TerminateError::StaleIdentity) => "StaleIdentity",
            Err(TerminateError::Provider(_)) => "Provider",
            Err(_) => "other",
        };
        assert_eq!(
            outcome, expected,
            "DELETE answered {answer}; got {result:?}"
        );
        assert_eq!(
            requests(&seen),
            get_then_delete(),
            "DELETE answered {answer}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_node_lookup_deletes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = terminating_node(&db.pool, Some("uid-a")).await?;
    let (kube, seen) = fake_apiserver(failure(StatusCode::INTERNAL_SERVER_ERROR), deleted());

    let result = KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
        .terminate(node_id)
        .await;

    assert!(
        matches!(result, Err(TerminateError::KubeLookup(_))),
        "an unreadable Node cannot pass the UID guard; got {result:?}"
    );
    assert_eq!(requests(&seen), get_only());
    Ok(())
}

/// The row re-fetch is the admin-undrain guard: a node that left `terminating`
/// after it was listed must not reach the API server at all, even though its
/// `node_k8s` row would resolve.
#[tokio::test(flavor = "current_thread")]
async fn a_node_no_longer_terminating_is_never_sent_to_kube() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for phase in [None, Some("draining"), Some("drained")] {
        let node_id = seed_node(&db.pool, phase).await?;
        seed_node_k8s(&db.pool, node_id, Some("uid-a")).await?;
        let (kube, seen) = fake_apiserver(node("uid-a"), deleted());

        let result = KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
            .terminate(node_id)
            .await;

        assert!(
            matches!(result, Err(TerminateError::StaleIdentity)),
            "drain_phase {phase:?}: got {result:?}"
        );
        assert_eq!(requests(&seen), Vec::new(), "drain_phase {phase:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unresolvable_row_is_a_lookup_error_before_any_kube_call() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let vanished = NodeId::new();
    let no_kube_name = seed_node(&db.pool, Some("terminating")).await?;

    for (node_id, case) in [
        (vanished, "no nodes row"),
        (no_kube_name, "no node_k8s row"),
    ] {
        let (kube, seen) = fake_apiserver(node("uid-a"), deleted());

        let result = KubeNodeDeleteTerminator::new(db.pool.clone(), kube)
            .terminate(node_id)
            .await;

        assert!(
            matches!(result, Err(TerminateError::KubeLookup(_))),
            "{case}: got {result:?}"
        );
        assert_eq!(requests(&seen), Vec::new(), "{case}");
    }
    Ok(())
}
