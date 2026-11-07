//! [`KubernetesSubstrate::remove_leftover_upgrade_pod`] against a scripted
//! Kubernetes API: which pod at the execution's name is the upgrade's, when
//! it's deleted, and when it counts as gone.

use std::time::Duration;

use axum::http::{Method, StatusCode};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use pplx_space_common_core::kubernetes::SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION;
use sqlx::postgres::PgPoolOptions;

use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    KubeReply, kube_status, pod_path, pod_reply, requests_matching, scripted_kube_client,
};
use crate::controllers::substrate::kubernetes::stopped_evacuation::LeftoverStoppedUpgrade;

const POD: &str = "space-sbx-0190000000007000800000000000000a-g3";
const RECORDED_UID: &str = "upgrade-pod-uid";

fn operation() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_00aa)
}

/// The pod at [`POD`] with `uid`, carrying the upgrade's operation when
/// `carries`, and being deleted when `deleting`.
fn pod(uid: &str, carries: bool, deleting: bool) -> Pod {
    let annotations = carries.then(|| {
        std::collections::BTreeMap::from([(
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION.to_string(),
            operation().to_string(),
        )])
    });
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(POD.to_string()),
            namespace: Some("space".to_string()),
            uid: Some(uid.to_string()),
            annotations,
            deletion_timestamp: deleting.then(|| Time(jiff::Timestamp::now())),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// What the execution left recorded: [`RECORDED_UID`] when `recorded`.
fn leftover(recorded: bool) -> LeftoverStoppedUpgrade {
    LeftoverStoppedUpgrade {
        pod_name: POD.to_string(),
        pod_uid: recorded.then(|| RECORDED_UID.to_string()),
        operation_id: operation(),
        lease_live: false,
    }
}

/// A substrate whose pod reflector holds `cached`, and whose API answers a
/// read of [`POD`] with `read` and its delete with `delete`. No store: the
/// removal reads none.
fn substrate(
    cached: Vec<Pod>,
    read: impl Fn() -> KubeReply + Send + Sync + 'static,
    delete: impl Fn() -> KubeReply + Send + Sync + 'static,
) -> (
    KubernetesSubstrate,
    crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeLog,
) {
    let (client, log) = scripted_kube_client(move |request| {
        match (&request.method, request.path == pod_path(POD)) {
            (&Method::GET, true) => read(),
            (&Method::DELETE, true) => delete(),
            (method, _) => kube_status(
                StatusCode::BAD_REQUEST,
                &format!("unexpected Kubernetes request {method} {}", request.path),
            ),
        }
    });
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(1))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
        .expect("lazy pool should parse");
    (
        substrate_with_store_client_pods_and_nodes(pool, client, cached, Vec::new()),
        log,
    )
}

/// The UID precondition on every pod delete sent, in order.
fn deletes(
    log: &crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeLog,
) -> Vec<Option<String>> {
    requests_matching(log, &Method::DELETE)
        .iter()
        .map(|request| {
            request
                .body
                .as_ref()
                .and_then(|body| body.pointer("/preconditions/uid"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Expect {
    Gone,
    NotGoneYet,
    Unavailable,
}

/// Read through the API (the reflector has no pod there). A pod at the name
/// is the upgrade's when it has the UID the execution recorded or carries
/// the operation, and only then is it deleted, under its own UID. It counts
/// as gone only once the API no longer has it: a delete the API took, a pod
/// already being deleted, a delete refused for its UID precondition (another
/// pod took the name in between) and a 404 on the delete all wait for the
/// next read.
#[rstest::rstest]
#[case::no_pod(true, Some(None), None, Expect::Gone, None)]
#[case::recorded_uid(true, Some(Some((RECORDED_UID, true, false))), Some(StatusCode::OK), Expect::NotGoneYet, Some(RECORDED_UID))]
#[case::carries_operation(false, Some(Some(("other-uid", true, false))), Some(StatusCode::OK), Expect::NotGoneYet, Some("other-uid"))]
#[case::recorded_uid_without_operation(true, Some(Some((RECORDED_UID, false, false))), Some(StatusCode::OK), Expect::NotGoneYet, Some(RECORDED_UID))]
#[case::neither(true, Some(Some(("other-uid", false, false))), None, Expect::Gone, None)]
#[case::unrecorded_and_unmarked(false, Some(Some((RECORDED_UID, false, false))), None, Expect::Gone, None)]
#[case::already_deleting(true, Some(Some((RECORDED_UID, true, true))), None, Expect::NotGoneYet, None)]
#[case::precondition_failed(true, Some(Some((RECORDED_UID, true, false))), Some(StatusCode::CONFLICT), Expect::NotGoneYet, Some(RECORDED_UID))]
#[case::deleted_meanwhile(true, Some(Some((RECORDED_UID, true, false))), Some(StatusCode::NOT_FOUND), Expect::NotGoneYet, Some(RECORDED_UID))]
#[case::delete_failed(true, Some(Some((RECORDED_UID, true, false))), Some(StatusCode::INTERNAL_SERVER_ERROR), Expect::Unavailable, Some(RECORDED_UID))]
#[case::read_failed(true, None, None, Expect::Unavailable, None)]
#[tokio::test(flavor = "current_thread")]
async fn a_leftover_upgrade_pod_is_deleted_and_gone_only_when_its_the_upgrades(
    #[case] recorded: bool,
    #[case] at_the_name: Option<Option<(&'static str, bool, bool)>>,
    #[case] delete: Option<StatusCode>,
    #[case] expect: Expect,
    #[case] deleted_under: Option<&'static str>,
) {
    let read = move || match at_the_name {
        Some(Some((uid, carries, deleting))) => pod_reply(&pod(uid, carries, deleting)),
        Some(None) => kube_status(StatusCode::NOT_FOUND, "NotFound"),
        None => kube_status(StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
    };
    let (substrate, log) = substrate(Vec::new(), read, move || {
        let code = delete.expect("no delete was scripted");
        kube_status(code, if code.is_success() { "" } else { "Refused" })
    });

    let removed = substrate
        .remove_leftover_upgrade_pod(&leftover(recorded))
        .await;

    match (expect, &removed) {
        (Expect::Gone, Ok(true))
        | (Expect::NotGoneYet, Ok(false))
        | (Expect::Unavailable, Err(SubstrateError::Unavailable { .. })) => {}
        _ => panic!("expected {expect:?}, got {removed:?}"),
    }
    assert_eq!(
        deletes(&log),
        deleted_under
            .map(|uid| Some(uid.to_string()))
            .into_iter()
            .collect::<Vec<_>>()
    );
}

/// A pod the reflector holds is taken from it, with no API read; one it
/// doesn't hold is read from the API, so a pod the reflector hasn't seen yet
/// isn't taken as gone.
#[tokio::test(flavor = "current_thread")]
async fn a_leftover_upgrade_pod_is_read_from_the_reflector_or_else_the_api() {
    let (cached, log) = substrate(
        vec![pod(RECORDED_UID, true, false)],
        || kube_status(StatusCode::NOT_FOUND, "NotFound"),
        || kube_status(StatusCode::OK, ""),
    );
    assert!(
        !cached
            .remove_leftover_upgrade_pod(&leftover(true))
            .await
            .unwrap(),
        "the cached pod isn't gone"
    );
    assert!(requests_matching(&log, &Method::GET).is_empty());
    assert_eq!(deletes(&log), [Some(RECORDED_UID.to_string())]);

    let (uncached, log) = substrate(
        Vec::new(),
        || pod_reply(&pod(RECORDED_UID, true, false)),
        || kube_status(StatusCode::OK, ""),
    );
    assert!(
        !uncached
            .remove_leftover_upgrade_pod(&leftover(true))
            .await
            .unwrap(),
        "a pod only the API has isn't gone"
    );
    assert_eq!(requests_matching(&log, &Method::GET).len(), 1);
    assert_eq!(deletes(&log), [Some(RECORDED_UID.to_string())]);
}
