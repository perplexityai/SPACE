//! mkdir, remove, rename, change tracking and modified: each is prepared with
//! the fingerprint its key is bound to, and reaches the sandbox only once the
//! API has bound it.

use pplx_space_guest_process::GuestUserName;
use pplx_space_native_files::{
    FileOperation, mkdir_fingerprint, remove_fingerprint, rename_fingerprint,
};
use pplx_space_sbx_mgr_client::operations::{
    DisableFileChangesResponse, EnableFileChangesResponse, GetModifiedFilesResponse, MkdirResponse,
    ModifiedFile, RemoveResponse, RenameResponse,
};

use super::fakes::IDEMPOTENCY_CONFLICT;
use super::*;

impl Harness {
    fn prepared(&self) -> Vec<fakes::AccessCall> {
        self.api
            .calls()
            .into_iter()
            .filter(|call| call.rpc == "prepare")
            .collect()
    }
}

fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

#[tokio::test]
async fn mutations_answer_through_the_granted_route_in_the_apis_shapes() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    harness
        .worker
        .reply(Reply::Mkdir(Ok(MkdirResponse::default())));
    harness
        .worker
        .reply(Reply::Remove(Ok(RemoveResponse::default())));
    harness
        .worker
        .reply(Reply::Rename(Ok(RenameResponse::default())));
    harness
        .worker
        .reply(Reply::EnableFileChanges(Ok(EnableFileChangesResponse {
            observed_since: Some(timestamp(1_700_000_000)),
        })));
    harness.worker.reply(Reply::DisableFileChanges(Ok(
        DisableFileChangesResponse::default(),
    )));
    harness
        .worker
        .reply(Reply::GetModifiedFiles(Ok(GetModifiedFilesResponse {
            files: vec![ModifiedFile {
                path: "/workspace/a.txt".to_owned(),
                root: "/workspace".to_owned(),
                last_modified_at: Some(timestamp(1_700_000_100)),
                kinds: vec![pplx_space_sbx_mgr_client::proto::SpacedFileEventKind::Modified as i32],
            }],
            observed_since: Some(timestamp(1_700_000_000)),
            monitoring_enabled: true,
            cursor: Some("next".to_owned()),
            ..GetModifiedFilesResponse::default()
        })));

    let answers = [
        harness.post_keyed(
            "mkdir",
            "k-mkdir",
            json!({"path": "/workspace/d", "parents": true, "user": "sandbox"}),
        ),
        harness.post_keyed(
            "remove",
            "k-remove",
            json!({"path": "/workspace/old", "recursive": true}),
        ),
        harness.post_keyed(
            "rename",
            "k-rename",
            json!({"old_path": "/workspace/a", "new_path": "/workspace/b"}),
        ),
        harness.post_keyed(
            "changes_enable",
            "k-enable",
            json!({"roots": ["/workspace"], "include_globs": ["*.rs"]}),
        ),
        // Neither tracking call needs a body.
        Request::post(harness.path("changes_disable"))
            .header("idempotency-key", "k-disable")
            .body(Body::empty())
            .unwrap(),
        harness.post("modified", json!({"window_seconds": 60})),
    ];
    let mut bodies = Vec::new();
    for request in answers {
        let (status, _, body) = read_body(harness.send(request).await).await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        bodies.push(serde_json::from_slice::<Value>(&body).unwrap());
    }
    assert_eq!(bodies[0], json!({"ok": true}));
    assert_eq!(bodies[1], json!({"ok": true}));
    assert_eq!(bodies[2], json!({"ok": true}));
    assert_eq!(
        bodies[3],
        json!({"monitoring_enabled": true, "observed_since": "2023-11-14T22:13:20Z"})
    );
    assert_eq!(bodies[4], json!({"ok": true}));
    assert_eq!(bodies[5]["files"][0]["path"], "/workspace/a.txt");
    assert_eq!(bodies[5]["files"][0]["kinds"], json!(["modified"]));
    assert_eq!(bodies[5]["cursor"], "next");

    // The sandbox got the guest requests the API's handlers send, for the
    // granted sandbox.
    let sandbox_id = harness.api.sandbox_id.to_string();
    let calls = harness.worker.calls();
    assert_eq!(calls.len(), 6);
    let mut rename_key = None;
    for call in &calls {
        assert_eq!(call.route_token.as_deref(), Some(ROUTE_TOKEN));
        match &call.request {
            WorkerRequest::Mkdir(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
                let operation = request.operation.as_ref().unwrap();
                assert_eq!(operation.path, "/workspace/d");
                assert!(operation.parents && operation.exist_ok);
                assert_eq!(operation.user.as_deref(), Some("sandbox"));
            }
            WorkerRequest::Remove(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
                let operation = request.operation.as_ref().unwrap();
                assert!(operation.recursive && operation.missing_ok);
            }
            WorkerRequest::Rename(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
                rename_key = request.operation.as_ref().unwrap().idempotency_key.clone();
            }
            WorkerRequest::EnableFileChanges(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
                let operation = request.operation.as_ref().unwrap();
                assert_eq!(operation.roots, ["/workspace"]);
                assert_eq!(operation.include_globs, ["*.rs"]);
            }
            WorkerRequest::DisableFileChanges(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
            }
            WorkerRequest::GetModifiedFiles(request) => {
                assert_eq!(request.sandbox_id, sandbox_id);
                let window = request.operation.as_ref().unwrap().window.as_ref().unwrap();
                assert_eq!(window.seconds, 60);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    // Each bound operation was prepared with the fingerprint the API's own
    // handler computes for its body; the others with none.
    let prepared = harness.prepared();
    let fingerprints: Vec<_> = prepared
        .iter()
        .map(|call| call.fingerprint.clone())
        .collect();
    let sandbox = GuestUserName::parse("sandbox").unwrap();
    assert_eq!(
        fingerprints,
        [
            Some(
                mkdir_fingerprint("/workspace/d", true, Some(&sandbox))
                    .unwrap()
                    .as_str()
                    .to_owned()
            ),
            Some(
                remove_fingerprint("/workspace/old", true, None)
                    .unwrap()
                    .as_str()
                    .to_owned()
            ),
            Some(
                rename_fingerprint("/workspace/a", "/workspace/b", None)
                    .unwrap()
                    .as_str()
                    .to_owned()
            ),
            None,
            None,
            None,
        ]
    );
    // The rename carried the guest key the API derived from the key it bound
    // and that fingerprint.
    let expected = pplx_space_native_files::guest_key(
        FileOperation::Rename.route(),
        &pplx_space_guest_process::IdempotencyKey::parse("k-rename").unwrap(),
        &rename_fingerprint("/workspace/a", "/workspace/b", None).unwrap(),
    )
    .unwrap();
    assert_eq!(rename_key.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn a_changed_request_under_a_bound_key_never_reaches_the_sandbox() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    let rename = |new_path: &str| {
        harness.post_keyed(
            "rename",
            "k-1",
            json!({"old_path": "/workspace/a", "new_path": new_path}),
        )
    };
    harness
        .worker
        .reply(Reply::Rename(Ok(RenameResponse::default())));
    harness
        .worker
        .reply(Reply::Rename(Ok(RenameResponse::default())));
    assert_eq!(
        harness.send(rename("/workspace/b")).await.status(),
        StatusCode::OK
    );
    // A retry of the same request is prepared with the same fingerprint, so
    // the sandbox replays it by the same guest key.
    assert_eq!(
        harness.send(rename("/workspace/b")).await.status(),
        StatusCode::OK
    );
    let keys: Vec<_> = harness
        .worker
        .calls()
        .iter()
        .map(|call| match &call.request {
            WorkerRequest::Rename(request) => request.operation.clone().unwrap().idempotency_key,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(keys.len(), 2);
    assert!(keys[0].is_some());
    assert_eq!(keys[0], keys[1]);

    // A different request under the key is refused as the API refuses it,
    // and never reaches the sandbox.
    let (status, _, body) = read_body(harness.send(rename("/workspace/c")).await).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.as_ref(), IDEMPOTENCY_CONFLICT);
    for (action, first, changed) in [
        (
            "mkdir",
            json!({"path": "/d"}),
            json!({"path": "/d", "parents": false}),
        ),
        (
            "remove",
            json!({"path": "/r"}),
            json!({"path": "/r", "recursive": true}),
        ),
    ] {
        harness.worker.reply(match action {
            "mkdir" => Reply::Mkdir(Ok(MkdirResponse::default())),
            _ => Reply::Remove(Ok(RemoveResponse::default())),
        });
        let response = harness.send(harness.post_keyed(action, "k-2", first)).await;
        assert_eq!(response.status(), StatusCode::OK, "{action}");
        let (status, _, body) = read_body(
            harness
                .send(harness.post_keyed(action, "k-2", changed))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{action}: {body:?}");
    }
    assert_eq!(harness.worker.calls().len(), 4);

    // A rename the API granted without a guest key is not sent: without the
    // key the sandbox could not replay it.
    harness.api.prepare(fakes::Answer::Ok(harness.api.grant()));
    let response = harness
        .send(harness.post_keyed(
            "rename",
            "k-3",
            json!({"old_path": "/workspace/a", "new_path": "/workspace/b"}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(harness.worker.calls().len(), 4);

    // A request without its key is refused before it is prepared.
    let before = harness.prepared().len();
    let response = harness
        .send(harness.post("mkdir", json!({"path": "/d"})))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(harness.prepared().len(), before);
    assert_eq!(harness.worker.calls().len(), 4);
}

/// How each operation's sandbox call recovers, as the API's spaced client
/// does: every one reconnects once through a fresh preparation when the
/// node-api drains, none after a closed connection, and the answer to that
/// closed connection invites a retry only for an operation the sandbox
/// replays by key or that changes nothing.
#[tokio::test]
async fn each_mutation_reconnects_only_when_the_node_api_drains() {
    if !test_process() {
        return;
    }
    let draining = || Status::unavailable(pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE);
    let closed = || Status::unavailable("transport is closing");
    type Case = (
        &'static str,
        Option<Value>,
        fn(Status) -> Reply,
        fn() -> Reply,
        bool,
    );
    let cases: [Case; 6] = [
        (
            "mkdir",
            Some(json!({"path": "/d"})),
            |status| Reply::Mkdir(Err(status)),
            || Reply::Mkdir(Ok(MkdirResponse::default())),
            false,
        ),
        (
            "remove",
            Some(json!({"path": "/r"})),
            |status| Reply::Remove(Err(status)),
            || Reply::Remove(Ok(RemoveResponse::default())),
            false,
        ),
        (
            "rename",
            Some(json!({"old_path": "/a", "new_path": "/b"})),
            |status| Reply::Rename(Err(status)),
            || Reply::Rename(Ok(RenameResponse::default())),
            true,
        ),
        (
            "changes_enable",
            Some(json!({})),
            |status| Reply::EnableFileChanges(Err(status)),
            || Reply::EnableFileChanges(Ok(EnableFileChangesResponse::default())),
            false,
        ),
        (
            "changes_disable",
            Some(json!({})),
            |status| Reply::DisableFileChanges(Err(status)),
            || Reply::DisableFileChanges(Ok(DisableFileChangesResponse::default())),
            false,
        ),
        (
            "modified",
            None,
            |status| Reply::GetModifiedFiles(Err(status)),
            || Reply::GetModifiedFiles(Ok(GetModifiedFilesResponse::default())),
            true,
        ),
    ];
    let harness = Harness::start().await;
    for (index, (action, keyed_body, failed, ok, retryable_after_close)) in
        cases.into_iter().enumerate()
    {
        let send = |attempt: usize| {
            let key = format!("k-{index}-{attempt}");
            match &keyed_body {
                Some(body) => harness.post_keyed(action, &key, body.clone()),
                None => harness.post(action, json!({})),
            }
        };
        let prepared = harness.prepared().len();
        harness.worker.reply(failed(draining()));
        harness.worker.reply(ok());
        let response = harness.send(send(0)).await;
        assert_eq!(response.status(), StatusCode::OK, "{action}");
        let again = &harness.prepared()[prepared..];
        assert_eq!(again.len(), 2, "{action}");
        assert_eq!(again[0].fingerprint, again[1].fingerprint, "{action}");

        harness.worker.reply(failed(closed()));
        let (status, _, body) = read_body(harness.send(send(1)).await).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{action}");
        assert_eq!(harness.prepared().len(), prepared + 3, "{action}");
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope.retryable, retryable_after_close, "{action}");
    }
    assert_eq!(harness.worker.unanswered(), 0);
}
