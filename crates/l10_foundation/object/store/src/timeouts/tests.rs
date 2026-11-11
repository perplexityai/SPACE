use super::*;
use crate::ObjectStore;
use crate::test_support::{Recorder, credentials, serve, store, target};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::task::JoinSet;

const CLASSES: [S3TimeoutClass; 3] = [
    S3TimeoutClass::Request,
    S3TimeoutClass::BodyUpload,
    S3TimeoutClass::ServerSide,
];

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn env(vars: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
    let vars = vars.to_vec();
    move |name| {
        vars.iter()
            .find(|(var, _)| *var == name)
            .map(|(_, value)| (*value).to_string())
    }
}

#[test]
fn class_timeouts_default_derive_and_override() {
    let defaults = S3Timeouts::resolve(|_| None);
    for (class, attempt, operation) in [
        (S3TimeoutClass::Request, 10, 35),
        (S3TimeoutClass::BodyUpload, 60, 185),
        (S3TimeoutClass::ServerSide, 30, 95),
    ] {
        assert_eq!(
            defaults.class(class),
            ClassTimeouts {
                attempt: secs(attempt),
                operation: secs(operation),
            },
            "{class:?} defaults",
        );

        // Raising only the attempt timeout keeps room for every retry, and
        // touches no other class.
        let raised = S3Timeouts::resolve(env(&[(class.attempt_timeout_env(), "120")]));
        assert_eq!(
            raised.class(class),
            ClassTimeouts {
                attempt: secs(120),
                operation: secs(3 * 120 + 5),
            },
            "{class:?} derived",
        );
        for other in CLASSES.into_iter().filter(|other| *other != class) {
            assert_eq!(raised.class(other), defaults.class(other), "{other:?}");
        }

        let explicit = S3Timeouts::resolve(env(&[
            (class.attempt_timeout_env(), "120"),
            (class.operation_timeout_env(), "7200"),
        ]));
        assert_eq!(
            explicit.class(class),
            ClassTimeouts {
                attempt: secs(120),
                operation: secs(7200),
            },
            "{class:?} explicit",
        );

        for rejected in ["4", "21601", "ten"] {
            assert_eq!(
                S3Timeouts::resolve(env(&[(class.attempt_timeout_env(), rejected)])).class(class),
                defaults.class(class),
                "{class:?} attempt {rejected} should fall back to the default",
            );
        }
        assert_eq!(
            S3Timeouts::resolve(env(&[(class.operation_timeout_env(), "86401")])).class(class),
            defaults.class(class),
            "{class:?} operation 86401 should fall back to the derived value",
        );
    }
}

/// Which S3 operation a stand-in request is, by method and query.
fn operation_of(method: &Method, uri: &Uri) -> S3Operation {
    let query = uri.query().unwrap_or_default();
    match *method {
        Method::HEAD => S3Operation::HeadObject,
        Method::GET => S3Operation::GetObject,
        Method::DELETE => S3Operation::AbortMultipartUpload,
        Method::PUT if query.contains("partNumber=") => S3Operation::UploadPart,
        Method::PUT => S3Operation::PutObject,
        Method::POST if query.contains("uploads") => S3Operation::CreateMultipartUpload,
        Method::POST => S3Operation::CompleteMultipartUpload,
        _ => panic!("unexpected stand-in request {method} {uri}"),
    }
}

/// Requests for the operation a stand-in never answers.
#[derive(Debug, Default)]
struct Unanswered {
    requests: AtomicUsize,
    first_at: OnceLock<Instant>,
}

/// Answers every operation except `hang`, which it reads in full and never
/// answers.
fn never_answering(hang: S3Operation, unanswered: Arc<Unanswered>) -> Router {
    Router::new().fallback(move |method: Method, uri: Uri, body: Body| {
        let unanswered = unanswered.clone();
        async move {
            let operation = operation_of(&method, &uri);
            if operation == hang {
                unanswered.first_at.get_or_init(Instant::now);
                unanswered.requests.fetch_add(1, Ordering::SeqCst);
            }
            axum::body::to_bytes(body, usize::MAX)
                .await
                .expect("read request body");
            if operation == hang {
                return std::future::pending::<Response>().await;
            }
            let etag = [("etag", "\"synthetic-etag\"")];
            match operation {
                S3Operation::CreateMultipartUpload => (
                    StatusCode::OK,
                    "<InitiateMultipartUploadResult><Bucket>synthetic-bucket</Bucket>\
                     <Key>synthetic</Key><UploadId>synthetic-upload</UploadId>\
                     </InitiateMultipartUploadResult>",
                )
                    .into_response(),
                S3Operation::CompleteMultipartUpload => (
                    StatusCode::OK,
                    "<CompleteMultipartUploadResult><ETag>\"synthetic-etag\"</ETag>\
                     </CompleteMultipartUploadResult>",
                )
                    .into_response(),
                S3Operation::AbortMultipartUpload => StatusCode::NO_CONTENT.into_response(),
                _ => (StatusCode::OK, etag).into_response(),
            }
        }
    })
}

/// Each operation's class bound, observed end to end: against an endpoint
/// that never answers it, the call makes all three attempts, so it ends no
/// earlier than three attempt timeouts, and no later than its operation
/// timeout. The lower bound is measured from the call, which starts before
/// the operation; the upper from its first request, which arrives after the
/// operation starts and so excludes the multipart work before a stalled part
/// or completion. The classes are far enough apart that an operation bounded by any other
/// class falls outside its window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_answered_operations_end_at_their_class_bound() {
    // Allowance scaled down with the attempts, as the production 5 s is to
    // its seconds-long attempts.
    let class = |attempt_ms: u64| ClassTimeouts {
        attempt: Duration::from_millis(attempt_ms),
        operation: Duration::from_millis(3 * attempt_ms + 400),
    };
    // Long enough that a loaded machine still delivers each attempt's
    // request before its timeout; an operation in a neighbouring class lands
    // at least 250 ms outside the window.
    let timeouts = S3Timeouts {
        request: class(250),
        server_side: class(800),
        body_upload: class(1400),
    };
    // The failure takes a moment to reach the caller.
    let late = Duration::from_secs(1);

    let mut calls = JoinSet::new();
    for (hang, class) in [
        (S3Operation::GetObject, S3TimeoutClass::Request),
        (S3Operation::HeadObject, S3TimeoutClass::Request),
        (S3Operation::PutObject, S3TimeoutClass::BodyUpload),
        (S3Operation::UploadPart, S3TimeoutClass::BodyUpload),
        (
            S3Operation::CompleteMultipartUpload,
            S3TimeoutClass::ServerSide,
        ),
    ] {
        calls.spawn(async move {
            let unanswered = Arc::new(Unanswered::default());
            let endpoint = serve(never_answering(hang, unanswered.clone())).await;
            let mut store = store(
                timeouts,
                Duration::from_secs(5),
                &Arc::new(Recorder::default()),
            );
            // Two parts, uploaded one at a time so a stalled part is the
            // only request in flight.
            store.multipart_part_size = crate::MULTIPART_PART_SIZE_MIN;
            store.multipart_concurrency = 1;
            let src = tempfile::NamedTempFile::new().expect("create upload source");
            src.as_file()
                .set_len(2 * crate::MULTIPART_PART_SIZE_MIN as u64)
                .expect("size upload source");

            let (creds, target) = (credentials(), target(&endpoint));
            let started_at = Instant::now();
            let failed = match hang {
                S3Operation::GetObject => store.get_bytes(&creds, target).await.is_err(),
                S3Operation::HeadObject => store.head(&creds, target).await.is_err(),
                S3Operation::PutObject => store
                    .put_bytes(&creds, target, bytes::Bytes::from_static(b"payload"))
                    .await
                    .is_err(),
                _ => store.put_file(&creds, target, src.path()).await.is_err(),
            };
            let call = started_at.elapsed();
            let since_request = unanswered.first_at.get().map(Instant::elapsed);
            let requests = unanswered.requests.load(Ordering::SeqCst);
            (hang, class, failed, call, since_request, requests)
        });
    }

    while let Some(joined) = calls.join_next().await {
        let (operation, class, failed, call, since_request, requests) = joined.expect("call task");
        let bound = timeouts.class(class);
        assert!(failed, "{operation} should fail");
        assert_eq!(requests, 3, "{operation} should make every attempt");
        assert!(
            call >= bound.attempt * 3,
            "{operation} ended after {call:?}, before three {:?} attempts",
            bound.attempt,
        );
        let since_request = since_request.expect("the stalled operation was requested");
        assert!(
            since_request <= bound.operation + late,
            "{operation} ended {since_request:?} after its first request, \
             past its {:?} operation bound",
            bound.operation,
        );
    }
}
