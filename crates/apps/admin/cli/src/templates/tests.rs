use super::*;
use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use clap::Parser;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::cli::{Cli, Commands};

const JOB: &str = "01900000-0000-7000-8000-000000000001";
const HASH: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

#[tokio::test]
async fn system_build_submits_options_and_reports_publication_outcome() {
    for (state, hash, succeeds) in [
        ("succeeded", Some(HASH), true),
        ("failed", None, false),
        ("publish_failed", Some(HASH), false),
        ("succeeded", None, false),
    ] {
        let (sender, mut requests) = mpsc::channel(1);
        let app = Router::new()
            .route("/admin/api/v1/system-templates/code-runtime/builds", post(
                |State(sender): State<mpsc::Sender<Value>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                    assert_eq!(headers["authorization"], "Bearer synthetic-token");
                    sender.send(body).await.unwrap();
                    Json(json!({"job_id": JOB, "template_name": "code-runtime"}))
                },
            ))
            .route(&format!("/admin/api/v1/templates/code-runtime/builds/{JOB}"), get(move || async move {
                Json(json!({"job_id": JOB, "template_name": "code-runtime", "state": state, "content_hash": hash, "error": "synthetic build failure"}))
            }))
            .with_state(sender);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = AdminClient::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            "synthetic-token",
        )
        .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let seed = format!("seed@{HASH}");
        let cli = Cli::try_parse_from([
            "space-admin-cli",
            "template",
            "build",
            "code-runtime",
            "--base",
            &seed,
            "--publish-tag",
            "latest",
        ])
        .unwrap();
        let Commands::Template { command } = cli.command else {
            panic!("template command")
        };

        let result = client.template_command(command).await;

        assert_eq!(result.is_ok(), succeeds, "{state}: {result:?}");
        let request = requests.try_recv().unwrap();
        assert_eq!(request, json!({"base": seed, "publish_tags": ["latest"]}));
        if succeeds {
            let status: Value = serde_json::from_str(result.as_ref().unwrap()).unwrap();
            assert_eq!(status["content_hash"], HASH);
        }
        if state == "publish_failed" {
            assert!(result.unwrap_err().to_string().contains(HASH));
        }
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn async_seed_build_returns_job_without_polling_or_promoting_latest() {
    let app = Router::new().route(
        "/admin/api/v1/system-templates/seed/builds",
        post(|Json(body): Json<Value>| async move {
            assert_eq!(body, json!({"base": null, "publish_tags": []}));
            Json(json!({"job_id": JOB, "template_name": "seed"}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = AdminClient::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "synthetic-token",
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cli =
        Cli::try_parse_from(["space-admin-cli", "template", "build", "seed", "--async"]).unwrap();
    let Commands::Template { command } = cli.command else {
        panic!("template command")
    };

    let result = client.template_command(command).await.unwrap();

    assert_eq!(
        serde_json::from_str::<Value>(&result).unwrap()["job_id"],
        JOB
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn build_wait_retries_transient_polls_but_rejects_client_errors() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let succeeded =
        json!({"job_id": JOB, "template_name": "seed", "state": "succeeded", "content_hash": HASH})
            .to_string();
    for codes in [vec![200, 503, 200], vec![401], vec![403], vec![404]] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = AdminClient::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            "synthetic-token",
        )
        .unwrap();
        let retries = codes.len() > 1;
        let terminal_code = *codes.last().unwrap();
        let succeeded = succeeded.clone();
        let server = tokio::spawn(async move {
            for (index, code) in codes.into_iter().enumerate() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
                assert!(
                    String::from_utf8(request)
                        .unwrap()
                        .starts_with(&format!("GET /admin/api/v1/templates/seed/builds/{JOB} "))
                );
                let body = if code == 200 {
                    &succeeded
                } else {
                    "unavailable"
                };
                // A truncated first response exercises a transport failure while reading the body.
                let length = body.len() + usize::from(retries && index == 0);
                socket.write_all(format!(
                    "HTTP/1.1 {code} Response\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}"
                ).as_bytes()).await.unwrap();
            }
        });
        let job = BuildJob {
            job_id: JOB.parse().unwrap(),
            template_name: SystemTemplate::Seed,
        };

        let result = tokio::time::timeout(Duration::from_secs(10), client.wait_for_build(&job))
            .await
            .unwrap();

        if retries {
            assert_eq!(result.unwrap().content_hash.unwrap().to_string(), HASH);
        } else {
            assert!(matches!(result, Err(CliError::Api { status, .. }) if status == terminal_code));
        }
        server.await.unwrap();
    }
}
