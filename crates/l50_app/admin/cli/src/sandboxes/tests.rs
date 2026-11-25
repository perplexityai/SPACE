use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use clap::Parser;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::admin_client::{AdminClient, CliError};
use crate::cli::{Cli, Commands};

const SANDBOX: &str = "sbx_01900000-0000-7000-8000-000000000001";

fn parse(args: &[&str]) -> crate::cli::SandboxCommands {
    let cli = Cli::try_parse_from(args).unwrap();
    let Commands::Sandbox { command } = cli.command else {
        panic!("sandbox command")
    };
    command
}

#[tokio::test]
async fn drop_memory_posts_the_reason_under_an_idempotency_key() {
    let (sender, mut requests) = mpsc::channel(1);
    let app = Router::new()
        .route(
            &format!("/admin/api/v1/sandboxes/{SANDBOX}:drop-memory"),
            post(
                |State(sender): State<mpsc::Sender<(Option<String>, Value)>>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    assert_eq!(headers["authorization"], "Bearer synthetic-token");
                    let key = headers
                        .get("idempotency-key")
                        .map(|value| value.to_str().unwrap().to_string());
                    sender.send((key, body)).await.unwrap();
                    (
                        axum::http::StatusCode::ACCEPTED,
                        Json(json!({"operation_id": "op_1", "sandbox_id": SANDBOX})),
                    )
                },
            ),
        )
        .with_state(sender);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = AdminClient::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "synthetic-token",
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let output = client
        .sandbox_command(parse(&[
            "space-admin-cli",
            "sandbox",
            "drop-memory",
            SANDBOX,
            "--reason",
            "node retired",
            "--idempotency-key",
            "retry-1",
        ]))
        .await
        .unwrap();
    assert!(output.contains("op_1"), "{output}");
    let (key, body) = requests.recv().await.unwrap();
    assert_eq!(key.as_deref(), Some("retry-1"));
    assert_eq!(body, json!({"reason": "node retired"}));

    // Without --idempotency-key, a fresh key is sent.
    client
        .sandbox_command(parse(&[
            "space-admin-cli",
            "sandbox",
            "drop-memory",
            SANDBOX,
            "--reason",
            "node retired",
        ]))
        .await
        .unwrap();
    let (key, _) = requests.recv().await.unwrap();
    assert!(key.is_some_and(|key| uuid::Uuid::parse_str(&key).is_ok()));
    server.abort();
}

#[tokio::test]
async fn drop_memory_refuses_a_blank_reason_or_a_path_changing_id() {
    let client = AdminClient::new("http://127.0.0.1:9", "synthetic-token").unwrap();
    for (sandbox, reason) in [
        (SANDBOX, " "),
        ("sbx/../nodes", "why"),
        ("a:force-stop", "why"),
    ] {
        let error = client
            .sandbox_command(parse(&[
                "space-admin-cli",
                "sandbox",
                "drop-memory",
                sandbox,
                "--reason",
                reason,
            ]))
            .await
            .unwrap_err();
        assert!(matches!(error, CliError::Argument(_)), "{error:?}");
    }
    assert!(Cli::try_parse_from(["space-admin-cli", "sandbox", "drop-memory", SANDBOX]).is_err());
}
