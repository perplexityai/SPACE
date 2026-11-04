use super::*;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, put},
};
use clap::Parser;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::cli::{Cli, Commands};

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const NODE_A: &str = "01900000-0000-7000-8000-00000000000a";
const NODE_B: &str = "01900000-0000-7000-8000-00000000000b";

/// `space-admin-cli runtime env <args>`, parsed.
fn env(args: &[&str]) -> RuntimeCommands {
    let argv = ["space-admin-cli", "runtime", "env"].iter().chain(args);
    let cli = Cli::try_parse_from(argv).expect("argv should parse");
    let Commands::Runtime { command } = cli.command else {
        panic!("expected a runtime command");
    };
    command
}

/// Whether `space-admin-cli runtime env <args>` parses.
fn if_match(headers: &HeaderMap) -> Option<String> {
    headers
        .get("if-match")
        .map(|value| value.to_str().unwrap().to_string())
}

fn env_parses(args: &[&str]) -> bool {
    Cli::try_parse_from(["space-admin-cli", "runtime", "env"].iter().chain(args)).is_ok()
}

/// Serve `app` on a loopback port for the duration of `run`.
async fn with_server<F, Fut>(app: Router, run: F)
where
    F: FnOnce(AdminClient) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = AdminClient::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "synthetic-token",
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    run(client).await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn list_renders_the_policy_and_each_environment() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-environments",
        get(|headers: HeaderMap| async move {
            assert_eq!(headers["authorization"], "Bearer synthetic-token");
            Json(json!({
                "generation": 3,
                "default_environment_ref": E1,
                "legacy_unbound_allowed": null,
                "last_change": {"updated_at": "2026-10-26T00:00:00Z", "updated_by_user_id": null, "reason": "block e0"},
                "environments": [
                    {"environment_ref": E0, "state": "blocked", "executes": false, "is_default": false,
                     "last_change": {"updated_at": "2026-10-25T09:00:00Z", "updated_by_user_id": "01900000-0000-7000-8000-0000000000aa", "reason": "zero-day"}},
                    {"environment_ref": E1, "state": "allowed", "executes": true, "is_default": true, "last_change": null},
                ],
            }))
        }),
    );
    with_server(app, |client| async move {
        let rendered = client.runtime_command(env(&["list"])).await.unwrap();
        assert!(rendered.contains("Policy generation: 3"), "{rendered}");
        assert!(rendered.contains(&format!("Default: {E1}")), "{rendered}");
        assert!(
            rendered.contains("Legacy unbound work: unset"),
            "{rendered}"
        );
        assert!(
            rendered.contains("Last change: 2026-10-26T00:00:00Z by -: block e0"),
            "{rendered}"
        );
        assert!(
            rendered.contains(E0) && rendered.contains("blocked"),
            "{rendered}"
        );

        // Each environment's own change, distinct from the policy row's.
        for cell in [
            "2026-10-25T09:00:00Z",
            "01900000-0000-7000-8000-0000000000aa",
            "zero-day",
        ] {
            assert!(rendered.contains(cell), "{cell}: {rendered}");
        }

        let raw = client
            .runtime_command(env(&["list", "--json"]))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&raw).unwrap()["generation"],
            3
        );
    })
    .await;
}

#[tokio::test]
async fn default_and_state_send_the_reason_and_report_the_generation() {
    let (sender, mut requests) = mpsc::channel(4);
    let app = Router::new()
        .route(
            "/admin/api/v1/runtime-environments/default",
            put(
                |State(sender): State<mpsc::Sender<(String, Value, Option<String>)>>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    sender.send(("default".into(), body, if_match(&headers))).await.unwrap();
                    Json(json!({"generation": 4, "default_environment_ref": E1}))
                },
            ),
        )
        .route(
            "/admin/api/v1/runtime-environments/{environment}/state",
            put(
                |State(sender): State<mpsc::Sender<(String, Value, Option<String>)>>,
                 axum::extract::Path(environment): axum::extract::Path<String>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    sender
                        .send((environment.clone(), body, if_match(&headers)))
                        .await
                        .unwrap();
                    Json(json!({"generation": 5, "environment_ref": environment, "state": "blocked"}))
                },
            ),
        )
        .with_state(sender);
    with_server(app, |client| async move {
        let rendered = client
            .runtime_command(env(&["default", E1, "--reason", "roll forward"]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!("Default is now {E1} (policy generation 4).")
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                "default".into(),
                json!({"environment_ref": E1, "reason": "roll forward", "strand": false}),
                None
            )
        );

        let rendered = client
            .runtime_command(env(&[
                "state",
                E0,
                "blocked",
                "--reason",
                "0-day",
                "--strand",
                "--if-generation",
                "4",
            ]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!("{E0} is now blocked (policy generation 5).")
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                E0.into(),
                json!({"state": "blocked", "reason": "0-day", "strand": true}),
                Some("4".into())
            )
        );

        client
            .runtime_command(env(&["state", E0, "deprecated", "--reason", "superseded"]))
            .await
            .unwrap();
        assert_eq!(
            requests.try_recv().unwrap().1,
            json!({"state": "deprecated", "reason": "superseded", "strand": false})
        );
    })
    .await;
}

#[tokio::test]
async fn a_refusal_comes_back_with_the_servers_status_and_body() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-environments/{environment}/state",
        put(|| async {
            (
                StatusCode::CONFLICT,
                Json(json!({"error": "still bound", "at_risk": {"bound": {"sandboxes": 2}}})),
            )
        }),
    );
    with_server(app, |client| async move {
        let error = client
            .runtime_command(env(&["state", E0, "blocked", "--reason", "0-day"]))
            .await
            .unwrap_err();
        let CliError::Api { status, body } = &error else {
            panic!("expected an API error, got {error:?}");
        };
        assert_eq!(*status, 409);
        assert!(
            body.contains("still bound") && body.contains("\"sandboxes\": 2"),
            "{body}"
        );
    })
    .await;
}

fn node_page(node: &str, conformance: Value, next_after: Option<&str>) -> Value {
    let mut entry = json!({
        "node_id": node,
        "name": null,
        "role": "worker",
        "substrate_node_id": format!("ip-{node}"),
        "status_fresh": true,
        "policy_received_at": "2026-10-26T00:00:00Z",
        "applied_generation": 1,
        "applied_current": false,
        "enforced": {
            "schema_version": 1,
            "allowed_environment_references": [E0, E1],
            "default_environment_reference": E0,
            "legacy_unbound_allowed": false,
        },
    });
    entry
        .as_object_mut()
        .unwrap()
        .extend(conformance.as_object().unwrap().clone());
    json!({
        "generation": 2,
        "default_environment_ref": E1,
        "nodes": [entry],
        "next_after": next_after,
    })
}

#[derive(Deserialize)]
struct AfterQuery {
    after: Option<String>,
}

fn paged_nodes_app() -> Router {
    Router::new().route(
        "/admin/api/v1/runtime-environments/nodes",
        get(|Query(query): Query<AfterQuery>| async move {
            Json(match query.after.as_deref() {
                None => node_page(NODE_A, json!({"conformance": "conforms"}), Some(NODE_A)),
                Some(NODE_A) => node_page(
                    NODE_B,
                    json!({
                        "conformance": "diverges",
                        "default_differs": true,
                        "executes_outside_policy": [E0],
                        "allows_revoked_legacy_unbound": false,
                    }),
                    None,
                ),
                Some(other) => panic!("unexpected cursor {other}"),
            })
        }),
    )
}

#[tokio::test]
async fn nodes_shows_one_page_unless_asked_to_follow_the_cursor() {
    with_server(paged_nodes_app(), |client| async move {
        let one = client.runtime_command(env(&["nodes"])).await.unwrap();
        assert!(one.contains(NODE_A) && !one.contains(NODE_B), "{one}");
        assert!(one.contains(&format!("pass --after {NODE_A}")), "{one}");
        assert!(one.contains("1 conforms"), "{one}");

        let next = client
            .runtime_command(env(&["nodes", "--after", NODE_A]))
            .await
            .unwrap();
        assert!(next.contains(NODE_B) && !next.contains(NODE_A), "{next}");

        let all = client
            .runtime_command(env(&["nodes", "--all"]))
            .await
            .unwrap();
        assert!(all.contains(NODE_A) && all.contains(NODE_B), "{all}");
        assert!(all.contains("1 conforms, 1 diverges"), "{all}");
        assert!(all.contains("1 (not current)"), "{all}");
        assert!(
            all.contains("default differs; executes sha256:000000000000…"),
            "{all}"
        );
        assert!(!all.contains("More nodes follow"), "{all}");
        assert!(!all.contains("The policy changed while paging"), "{all}");

        let pages = client
            .runtime_command(env(&["nodes", "--all", "--json"]))
            .await
            .unwrap();
        // One page without --all, printed as the page itself.
        let page = client
            .runtime_command(env(&["nodes", "--json"]))
            .await
            .unwrap();
        let page: Value = serde_json::from_str(&page).unwrap();
        assert_eq!(page["nodes"][0]["node_id"], NODE_A);
        assert_eq!(page["next_after"], NODE_A);

        let pages: Vec<Value> = serde_json::from_str(&pages).unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[1]["nodes"][0]["node_id"], NODE_B);
    })
    .await;
}

#[tokio::test]
async fn all_stops_when_the_cursor_does_not_advance() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-environments/nodes",
        get(|| async {
            Json(node_page(
                NODE_A,
                json!({"conformance": "conforms"}),
                Some(NODE_A),
            ))
        }),
    );
    with_server(app, |client| async move {
        let error = client
            .runtime_command(env(&["nodes", "--all"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("didn't advance"), "{error}");
    })
    .await;
}

#[tokio::test]
async fn population_lists_each_environment_and_the_unattributed_buckets() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-environments/population",
        get(|| async {
            Json(json!({
                "generation": 2,
                "environments": [{
                    "environment_ref": E1, "state": "allowed", "is_default": true,
                    "sandboxes": 3, "sandboxes_by_lifecycle": {"started": 2, "suspended": 1},
                    "sandboxes_not_runnable": 1,
                    "template_releases": 4, "template_releases_in_use": 1,
                }],
                "unbound": {"sandboxes": 5, "sandboxes_by_lifecycle": {"stopped": 5}, "template_releases": 1},
                "unrecorded": {"sandboxes": 0, "sandboxes_by_lifecycle": {}, "template_releases": 0},
                "invalid": {"sandboxes": 0, "sandboxes_by_lifecycle": {}, "template_releases": 0},
            }))
        }),
    );
    with_server(app, |client| async move {
        let rendered = client.runtime_command(env(&["population"])).await.unwrap();
        assert!(rendered.contains("3 (1 can't run)"), "{rendered}");
        assert!(rendered.contains("4 (1 in use)"), "{rendered}");
        assert!(rendered.contains("started=2 suspended=1"), "{rendered}");
        assert!(
            rendered.contains("(unbound)") && rendered.contains("stopped=5"),
            "{rendered}"
        );
        assert!(
            rendered.contains("(unrecorded)") && rendered.contains("(invalid binding)"),
            "{rendered}"
        );
    })
    .await;
}

#[tokio::test]
async fn a_write_that_fails_in_transport_says_it_may_have_landed() {
    // Bind and drop a listener, so the port is closed and nothing answers.
    let addr = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let client = AdminClient::new(&format!("http://{addr}"), "synthetic-token").unwrap();
    let error = client
        .runtime_command(env(&["default", E1, "--reason", "r"]))
        .await
        .unwrap_err();
    assert!(matches!(error, CliError::Transport(_)), "{error:?}");
    assert!(error.to_string().contains("may have landed"), "{error}");

    // A read has nothing to land.
    let error = client.runtime_command(env(&["list"])).await.unwrap_err();
    assert!(!error.to_string().contains("may have landed"), "{error}");
}

#[tokio::test]
async fn strand_without_a_block_is_refused_before_sending() {
    // Nothing listens here: the refusal must come before any request.
    let client = AdminClient::new("http://127.0.0.1:1", "synthetic-token").unwrap();
    let error = client
        .runtime_command(env(&["state", E1, "allowed", "--reason", "r", "--strand"]))
        .await
        .unwrap_err();
    assert!(matches!(error, CliError::Argument(_)), "{error:?}");
}

#[test]
fn arguments_are_checked_before_anything_is_sent() {
    // A malformed reference never reaches a URL path.
    assert!(!env_parses(&["default", "sha256:short", "--reason", "r"]));
    assert!(!env_parses(&[
        "state", "../nodes", "allowed", "--reason", "r"
    ]));
    // Every write names a reason.
    assert!(!env_parses(&["default", E1]));
    assert!(!env_parses(&["state", E1, "allowed"]));
    assert!(!env_parses(&["state", E1, "retired", "--reason", "r"]));
    assert!(!env_parses(&["nodes", "--all", "--after", NODE_A]));
    assert!(env_parses(&[
        "state", E1, "blocked", "--reason", "r", "--strand"
    ]));

    assert!(env_parses(&["default", E1, "--reason", "r", "--strand"]));
    let RuntimeCommands::Env {
        command: RuntimeEnvCommands::State { state, .. },
    } = env(&["state", E1, "deprecated", "--reason", "r"])
    else {
        panic!("expected env state");
    };
    assert_eq!(state.as_str(), "deprecated");
}

#[test]
fn short_refs_keep_the_algorithm_and_twelve_digits() {
    assert_eq!(short_ref(E1), "sha256:111111111111…");
    // Nothing is cut, so nothing is marked as cut.
    assert_eq!(short_ref("sha256:abcdefabcdef"), "sha256:abcdefabcdef");
    assert_eq!(short_ref("sha256:abc"), "sha256:abc");
    assert_eq!(short_ref("odd"), "odd");
    // Twelve bytes in would split a character: left whole, no panic.
    assert_eq!(short_ref("x:aééééééé"), "x:aééééééé");
}

#[test]
fn a_policy_change_between_pages_is_called_out() {
    let page = |generation: u64, node: &str| NodePage {
        generation,
        default_environment_ref: Some(E1.to_string()),
        nodes: vec![
            serde_json::from_value(
                node_page(node, json!({"conformance": "conforms"}), None)["nodes"][0].clone(),
            )
            .unwrap(),
        ],
        next_after: None,
    };
    let rendered = render_nodes(&[page(2, NODE_A), page(3, NODE_B)]);
    assert!(
        rendered.contains("Compared with policy generation 2"),
        "{rendered}"
    );
    assert!(
        rendered.contains("The policy changed while paging"),
        "{rendered}"
    );
    assert!(!render_nodes(&[page(2, NODE_A), page(2, NODE_B)]).contains("changed while paging"));
}
