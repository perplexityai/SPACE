use super::*;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use clap::Parser;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    cli::{Cli, Commands, RuntimeCommands},
    runtime::tests::{E0, E1, if_match, with_server},
};

const C1: &str = "01900000-0000-7000-8000-0000000000c1";
const C2: &str = "01900000-0000-7000-8000-0000000000c2";
const USER: &str = "01900000-0000-7000-8000-0000000000aa";

/// `space-admin-cli runtime campaign <args>`, parsed.
fn campaign(args: &[&str]) -> RuntimeCommands {
    let argv = ["space-admin-cli", "runtime", "campaign"]
        .iter()
        .chain(args);
    let cli = Cli::try_parse_from(argv).expect("argv should parse");
    let Commands::Runtime { command } = cli.command else {
        panic!("expected a runtime command");
    };
    command
}

/// Whether `space-admin-cli runtime campaign <args>` parses.
fn campaign_parses(args: &[&str]) -> bool {
    Cli::try_parse_from(
        ["space-admin-cli", "runtime", "campaign"]
            .iter()
            .chain(args),
    )
    .is_ok()
}

fn campaign_json(id: &str, phase: &str, stranded: Option<u64>) -> Value {
    json!({
        "id": id,
        "kind": "sandboxes",
        "from_environment_ref": E0,
        "to_environment_ref": E1,
        "phase": phase,
        "generation": 2,
        "max_in_flight_per_node": 1,
        "max_in_flight_per_cluster": 5,
        "failure_budget": 3,
        "include_running": false,
        "reason": "roll forward",
        "created_at": "2026-11-01T00:00:00Z",
        "created_by_user_id": USER,
        "updated_at": "2026-11-01T01:00:00Z",
        "updated_by_user_id": null,
        "finished_at": null,
        "items": {"pending": 7, "in_flight": 2, "done": 1, "failed": 0, "skipped": 4},
        "stranded": stranded,
    })
}

type Recorded = (String, Value, Option<String>);

#[tokio::test]
async fn start_sends_the_settings_and_leaves_unset_defaults_to_the_server() {
    let (sender, mut requests) = mpsc::channel(4);
    let app = Router::new()
        .route(
            "/admin/api/v1/runtime-upgrade-campaigns",
            axum::routing::post(
                |State(sender): State<mpsc::Sender<Recorded>>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    sender
                        .send(("start".into(), body, if_match(&headers)))
                        .await
                        .unwrap();
                    (
                        StatusCode::CREATED,
                        Json(json!({"id": C1, "phase": "canary", "generation": 0})),
                    )
                },
            ),
        )
        .with_state(sender);
    with_server(app, |client| async move {
        let rendered = client
            .runtime_command(campaign(&[
                "start",
                "--kind",
                "templates",
                "--from",
                E0,
                "--to",
                E1,
                "--wave-size",
                "5",
                "--failure-budget",
                "3",
                "--reason",
                "roll forward",
            ]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!("Started campaign {C1} in canary (generation 0).")
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                "start".into(),
                json!({
                    "kind": "templates",
                    "from_environment_ref": E0,
                    "to_environment_ref": E1,
                    "max_in_flight_per_cluster": 5,
                    "failure_budget": 3,
                    "include_running": false,
                    "reason": "roll forward",
                }),
                None
            )
        );

        let raw = client
            .runtime_command(campaign(&[
                "start",
                "--kind",
                "sandboxes",
                "--from",
                E0,
                "--to",
                E1,
                "--canary-size",
                "0",
                "--wave-size",
                "4",
                "--per-node",
                "2",
                "--failure-budget",
                "1",
                "--include-running",
                "--reason",
                "r",
                "--json",
            ]))
            .await
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&raw).unwrap()["id"], C1);
        assert_eq!(
            requests.try_recv().unwrap().1,
            json!({
                "kind": "sandboxes",
                "from_environment_ref": E0,
                "to_environment_ref": E1,
                "canary_size": 0,
                "max_in_flight_per_cluster": 4,
                "max_in_flight_per_node": 2,
                "failure_budget": 1,
                "include_running": true,
                "reason": "r",
            })
        );

        client
            .runtime_command(campaign(&[
                "start",
                "--kind",
                "sandbox_force_restarts",
                "--from",
                E0,
                "--to",
                E1,
                "--wave-size",
                "3",
                "--per-node",
                "1",
                "--failure-budget",
                "2",
                "--reason",
                "retire memory",
            ]))
            .await
            .unwrap();
        assert_eq!(
            requests.try_recv().unwrap().1,
            json!({
                "kind": "sandbox_force_restarts",
                "from_environment_ref": E0,
                "to_environment_ref": E1,
                "max_in_flight_per_cluster": 3,
                "max_in_flight_per_node": 1,
                "failure_budget": 2,
                "include_running": false,
                "reason": "retire memory",
            })
        );

        client
            .runtime_command(campaign(&[
                "start",
                "--kind",
                "sandbox_memory_drops",
                "--from",
                E0,
                "--to",
                E1,
                "--wave-size",
                "6",
                "--failure-budget",
                "2",
                "--reason",
                "drop suspended memory",
            ]))
            .await
            .unwrap();
        assert_eq!(
            requests.try_recv().unwrap().1,
            json!({
                "kind": "sandbox_memory_drops",
                "from_environment_ref": E0,
                "to_environment_ref": E1,
                "max_in_flight_per_cluster": 6,
                "failure_budget": 2,
                "include_running": false,
                "reason": "drop suspended memory",
            })
        );
    })
    .await;
}

#[tokio::test]
async fn pause_and_resume_send_the_reason_and_the_expected_generation() {
    let (sender, mut requests) = mpsc::channel(4);
    let app = Router::new()
        .route(
            "/admin/api/v1/runtime-upgrade-campaigns/{spec}",
            axum::routing::post(
                |State(sender): State<mpsc::Sender<Recorded>>,
                 Path(spec): Path<String>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    let (id, action) = spec.rsplit_once(':').unwrap();
                    let (phase, retried) = match action {
                        "pause" => ("paused", 0),
                        _ if body["retry_failed"] == true => ("rolling", 3),
                        _ => ("canary", 0),
                    };
                    let response =
                        json!({"id": id, "phase": phase, "generation": 3, "retried": retried, "draining": 0});
                    sender
                        .send((spec.clone(), body, if_match(&headers)))
                        .await
                        .unwrap();
                    Json(response)
                },
            ),
        )
        .with_state(sender);
    with_server(app, |client| async move {
        let rendered = client
            .runtime_command(campaign(&[
                "pause",
                C1,
                "--reason",
                "canary looks off",
                "--if-generation",
                "2",
            ]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!("Campaign {C1} is now paused (generation 3).")
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                format!("{C1}:pause"),
                json!({"reason": "canary looks off"}),
                Some("2".into())
            )
        );

        let rendered = client
            .runtime_command(campaign(&["resume", C1, "--reason", "fixed"]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!("Campaign {C1} is now canary (generation 3).")
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                format!("{C1}:resume"),
                json!({"reason": "fixed", "retry_failed": false}),
                None
            )
        );

        let rendered = client
            .runtime_command(campaign(&[
                "resume",
                C1,
                "--reason",
                "retry",
                "--retry-failed",
                "--if-generation",
                "3",
            ]))
            .await
            .unwrap();
        assert!(
            rendered.ends_with("3 failed items returned to pending."),
            "{rendered}"
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                format!("{C1}:resume"),
                json!({"reason": "retry", "retry_failed": true}),
                Some("3".into())
            )
        );

        let raw = client
            .runtime_command(campaign(&["pause", C1, "--reason", "r", "--json"]))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&raw).unwrap()["phase"],
            "paused"
        );
    })
    .await;
}

#[tokio::test]
async fn a_stale_generation_comes_back_with_the_servers_status_and_body() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-upgrade-campaigns/{spec}",
        axum::routing::post(|| async {
            (
                StatusCode::PRECONDITION_FAILED,
                Json(json!({"error": "the campaign is at generation 4"})),
            )
        }),
    );
    with_server(app, |client| async move {
        let error = client
            .runtime_command(campaign(&[
                "pause",
                C1,
                "--reason",
                "r",
                "--if-generation",
                "2",
            ]))
            .await
            .unwrap_err();
        let CliError::Api { status, body } = &error else {
            panic!("expected an API error, got {error:?}");
        };
        assert_eq!(*status, 412);
        assert!(body.contains("at generation 4"), "{body}");
    })
    .await;
}

#[tokio::test]
async fn show_renders_the_settings_phase_and_item_counts() {
    let app =
        Router::new().route(
            "/admin/api/v1/runtime-upgrade-campaigns/{spec}",
            get(|Path(spec): Path<String>| async move {
                Json(campaign_json(&spec, "rolling", Some(3)))
            }),
        );
    with_server(app, |client| async move {
        let rendered = client
            .runtime_command(campaign(&["show", C1]))
            .await
            .unwrap();
        for line in [
            format!("Campaign: {C1}"),
            format!("From: {E0}"),
            format!("To: {E1}"),
            "Phase: rolling (generation 2)".to_string(),
            "Wave size: 5 per cluster, 1 per node".to_string(),
            "Failure budget: 3".to_string(),
            "Items: 7 pending, 2 in flight, 1 done, 0 failed, 4 skipped".to_string(),
            "Stranded: 3".to_string(),
            format!("Created: 2026-11-01T00:00:00Z by {USER}"),
            "Updated: 2026-11-01T01:00:00Z by the reconciler".to_string(),
        ] {
            assert!(rendered.contains(&line), "{line}: {rendered}");
        }
        assert!(!rendered.contains("Finished"), "{rendered}");

        let raw = client
            .runtime_command(campaign(&["show", C1, "--json"]))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&raw).unwrap()["items"]["pending"],
            7
        );
    })
    .await;
}

#[test]
fn show_leaves_out_stranded_for_a_kind_without_it_and_adds_when_it_finished() {
    let mut value = campaign_json(C1, "completed", None);
    value["kind"] = "templates".into();
    value["finished_at"] = "2026-11-02T00:00:00Z".into();
    let rendered = render_campaign(&serde_json::from_value(value).unwrap());
    assert!(!rendered.contains("Stranded"), "{rendered}");
    assert!(!rendered.contains("Held"), "{rendered}");
    assert!(
        rendered.ends_with("Finished: 2026-11-02T00:00:00Z"),
        "{rendered}"
    );
}

#[test]
fn show_reports_why_a_campaign_is_held() {
    let mut value = campaign_json(C1, "canary", None);
    value["kind"] = "templates".into();
    value["held"] = json!({
        "reason": "twins_not_booted",
        "detail": "creates here don't boot a twin's environment",
    });
    let rendered = render_campaign(&serde_json::from_value(value).unwrap());
    assert!(
        rendered
            .contains("Held: twins_not_booted (creates here don't boot a twin's environment)\n"),
        "{rendered}"
    );
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    unfinished: bool,
    after: Option<String>,
}

fn paged_campaigns_app() -> Router {
    Router::new().route(
        "/admin/api/v1/runtime-upgrade-campaigns",
        get(|Query(query): Query<ListQuery>| async move {
            if query.unfinished {
                return Json(json!({"campaigns": [], "next_after": null}));
            }
            Json(match query.after.as_deref() {
                None => json!({
                    "campaigns": [campaign_json(C1, "completed", Some(0))],
                    "next_after": C1,
                }),
                Some(C1) => json!({
                    "campaigns": [campaign_json(C2, "paused", None)],
                    "next_after": null,
                }),
                Some(other) => panic!("unexpected cursor {other}"),
            })
        }),
    )
}

#[tokio::test]
async fn list_follows_every_page() {
    with_server(paged_campaigns_app(), |client| async move {
        let rendered = client.runtime_command(campaign(&["list"])).await.unwrap();
        assert!(rendered.contains(C1) && rendered.contains(C2), "{rendered}");
        assert!(
            rendered.contains("completed") && rendered.contains("paused"),
            "{rendered}"
        );
        assert!(rendered.contains("sha256:111111111111…"), "{rendered}");

        let raw = client
            .runtime_command(campaign(&["list", "--json"]))
            .await
            .unwrap();
        let raw: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(raw["campaigns"].as_array().unwrap().len(), 2);
        assert_eq!(raw["campaigns"][1]["id"], C2);
        assert_eq!(raw["next_after"], Value::Null);

        let none = client
            .runtime_command(campaign(&["list", "--unfinished"]))
            .await
            .unwrap();
        assert_eq!(none, "No unfinished campaigns.");
    })
    .await;
}

#[tokio::test]
async fn list_stops_when_the_cursor_does_not_advance() {
    let app = Router::new().route(
        "/admin/api/v1/runtime-upgrade-campaigns",
        get(|| async {
            Json(json!({
                "campaigns": [campaign_json(C1, "rolling", None)],
                "next_after": C1,
            }))
        }),
    );
    with_server(app, |client| async move {
        let error = client
            .runtime_command(campaign(&["list"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("didn't advance"), "{error}");
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
        .runtime_command(campaign(&["pause", C1, "--reason", "r"]))
        .await
        .unwrap_err();
    assert!(matches!(error, CliError::Transport(_)), "{error:?}");
    assert!(
        error
            .to_string()
            .contains("check with `runtime campaign list`"),
        "{error}"
    );

    // A read has nothing to land.
    let error = client
        .runtime_command(campaign(&["show", C1]))
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("may have landed"), "{error}");
}

#[test]
fn arguments_are_checked_before_anything_is_sent() {
    let start = |extra: &[&'static str]| {
        let mut args = vec![
            "start",
            "--kind",
            "sandboxes",
            "--from",
            E0,
            "--to",
            E1,
            "--wave-size",
            "5",
            "--failure-budget",
            "3",
            "--reason",
            "r",
        ];
        args.extend_from_slice(extra);
        args
    };
    assert!(campaign_parses(&start(&[])));
    assert!(campaign_parses(&start(&["--canary-size", "1"])));
    // A wave of none could never lease anything.
    let no_wave: Vec<_> = start(&[])
        .into_iter()
        .map(|arg| if arg == "5" { "0" } else { arg })
        .collect();
    assert!(!campaign_parses(&no_wave));
    assert!(!campaign_parses(&start(&["--per-node", "0"])));
    // A budget of none would pause the campaign before it leased anything.
    let no_budget: Vec<_> = start(&[])
        .into_iter()
        .map(|arg| if arg == "3" { "0" } else { arg })
        .collect();
    assert!(!campaign_parses(&no_budget));
    // A canary upgrades one subject or none.
    assert!(!campaign_parses(&start(&["--canary-size", "2"])));
    // A misspelled kind or a malformed reference never reaches the server.
    assert!(!campaign_parses(&[
        "start",
        "--kind",
        "nodes",
        "--from",
        E0,
        "--to",
        E1,
        "--wave-size",
        "5",
        "--failure-budget",
        "3",
        "--reason",
        "r",
    ]));
    assert!(!campaign_parses(&[
        "start",
        "--kind",
        "sandboxes",
        "--from",
        "sha256:short",
        "--to",
        E1,
        "--wave-size",
        "5",
        "--failure-budget",
        "3",
        "--reason",
        "r",
    ]));
    // The wave size, failure budget and reason have no default.
    for required in ["--wave-size", "--failure-budget", "--reason"] {
        let args: Vec<_> = start(&[]);
        let at = args.iter().position(|arg| *arg == required).unwrap();
        let mut without = args.clone();
        without.drain(at..at + 2);
        assert!(!campaign_parses(&without), "{required} should be required");
    }
    // Every phase change names a reason, and a campaign id is a UUID, so it
    // never reaches a URL path otherwise.
    assert!(!campaign_parses(&["pause", C1]));
    assert!(!campaign_parses(&["resume", C1]));
    assert!(!campaign_parses(&["abort", C1]));
    assert!(!campaign_parses(&["abort", "../nodes", "--reason", "r"]));
    assert!(!campaign_parses(&["pause", "../nodes", "--reason", "r"]));
    assert!(!campaign_parses(&["show", "not-a-uuid"]));
    assert!(campaign_parses(&[
        "resume",
        C1,
        "--reason",
        "r",
        "--retry-failed",
        "--if-generation",
        "4"
    ]));
}

#[tokio::test]
async fn abort_sends_the_reason_and_the_expected_generation() {
    let (sender, mut requests) = mpsc::channel(4);
    let app = Router::new()
        .route(
            "/admin/api/v1/runtime-upgrade-campaigns/{spec}",
            axum::routing::post(
                |State(sender): State<mpsc::Sender<Recorded>>,
                 Path(spec): Path<String>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    let (id, _) = spec.rsplit_once(':').unwrap();
                    let response = json!({
                        "id": id, "phase": "aborted", "generation": 4,
                        "retried": 0, "draining": 2
                    });
                    sender
                        .send((spec.clone(), body, if_match(&headers)))
                        .await
                        .unwrap();
                    Json(response)
                },
            ),
        )
        .with_state(sender);
    with_server(app, |client| async move {
        let rendered = client
            .runtime_command(campaign(&[
                "abort",
                C1,
                "--reason",
                "0-day",
                "--if-generation",
                "3",
            ]))
            .await
            .unwrap();
        assert_eq!(
            rendered,
            format!(
                "Campaign {C1} is now aborted (generation 4).\n\
                 2 items in flight. Each worker stops at its next lease renewal, or at \
                 its own lease deadline if the database is out of reach, if it hasn't \
                 already. An item whose worker records an outcome first keeps it; \
                 the loop returns the rest to pending once their leases lapse."
            )
        );
        assert_eq!(
            requests.try_recv().unwrap(),
            (
                format!("{C1}:abort"),
                json!({"reason": "0-day"}),
                Some("3".into())
            )
        );
    })
    .await;
}

#[test]
fn counts_read_as_singular_or_plural() {
    assert_eq!(super::count(1, "failed item"), "1 failed item");
    assert_eq!(super::count(2, "failed item"), "2 failed items");
}
