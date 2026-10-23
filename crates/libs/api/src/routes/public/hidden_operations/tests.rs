use super::*;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_feature_flags::{AttributeValue, FeatureFlagProvider, ProviderState, Unresolved};
use pplx_space_public_api::{PublicSandboxId, routes::NATIVE_V1_OPERATIONS};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env::VarError;
use tower::ServiceExt;
use uuid::Uuid;

use crate::federated_identity::{
    FEDERATED_ORG_ID_HEADER, FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
    FEDERATED_SERVICE_GRANT_HEADER, FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
    FEDERATED_USER_ID_HEADER,
};

const KEY: &str = "hidden-operations-test-key";
const FEDERATED_ORG: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";
/// Every native router mount the tests exercise, including one under a prefix
/// the gate has never heard of: it must not depend on where it is nested.
const ROOTS: [&str; 3] = [
    "/v1",
    crate::routes::PUBLIC_API_SHARED_ROOT,
    "/elsewhere/v1",
];

fn listing(orgs: EnabledOrgs) -> HiddenOperationsGate {
    HiddenOperationsGate {
        orgs,
        ..HiddenOperationsGate::default()
    }
}

#[test]
fn the_env_override_names_every_org_or_a_list_of_them() {
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let other = OrgId::from_uuid(Uuid::from_u128(2));
    for (raw, expected) in [
        (None, Ok(EnabledOrgs::None)),
        (Some(""), Ok(EnabledOrgs::None)),
        (Some(" "), Ok(EnabledOrgs::None)),
        (Some("*"), Ok(EnabledOrgs::All)),
        (
            Some(&format!("{org}, {other}")),
            Ok(EnabledOrgs::Listed(BTreeSet::from([org, other]))),
        ),
        (
            Some(&format!("{org},*")),
            Err("SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS: `*` must stand alone, not in a list of org ids".to_owned()),
        ),
        (
            Some(&format!("{org},")),
            Err(r#"SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS must be `*` or comma-separated org ids; "" is not an org id"#.to_owned()),
        ),
        (
            Some("not-an-org"),
            Err(r#"SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS must be `*` or comma-separated org ids; "not-an-org" is not an org id"#.to_owned()),
        ),
    ] {
        let orgs = raw.map_or(Err(VarError::NotPresent), |raw| Ok(raw.to_owned()));
        let parsed = HiddenOperationsGate::from_vars(orgs, Err(VarError::NotPresent))
            .map(|gate| gate.orgs)
            .map_err(|error| error.to_string());
        assert_eq!(parsed, expected, "{raw:?}");
    }
}

#[test]
fn the_flag_master_is_on_unless_set_off_and_refuses_anything_else() {
    for (raw, expected) in [
        (None, Some(true)),
        (Some(""), Some(true)),
        (Some("true"), Some(true)),
        (Some("1"), Some(true)),
        (Some("on"), Some(true)),
        (Some("enabled"), Some(true)),
        (Some("FALSE"), Some(false)),
        (Some(" 0 "), Some(false)),
        (Some("off"), Some(false)),
        (Some("Off"), Some(false)),
        (Some("no"), Some(false)),
        (Some("disabled"), Some(false)),
        (Some("off-ish"), None),
    ] {
        let master = raw.map_or(Err(VarError::NotPresent), |raw| Ok(raw.to_owned()));
        let parsed = HiddenOperationsGate::from_vars(Err(VarError::NotPresent), master)
            .ok()
            .map(|gate| gate.flag_enabled);
        assert_eq!(parsed, expected, "{raw:?}");
    }
}

#[test]
fn a_value_that_is_not_unicode_fails_startup_rather_than_reading_as_unset() {
    use std::os::unix::ffi::OsStringExt;
    let not_unicode = || {
        Err(VarError::NotUnicode(std::ffi::OsString::from_vec(vec![
            0xff,
        ])))
    };
    for (orgs, master, name) in [
        (
            not_unicode(),
            Err(VarError::NotPresent),
            HIDDEN_OPERATIONS_ORGS_ENV,
        ),
        (
            Err(VarError::NotPresent),
            not_unicode(),
            HIDDEN_OPERATIONS_FLAG_ENV,
        ),
    ] {
        assert!(
            matches!(
                HiddenOperationsGate::from_vars(orgs, master),
                Err(HiddenOperationsConfigError::NotUnicode(failed)) if failed == name
            ),
            "{name}"
        );
    }
}

/// With its env master off the flag opens nothing, even for an org it
/// targets; the env list still opens the orgs it names.
#[test]
fn the_flag_master_closes_every_org_the_flag_opened_and_none_the_list_did() {
    let listed = OrgId::from_uuid(SEED_ORG_ID);
    let flagged = OrgId::from_uuid(Uuid::from_u128(2));
    let flags = FeatureFlagClient::fixed_bools([(SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key(), true)]);
    let gate = |flag_enabled| HiddenOperationsGate {
        orgs: EnabledOrgs::Listed(BTreeSet::from([listed])),
        flag_enabled,
    };
    for (flag_enabled, case, org, expected) in [
        (true, "flagged org", flagged, true),
        (false, "flagged org", flagged, false),
        (true, "listed org", listed, true),
        (false, "listed org", listed, true),
    ] {
        assert_eq!(
            gate(flag_enabled).allows(&flags, org, native::LIST_SNAPSHOTS),
            expected,
            "master {flag_enabled}, {case}"
        );
        assert!(gate(flag_enabled).allows(&flags, org, native::LIST_PROCESSES));
    }
}

/// Each hidden operation is enforced by exactly one of the two checks: it owns
/// its mount and gets the route gate, or it is a sandbox action, which
/// `parse_action_segment` checks.
#[test]
fn every_hidden_operation_is_enforced_by_its_route_or_its_action() {
    let mut mounts = BTreeMap::<_, Vec<NativeOperation>>::new();
    for operation in NATIVE_V1_OPERATIONS {
        assert!(
            operation.surface() == NativeSurface::ControlPlane
                || !operation.status().is_hidden_by_default(),
            "{operation:?}"
        );
        if operation.surface() == NativeSurface::ControlPlane {
            mounts
                .entry((operation.relative_runtime_path(), operation.method()))
                .or_default()
                .push(*operation);
        }
    }
    let mounts = mounts.values().map(Vec::as_slice).collect::<Vec<_>>();
    for (operations, gate) in mounts.iter().zip(route_gates(&mounts)) {
        for operation in *operations {
            if !operation.status().is_hidden_by_default() {
                assert_ne!(gate, Some(*operation), "{operation:?}");
            } else if gate != Some(*operation) {
                let action = operation
                    .path()
                    .rsplit_once(':')
                    .and_then(|(_, action)| SandboxLifecycleAction::parse_known(action).ok())
                    .unwrap_or_else(|| panic!("{operation:?} is not enforced"));
                assert_eq!(
                    super::super::sandboxes::lifecycle_operation(action),
                    *operation
                );
            }
        }
    }
}

/// Building the router refuses a hidden operation that neither check would
/// cover, rather than serving it to every org.
#[test]
fn the_router_refuses_a_hidden_operation_no_check_covers() {
    let mounts: &[&[NativeOperation]] = &[
        &[native::LIST_SNAPSHOTS],
        &[native::GET_SESSION],
        &[native::REVOKE_SESSION],
        &[
            native::START_SANDBOX,
            native::ROLLBACK_SANDBOX,
            native::SET_SANDBOX_TIMEOUT,
        ],
        &[native::LIST_PROCESSES, native::CREATE_COMMAND],
    ];
    let manifest = &[
        native::LIST_SNAPSHOTS,
        native::GET_SESSION,
        native::REVOKE_SESSION,
        native::START_SANDBOX,
        native::ROLLBACK_SANDBOX,
        native::SET_SANDBOX_TIMEOUT,
        native::LIST_PROCESSES,
        native::CREATE_COMMAND,
        native::STREAM_SESSION_EVENTS,
    ];
    let gates = |hidden: &'static [NativeOperation]| {
        std::panic::catch_unwind(|| {
            route_gates_marking(mounts, manifest, |operation| hidden.contains(&operation))
        })
    };
    assert_eq!(
        gates(&[native::LIST_SNAPSHOTS, native::ROLLBACK_SANDBOX]).ok(),
        Some(vec![Some(native::LIST_SNAPSHOTS), None, None, None, None]),
    );
    for (hidden, why) in [
        (
            &[native::LIST_PROCESSES][..],
            "a shared mount that is not the action mount",
        ),
        (
            &[native::SET_SANDBOX_TIMEOUT],
            "an action answered before the action parser",
        ),
        (&[native::STREAM_SESSION_EVENTS], "a data-plane operation"),
        (
            &[native::GET_SESSION],
            "a path another method is mounted on",
        ),
    ] {
        assert!(gates(hidden).is_err(), "{why}");
    }
}

/// A flag backend that opens hidden operations only for `subject`, and only
/// the operation `operation_id` names.
struct Targeted {
    subject: String,
    operation_id: &'static str,
}

impl FeatureFlagProvider for Targeted {
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        assert_eq!(flag_key, SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key());
        Ok(subject_key == self.subject
            && attributes.get("operation_id")
                == Some(&AttributeValue::String(self.operation_id.to_owned())))
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn json_value(&self, _: &str, _: &str, _: &Attributes) -> Result<Value, Unresolved> {
        Err(Unresolved::NoAssignment)
    }
}

fn owner_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(FEDERATED_ORG_ID_HEADER, FEDERATED_ORG.parse().unwrap());
    headers.insert(FEDERATED_USER_ID_HEADER, "101".parse().unwrap());
    headers
}

/// A local caller in the integration key's own org, and a federated end user
/// the same key asserts, each with a sandbox of their own.
struct Fixture {
    _db: TestDb,
    app: Router,
    local_sandbox: String,
    federated_sandbox: String,
}

impl Fixture {
    /// `gate` and `flags` are handed the federated caller's effective org,
    /// which only exists once that caller has made a request.
    async fn new(
        gate: impl FnOnce(OrgId) -> HiddenOperationsGate,
        flags: impl FnOnce(OrgId) -> FeatureFlagClient,
    ) -> TestDbResult<Self> {
        let db = pg_test_db().await?;
        sqlx::query(sql!(
            "INSERT INTO api_keys (
                 id, org_id, name, key_hash, allowed_surfaces, federated_owner_assertions_enabled
             ) VALUES ($1, $2, 'hidden-operations-test', $3, ARRAY['native'], TRUE)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(crate::auth::hash_token(KEY))
        .execute(&db.pool)
        .await?;
        let mut state = AppState::from_pool(db.pool.clone()).await?;
        state.federated_identity = crate::federated_identity::FederatedIdentityConfig {
            enabled: true,
            policy: crate::federated_identity::FederatedIdentityPolicy::parse(
                Some("test-issuer"),
                None,
                Some(r#"["0"]"#),
            )?,
        };
        let local_sandbox = Self::seed(&state, &HeaderMap::new()).await?;
        let mut grant = owner_headers();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        grant.insert(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant".parse()?);
        grant.insert(
            FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
            (now + 3600).to_string().parse()?,
        );
        grant.insert(
            FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
            now.to_string().parse()?,
        );
        let federated_sandbox = Self::seed(&state, &grant).await?;
        let federated_org =
            sqlx::query_scalar::<_, Uuid>(sql!("SELECT org_id FROM sandboxes WHERE id = $1"))
                .bind(federated_sandbox.parse::<PublicSandboxId>()?.as_uuid())
                .fetch_one(&db.pool)
                .await?;
        let federated_org = OrgId::from_uuid(federated_org);
        assert_ne!(federated_org, OrgId::from_uuid(SEED_ORG_ID));
        state.native_hidden_operations = gate(federated_org);
        state.feature_flags = flags(federated_org);
        Ok(Self {
            _db: db,
            app: app(state),
            local_sandbox,
            federated_sandbox,
        })
    }

    /// Admit a create as `headers` names the caller, and record its sandbox
    /// as started.
    async fn seed(state: &AppState, headers: &HeaderMap) -> TestDbResult<String> {
        let (status, _, created) = send(
            &app(state.clone()),
            Method::POST,
            "/v1/sandboxes",
            headers,
            json!({"template": "seed:latest"}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{created}");
        let sandbox_id: PublicSandboxId = created["sandbox_id"].as_str().unwrap().parse()?;
        let operation_id: pplx_space_public_api::OperationId =
            created["operation_id"].as_str().unwrap().parse()?;
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
                 network_policy_generation)
             SELECT $1, actor_user_id, org_id, 'started', $3, 1
             FROM public_operations WHERE operation_id = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(operation_id.as_uuid())
        .bind(sqlx::types::Json(
            pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(&state.pool)
        .await?;
        Ok(sandbox_id.to_string())
    }
}

fn app(state: AppState) -> Router {
    ROOTS
        .into_iter()
        .fold(Router::new(), |router, root| {
            router.nest(root, crate::routes::public::v1_router(state.clone()))
        })
        .with_state(state)
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    body: Value,
) -> TestDbResult<(StatusCode, HeaderMap, Value)> {
    let mut request = Request::builder()
        .method(method.clone())
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header("idempotency-key", format!("key-{method}-{path}"))
        .header("x-request-id", "req-hidden");
    request.headers_mut().unwrap().extend(headers.clone());
    let response = app
        .clone()
        .oneshot(request.body(Body::from(serde_json::to_vec(&body)?))?)
        .await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body)?
    };
    Ok((status, headers, body))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Caller {
    Local,
    Federated,
}

type GateFor = Box<dyn Fn(OrgId) -> HiddenOperationsGate>;
type FlagsFor = Box<dyn Fn(OrgId) -> FeatureFlagClient>;
type Enabled = fn(Caller, NativeOperation) -> bool;

#[tokio::test]
async fn hidden_operations_answer_as_nonexistent_unless_the_effective_org_is_enabled()
-> TestDbResult<()> {
    let local_org = OrgId::from_uuid(SEED_ORG_ID);
    let unrelated = OrgId::from_uuid(Uuid::from_u128(2));
    let cases: [(&str, GateFor, FlagsFor, Enabled); 7] = [
        (
            "nothing enabled",
            Box::new(|_| HiddenOperationsGate::default()),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| false,
        ),
        (
            "an unrelated org listed",
            Box::new(move |_| listing(EnabledOrgs::Listed(BTreeSet::from([unrelated])))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| false,
        ),
        // The key's own org owns only the key's local data, not its end users'.
        (
            "the integration key's org listed",
            Box::new(move |_| listing(EnabledOrgs::Listed(BTreeSet::from([local_org])))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |caller, _| caller == Caller::Local,
        ),
        (
            "the federated owner's org listed",
            Box::new(|federated| listing(EnabledOrgs::Listed(BTreeSet::from([federated])))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |caller, _| caller == Caller::Federated,
        ),
        (
            "every org",
            Box::new(|_| listing(EnabledOrgs::All)),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| true,
        ),
        (
            "the flag on for everyone",
            Box::new(|_| HiddenOperationsGate::default()),
            Box::new(|_| {
                FeatureFlagClient::fixed_bools([(SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key(), true)])
            }),
            |_, _| true,
        ),
        (
            "the flag targeting the federated org's snapshot list",
            Box::new(|_| HiddenOperationsGate::default()),
            Box::new(|federated| {
                FeatureFlagClient::from_provider(Targeted {
                    subject: federated.to_string(),
                    operation_id: native::LIST_SNAPSHOTS.operation_id(),
                })
            }),
            |caller, operation| caller == Caller::Federated && operation == native::LIST_SNAPSHOTS,
        ),
    ];
    for (case, gate, flags, enabled) in cases {
        let fixture = Fixture::new(gate, flags).await?;
        for (caller, headers, own, foreign) in [
            (
                Caller::Local,
                HeaderMap::new(),
                &fixture.local_sandbox,
                &fixture.federated_sandbox,
            ),
            (
                Caller::Federated,
                owner_headers(),
                &fixture.federated_sandbox,
                &fixture.local_sandbox,
            ),
        ] {
            let app = &fixture.app;
            let ids = [own.as_str(), "not-a-sandbox-id", foreign.as_str()];
            for root in ROOTS {
                let case = format!("{case}: {caller:?} under {root}");

                // A route only a hidden operation owns answers, for every
                // method and every id, as a route that does not exist,
                // headers included: no `Allow` naming the hidden method.
                let visible = enabled(caller, native::LIST_SNAPSHOTS);
                for (method, id, visible_status) in [
                    (Method::GET, ids[0], StatusCode::OK),
                    (Method::GET, ids[1], StatusCode::BAD_REQUEST),
                    (Method::GET, ids[2], StatusCode::NOT_FOUND),
                    (Method::HEAD, ids[0], StatusCode::OK),
                    (Method::OPTIONS, ids[0], StatusCode::METHOD_NOT_ALLOWED),
                    (Method::POST, ids[0], StatusCode::METHOD_NOT_ALLOWED),
                ] {
                    let path = format!("{root}/sandboxes/{id}/snapshots");
                    let answer = send(app, method.clone(), &path, &headers, Value::Null).await?;
                    if visible {
                        assert_eq!(
                            answer.0, visible_status,
                            "{case}: {method} {path}: {answer:?}"
                        );
                    } else {
                        let unmounted = format!("{root}/sandboxes/{id}/no-such-route");
                        let absent =
                            send(app, method.clone(), &unmounted, &headers, Value::Null).await?;
                        assert_eq!(absent.0, StatusCode::NOT_FOUND, "{case}: {absent:?}");
                        assert_eq!(answer, absent, "{case}: {method} {path}");
                    }
                }

                // A hidden action answers, for every id, as an unknown one.
                let visible = enabled(caller, native::ROLLBACK_SANDBOX);
                for id in ids {
                    let path = format!("{root}/sandboxes/{id}:rollback");
                    let body = json!({"snapshot_name": "s"});
                    let answer = send(app, Method::POST, &path, &headers, body.clone()).await?;
                    if visible && id == own.as_str() {
                        assert_eq!(answer.0, StatusCode::ACCEPTED, "{case}: {path}: {answer:?}");
                    } else {
                        let unknown = format!("{root}/sandboxes/{id}:no_such_action");
                        let absent = send(app, Method::POST, &unknown, &headers, body).await?;
                        assert_ne!(absent.0, StatusCode::ACCEPTED, "{case}: {absent:?}");
                        assert_eq!(answer, absent, "{case}: {path}");
                    }
                }
            }

            // Visible incubating actions on the same mount are served
            // regardless: `:resume` and `:restore` reach transitions `:wake`
            // already offers, and `:disable_snapshots` is the off switch for
            // a schedule enabled before the gate.
            for action in ["stop", "resume", "restore", "disable_snapshots"] {
                let path = format!("/v1/sandboxes/{own}:{action}");
                let answer = send(app, Method::POST, &path, &headers, Value::Null).await?;
                assert_eq!(
                    answer.0,
                    StatusCode::ACCEPTED,
                    "{case}: {caller:?}: {path}: {answer:?}"
                );
            }
        }
    }
    Ok(())
}

/// The full document lists hidden operations, so it needs the same
/// credentials as `/v1`; the default one stays public. It reads no tenant
/// data, so a service credential that resolves no actor may fetch it too.
#[tokio::test]
async fn only_the_full_contract_requires_authentication() -> TestDbResult<()> {
    const ACTORLESS_KEY: &str = "hidden-operations-docs-actorless-key";
    let db = pg_test_db().await?;
    // The second seeded org has no users, so its key resolves no actor.
    for (key, org) in [(KEY, SEED_ORG_ID), (ACTORLESS_KEY, Uuid::from_u128(2))] {
        sqlx::query(sql!(
            "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
             VALUES ($1, $2, 'hidden-operations-docs-test', $3, ARRAY['native'])"
        ))
        .bind(Uuid::now_v7())
        .bind(org)
        .bind(crate::auth::hash_token(key))
        .execute(&db.pool)
        .await?;
    }
    let app = crate::routes::router(AppState::from_pool(db.pool.clone()).await?);
    let operations = |document: &Value| {
        document["paths"]
            .as_object()
            .expect("paths should be an object")
            .values()
            .flat_map(|item| item.as_object().expect("path item").values())
            .filter(|operation| operation.get("operationId").is_some())
            .count()
    };
    let get = async |path: String, token: Option<&str>| -> TestDbResult<(StatusCode, Value)> {
        let mut request = Request::builder().uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = app.clone().oneshot(request.body(Body::empty())?).await?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024).await?;
        Ok((status, serde_json::from_slice(&body)?))
    };
    for root in ["", "/api"] {
        let (status, default) = get(format!("{root}/openapi.json"), None).await?;
        assert_eq!(status, StatusCode::OK, "{root}/openapi.json");
        let (status, body) = get(format!("{root}/openapi-full.json"), None).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{root}: {body}");
        assert_eq!(body["code"], "unauthorized", "{root}: {body}");
        for key in [KEY, ACTORLESS_KEY] {
            let (status, full) = get(format!("{root}/openapi-full.json"), Some(key)).await?;
            assert_eq!(
                status,
                StatusCode::OK,
                "{root}/openapi-full.json: {key}: {full}"
            );
            assert_eq!(operations(&full), NATIVE_V1_OPERATIONS.len(), "{root}");
            assert!(operations(&default) < operations(&full), "{root}");
        }
        // The marker the documents set does not leak to `/v1`.
        let (status, body) = get(format!("{root}/v1/templates"), Some(ACTORLESS_KEY)).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{root}/v1/templates: {body}");
    }
    Ok(())
}
