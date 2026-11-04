use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

use super::*;
use crate::{BoolFlag, FeatureFlagClient, FloatFlag, IntegerFlag, JsonFlag, StringFlag};

impl EppoProvider {
    /// An SDK client with no configuration and no poller, so it stays in the
    /// evaluation state a real provider passes through between startup and its
    /// first configuration fetch.
    fn unconfigured() -> Self {
        Self::unconfigured_started_ago(Duration::ZERO)
    }

    /// [`Self::unconfigured`] with the probe clock's baseline moved back, so a
    /// test can reach the elapsed-interval branch of
    /// [`EppoProvider::claim_readiness_probe`] without sleeping for it.
    fn unconfigured_started_ago(age: Duration) -> Self {
        Self {
            client: eppo::ClientConfig::from_api_key("test-key").to_client(),
            poller_thread: None,
            configuration_seen: AtomicBool::new(false),
            started_at: Instant::now()
                .checked_sub(age)
                .expect("test clock baseline is within Instant's range"),
            last_readiness_probe_millis: AtomicU64::new(NO_PROBE_YET),
            diagnosed_unassigned_flags: std::sync::Mutex::new(std::collections::HashSet::new()),
            details_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

/// Every attribute shape the provider-neutral vocabulary carries, each one
/// reachable by an Eppo targeting rule in [`configured_eppo_client`]. All five
/// conditions are ANDed there, so a dropped or mis-converted attribute turns
/// the match into a miss.
fn targeting_attributes() -> Attributes {
    [
        ("team".into(), "platform".into()),
        ("tier".into(), String::from("gold").into()),
        ("score".into(), 12.0.into()),
        ("premium".into(), true.into()),
        ("unset".into(), AttributeValue::Null),
    ]
    .into()
}

/// With no configuration fetched yet, every typed read falls back to the flag's
/// declaration and the JSON read to `None`, so a service that starts before
/// Eppo answers behaves exactly as if the flags were unset.
///
/// And it says so: the provider is installed but `Unready`, and each read
/// reports `ProviderUnready` rather than the `NoAssignment` a served
/// configuration would produce. That difference is what an operator needs to
/// tell "this flag has no allocation" from "this process never reached Eppo".
#[test]
fn a_provider_awaiting_its_first_configuration_reports_unready() {
    let client = FeatureFlagClient::from_provider(EppoProvider::unconfigured());

    assert!(client.is_configured());
    assert_eq!(client.provider_state(), ProviderState::Unready);
    assert_eq!(
        client.resolve_bool(
            BoolFlag::new("missing", true),
            "subject",
            Attributes::default()
        ),
        Err(Unresolved::ProviderUnready),
    );
    assert!(client.bool_value(BoolFlag::new("missing", true), "subject"));
    assert!(!client.bool_value(BoolFlag::new("missing", false), "subject"));
    assert_eq!(
        client.integer_value(IntegerFlag::new("missing", 17), "subject"),
        17
    );
    assert_eq!(
        client
            .float_value(FloatFlag::new("missing", 0.5), "subject")
            .to_bits(),
        0.5_f64.to_bits()
    );
    assert_eq!(
        client.string_value(
            StringFlag::new("missing", "fallback"),
            "subject",
            Attributes::default()
        ),
        "fallback"
    );
    assert_eq!(
        client.json_value(JsonFlag::new("missing"), "subject", Attributes::default()),
        None
    );
}

/// The readiness probe runs at most once per interval, and always on the first
/// read.
///
/// Both halves matter and either alone is satisfiable by a wrong
/// implementation. If the first read did not probe, a provider that is ready
/// from startup would report `ProviderUnready` until the interval elapsed. If
/// every read probed, this crate's SDK calls would double for the life of a
/// process whose configuration never arrives — the normal shape of an Eppo
/// misconfiguration.
#[test]
fn the_readiness_probe_is_claimed_at_most_once_per_interval() {
    // Two intervals of apparent uptime, so moving the stored stamp back to the
    // baseline below represents a fully elapsed window without sleeping.
    let provider = EppoProvider::unconfigured_started_ago(2 * READINESS_PROBE_INTERVAL);

    assert!(
        provider.claim_readiness_probe(),
        "the first unready read must probe, or a provider that is ready from \
         startup reports ProviderUnready until the interval elapses"
    );
    for _ in 0..1_000 {
        assert!(
            !provider.claim_readiness_probe(),
            "a second probe inside one {READINESS_PROBE_INTERVAL:?} window"
        );
    }

    provider
        .last_readiness_probe_millis
        .store(0, Ordering::Relaxed);
    assert!(
        provider.claim_readiness_probe(),
        "a probe must be allowed again once the interval has elapsed"
    );
}

/// Unassigned reads are counted every time, because that rate is the signal
/// that a flag is not reaching a deployment. The per-flag diagnostic costs an
/// extra SDK call and a log line, so it fires once per flag instead.
#[test]
fn eppo_client_observes_each_unassigned_evaluation_and_diagnoses_once() {
    let client = FeatureFlagClient::from_provider(EppoProvider::unconfigured());
    const FLAG: BoolFlag = BoolFlag::new("space-test-flag", false);

    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    assert!(!client.bool_value(FLAG, "subject-1"));
    assert!(!client.bool_value(FLAG, "subject-2"));
    let metrics = capture.finish();

    assert_eq!(
        metrics
            .iter()
            .filter(|metric| {
                metric.name == "feature_flags.evaluation"
                    && metric.tags == ["flag_key:space-test-flag", "outcome:provider_unready"]
            })
            .count(),
        2
    );
    assert_eq!(
        metrics
            .iter()
            .filter(|metric| {
                metric.name == "feature_flags.unassigned_diagnostic"
                    && metric.tags == ["flag_key:space-test-flag", "reason:configuration_missing"]
            })
            .count(),
        1
    );
}

/// The dedupe above is per flag, and the set it uses is capped, so a deployment
/// reading more distinct unset flags than the cap stops diagnosing the rest.
/// This pins the cap itself: the test above reads one key twice and would pass
/// just as happily with the cap set to 1, which would silently mean "only the
/// first unset flag a process ever sees is diagnosed" while the counter keeps
/// emitting for that one key, so no dashboard looks broken.
#[test]
fn the_unassigned_diagnostic_is_capped_but_the_evaluation_counter_is_not() {
    let client = FeatureFlagClient::from_provider(EppoProvider::unconfigured());
    let over_cap = MAX_DIAGNOSED_UNASSIGNED_FLAGS + 1;
    // A declaration's key is `&'static str`, which is the point of the type —
    // every production flag is a const. Leaking here is the test paying for a
    // key set it generates rather than declares.
    let flags: Vec<BoolFlag> = (0..over_cap)
        .map(|index| BoolFlag::new(String::leak(format!("space-capped-flag-{index}")), false))
        .collect();

    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    for flag in flags {
        assert!(!client.bool_value(flag, "subject"));
    }
    let metrics = capture.finish();

    let diagnostics = metrics
        .iter()
        .filter(|metric| metric.name == "feature_flags.unassigned_diagnostic")
        .count();
    let evaluations = metrics
        .iter()
        .filter(|metric| metric.name == "feature_flags.evaluation")
        .count();

    // Two separate properties, because asserting the mechanism against the
    // constant compares it to itself and passes for *any* value — including 1.
    // First the value, which is operational rather than arbitrary: it bounds
    // `flag_key` cardinality on the diagnostic counter, so a change should be
    // deliberate enough to update this line.
    assert_eq!(
        MAX_DIAGNOSED_UNASSIGNED_FLAGS, 64,
        "the cap bounds metric tag cardinality; changing it is an operational \
         decision, not a refactor"
    );
    // Then that the cap is actually enforced, which catches its removal.
    assert_eq!(
        diagnostics, MAX_DIAGNOSED_UNASSIGNED_FLAGS,
        "the per-flag diagnostic is bounded by the cap"
    );
    assert_eq!(
        evaluations, over_cap,
        "while the evaluation counter stays uncapped, which is the asymmetry \
         documented at the metrics helpers"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eppo_client_initializes_inside_tokio_runtime_without_panicking() {
    // Regression: `wait_for_configuration_timeout` used to call `block_on`
    // during construction, panicking with "Cannot start a runtime from
    // within a runtime" because API startup builds the client inside the
    // Tokio runtime. Constructing here must succeed without panicking; the
    // poller may fail to fetch config with this dummy key, which is fine.
    //
    // The `#[tokio::test(flavor = "multi_thread")]` attribute *is* this test:
    // with a plain `#[test]` there is no ambient runtime, construction can
    // never hit the panic, and the test passes forever while covering nothing.
    // Verified — swapping the attribute leaves it green. So assert the ambient
    // runtime rather than trusting the attribute to survive an edit.
    let runtime = tokio::runtime::Handle::try_current()
        .expect("must run inside a Tokio runtime, or this test proves nothing");
    assert_eq!(
        runtime.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "the regression needs a multi-threaded runtime to reproduce"
    );
    let config = EppoClientConfig {
        sdk_key: "dummy-sdk-key".to_string(),
        base_url: Some("http://127.0.0.1:1/".into()),
    };

    let provider = EppoProvider::from_sdk_key(config)
        .expect("Eppo client construction should not fail or panic inside a Tokio runtime");
    let client = FeatureFlagClient::from_provider(provider);

    // Evaluations fall back to their declarations until (if ever) config
    // arrives.
    assert!(client.bool_value(BoolFlag::new("space-federated-identity", true), "subject-1"));
    assert!(!client.bool_value(
        BoolFlag::new("space-federated-identity", false),
        "subject-1"
    ));
}

/// With no SDK key in the environment nobody asked for a provider, so startup
/// reports the deliberate state rather than a failure.
#[test]
fn from_env_without_an_sdk_key_produces_a_disabled_client() {
    {
        let vars: Vec<(&str, Option<&std::ffi::OsStr>)> = [EPPO_SDK_KEY_ENV, EPPO_BASE_URL_ENV]
            .iter()
            .map(|key| (*key, None))
            .collect();

        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        assert!(matches!(
            EppoProvider::from_env(),
            EppoStartup::NotConfigured
        ));
        assert_eq!(
            FeatureFlagClient::from_env().provider_state(),
            ProviderState::Absent,
        );
    };
}

/// An operator who set `EPPO_SDK_KEY` and got no provider is **not** in the
/// same situation as one who set nothing, and the client must not report them
/// the same way: the first is `Unready` and counts `outcome:provider_unready`,
/// the second is `Absent` and counts `outcome:no_provider`. Both leave every
/// flag at its declaration — what separates them is the reason, not the
/// answer.
///
/// Driven through `for_eppo_startup` rather than `from_env`, because no
/// environment reaches this arm: `start_poller_thread` fails only when a
/// background thread cannot be spawned, and a bad key or unreachable base URL
/// instead yields a provider that starts fine and never becomes ready (which
/// `from_env_with_an_sdk_key_installs_the_eppo_provider` covers). Rare is not
/// the same as unreachable, and an OpenFeature flagd backend fails at
/// construction as a matter of course.
#[test]
fn a_provider_that_failed_to_start_is_unready_rather_than_absent() {
    let broken = FeatureFlagClient::for_eppo_startup(EppoStartup::Failed);

    assert!(broken.is_configured());
    assert_eq!(broken.provider_state(), ProviderState::Unready);
    assert_eq!(
        broken.resolve_bool(
            BoolFlag::new("missing", true),
            "subject",
            Attributes::default()
        ),
        Err(Unresolved::ProviderUnready),
    );
    // The value is still the declaration's, as it is for every unresolved
    // read.
    assert!(broken.bool_value(BoolFlag::new("missing", true), "subject"));
    assert!(!broken.bool_value(BoolFlag::new("missing", false), "subject"));

    // What keeps this apart from "nobody configured a backend" is the reason,
    // which is what `provider_state` reports and what reaches Datadog as the
    // `outcome` tag. The gate answers the same for both on purpose — see
    // `crate::policy` — so the reason is asserted directly rather than
    // inferred from a gate that cannot show it.
    const FAIL_CLOSED: BoolFlag = BoolFlag::new("missing", false);
    let unconfigured = FeatureFlagClient::disabled();
    assert_eq!(unconfigured.provider_state(), ProviderState::Absent);
    assert_eq!(
        unconfigured.resolve_bool(FAIL_CLOSED, "subject", Attributes::default()),
        Err(Unresolved::NoProvider),
    );
    for (label, client) in [
        ("the provider failed to start", &broken),
        ("no backend was configured", &unconfigured),
    ] {
        assert!(
            !crate::policy::gate_enabled(
                true,
                FAIL_CLOSED,
                client.resolve_bool(FAIL_CLOSED, "subject", Attributes::default()),
            ),
            "a fail-closed gate must stay closed when {label}",
        );
    }
}

/// With an SDK key set, startup installs the Eppo provider and honours
/// `EPPO_BASE_URL`. Pointed at a dead address no configuration can ever arrive,
/// so the provider is installed *and* `Unready`, and every read falls back —
/// the two halves of what a service sees in its first moments, now
/// distinguishable from the deployment that installed no provider at all.
#[test]
fn from_env_with_an_sdk_key_installs_the_eppo_provider() {
    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> = [EPPO_SDK_KEY_ENV, EPPO_BASE_URL_ENV]
            .iter()
            .map(|key| (*key, None))
            .collect();
        vars.extend([
            (
                EPPO_SDK_KEY_ENV,
                Some(std::ffi::OsStr::new("dummy-sdk-key")),
            ),
            (
                EPPO_BASE_URL_ENV,
                Some(std::ffi::OsStr::new("http://127.0.0.1:1/")),
            ),
        ]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        let client = FeatureFlagClient::from_env();

        assert!(client.is_configured());
        assert_eq!(client.provider_state(), ProviderState::Unready);
        assert!(client.bool_value(BoolFlag::new("missing", true), "subject"));
        assert!(!client.bool_value(BoolFlag::new("missing", false), "subject"));
    };
}

/// A client backed by a real SDK client that has loaded a local Eppo
/// configuration, so the assertions below exercise the SDK's own typed reads
/// and rule evaluation rather than a hand-written double.
fn configured_eppo_provider() -> EppoProvider {
    let mut flags: serde_json::Map<_, _> = [
        ("bool", "BOOLEAN", serde_json::json!(true)),
        ("integer", "INTEGER", serde_json::json!(42)),
        ("float", "NUMERIC", serde_json::json!(0.25)),
        ("string", "STRING", serde_json::json!("assigned")),
        ("json", "JSON", serde_json::json!(r#"{"limit":7}"#)),
    ]
    .into_iter()
    .map(|(key, kind, value)| {
        (
            key.into(),
            serde_json::json!({
                "key": key, "enabled": true, "variationType": kind, "totalShards": 100,
                "variations": {"value": {"key": "value", "value": value}},
                "allocations": [
                    {"key": "allocation", "splits": [{"variationKey": "value", "shards": []}]}
                ]
            }),
        )
    })
    .collect();
    // One rule naming every attribute shape. Dropping or mis-converting any of
    // the first four turns the match into a miss. The `IS_NULL` condition is
    // weaker on purpose and must not be read as covering the null conversion:
    // Eppo computes "null" as absent-or-null (`eppo_core`
    // `eval/eval_rules.rs:71-75`), so omitting `unset` entirely would satisfy it
    // too. What it does prove is the converted value is null *to Eppo* — a
    // conversion yielding, say, the string "null" would make the rule miss.
    let mut targeted = flags.get("bool").expect("boolean fixture").clone();
    targeted["key"] = serde_json::json!("targeted");
    targeted["allocations"][0]["rules"] = serde_json::json!([{"conditions": [
        {"attribute": "id", "operator": "ONE_OF", "value": ["sandbox-1"]},
        {"attribute": "team", "operator": "ONE_OF", "value": ["platform"]},
        {"attribute": "tier", "operator": "ONE_OF", "value": ["gold"]},
        {"attribute": "score", "operator": "GTE", "value": 10},
        {"attribute": "premium", "operator": "ONE_OF", "value": ["true"]},
        {"attribute": "unset", "operator": "IS_NULL", "value": true}
    ]}]);
    flags.insert("targeted".into(), targeted);
    let body = serde_json::json!({
        "createdAt": "2026-01-01T00:00:00Z", "environment": {"name": "test"}, "flags": flags,
    })
    .to_string();

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
    listener.set_nonblocking(true).expect("nonblocking fixture");
    let base_url = format!(
        "http://{}/",
        listener.local_addr().expect("fixture address")
    );
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "Eppo did not fetch fixture");
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(err) => panic!("accept fixture: {err}"),
            }
        };
        // Accepted sockets inherit nonblocking mode on macOS. The HTTP reader
        // must wait for request bytes even when accept wins that race.
        stream
            .set_nonblocking(false)
            .expect("blocking fixture stream");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut reader = BufReader::new(&stream);
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).expect("read request") > 0);
            if line == "\r\n" {
                break;
            }
        }
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("serve fixture");
    });

    let provider = EppoProvider::from_sdk_key(EppoClientConfig {
        sdk_key: "test-key".into(),
        base_url: Some(base_url),
    })
    .expect("start provider");
    let configured = provider
        .poller_thread
        .as_ref()
        .expect("poller")
        .wait_for_configuration_timeout(Duration::from_secs(5));
    server.join().expect("fixture server");
    configured.expect("load local Eppo configuration");
    provider
}

/// Readiness is observed, not asked for — the SDK has no non-blocking query —
/// so the two observations that set it are asserted separately.
///
/// The first is the one a busy process almost always hits: any resolved
/// assignment proves a configuration arrived, with no extra SDK call. The
/// second is the one a deployment whose flags are *all* unset depends on: an
/// unassigned read fetches details and reads `config_fetched_at`. Without it
/// such a process would sit at `Unready` forever and report every read as
/// `ProviderUnready`, which is exactly the misdiagnosis this state model
/// exists to prevent.
#[test]
fn readiness_is_observed_from_an_assignment_or_from_an_unassigned_reads_details() {
    let provider = configured_eppo_provider();
    // The poller has fetched, but nothing has looked yet.
    assert_eq!(provider.state(), ProviderState::Unready);

    let client = FeatureFlagClient::from_provider(provider);
    assert!(client.bool_value(BoolFlag::new("bool", false), "subject"));
    assert_eq!(client.provider_state(), ProviderState::Ready);

    // And from the other side: a provider that only ever sees unassigned reads
    // still notices, because the details call reports the fetch.
    let unassigned_only = configured_eppo_provider();
    // Spend this flag's diagnosis slot first, which is what a process whose
    // flags were all first read *before* its configuration landed arrives at.
    // Tying the details call to that slot — the obvious way to write it, since
    // the diagnosis needs the details anyway — is what would strand such a
    // process at `Unready` and report every read as `ProviderUnready` forever.
    // Without this line the assertions below pass on the first read's own
    // diagnosis and say nothing about that.
    assert!(unassigned_only.claim_diagnosis_slot("missing"));
    assert_eq!(unassigned_only.state(), ProviderState::Unready);

    assert_eq!(
        unassigned_only.bool_value("missing", "subject", &Attributes::default()),
        Err(Unresolved::NoAssignment),
        "a served configuration with no allocation is not an unready provider",
    );
    assert_eq!(unassigned_only.state(), ProviderState::Ready);
    // The details call is what made that possible, and counting it here is what
    // gives the *zero* in
    // `a_latched_provider_answers_no_assignment_without_a_further_sdk_call`
    // its meaning: without a read that demonstrably reaches the SDK, that test
    // compares zero against zero and would pass with the instrumentation
    // deleted.
    assert_eq!(
        unassigned_only.details_calls(),
        1,
        "the readiness observation goes through get_assignment_details"
    );
}

/// An evaluation error is itself evidence of a configuration, so a provider
/// whose every read errors still reports `Ready`.
///
/// The SDK maps a missing configuration to `Ok(None)`, never `Err`
/// (`eppo_core-10.0.0/src/eval/eval_assignment.rs:114,131`), so reaching the
/// error arm proves `eval_flag` ran against a loaded configuration.
///
/// The read ordering is the whole test. Every other test here resolves a real
/// assignment first, which latches readiness for free and hides whether the
/// error arm observes anything — so this one does a cross-type read *first*,
/// against a provider that has fetched but never been looked at. Without the
/// store on that arm, an operator debugging a flag defined with the wrong
/// variation type sees `provider_state()` say `unready` for the process's life
/// while the counter says `outcome:error`.
#[test]
fn an_evaluation_error_observes_readiness_even_as_the_first_read() {
    let provider = configured_eppo_provider();
    assert_eq!(
        provider.state(),
        ProviderState::Unready,
        "fetched, but nothing has looked yet"
    );

    let client = FeatureFlagClient::from_provider(provider);
    // `bool` is a BOOLEAN flag in the fixture, so an integer read of it is a
    // type mismatch the SDK reports as an error rather than as no assignment.
    assert_eq!(
        client.resolve_integer(
            IntegerFlag::new("bool", -1),
            "subject",
            Attributes::default()
        ),
        Err(Unresolved::EvaluationFailed),
    );
    assert_eq!(client.provider_state(), ProviderState::Ready);
}

/// The steady-state path: a latched provider reading an already-diagnosed flag
/// answers from state, without asking the SDK again.
///
/// This is the line every healthy process executes on nearly every unassigned
/// read. Driven through the provider rather than a client, because the claim is
/// about how many times the SDK is consulted and `FeatureFlagClient` owns its
/// provider behind an `Arc<dyn _>` with no way back to it. The client adds only
/// its own fallback here, which `crate::client::tests` covers.
///
/// The details-call count is load-bearing, not decoration: both the latch and
/// the details path answer `NoAssignment` on a provider whose configuration has
/// been fetched, so the returned `Unresolved` cannot tell them apart. Widening
/// the early-return guard to also require `!configuration_seen` — which sends
/// every steady-state read through the SDK, unbounded — passes every assertion
/// in this suite except the count.
///
/// It also pins the pair the state model turns on, and is the only place both
/// halves are asserted against one provider and one flag: the same read answers
/// `ProviderUnready` before a configuration is observed and `NoAssignment`
/// after, so the two are not interchangeable labels for "nothing came back".
#[test]
fn a_latched_provider_answers_no_assignment_without_a_further_sdk_call() {
    const MISSING: BoolFlag = BoolFlag::new("missing", false);
    let attributes = Attributes::default();

    let provider = configured_eppo_provider();
    // Spend both routes to the details call: the flag's diagnosis slot, and
    // the probe. The probe is suppressed rather than merely claimed, because a
    // claim only lasts one interval — a stall between here and the first
    // assertion below would let it be reclaimed and change that answer. Only
    // that one depends on it; by the last, readiness is latched and the probe
    // is short-circuited whatever the stamp holds.
    assert!(provider.claim_diagnosis_slot(MISSING.key()));
    provider.suppress_readiness_probe();
    assert_eq!(
        provider.state(),
        ProviderState::Unready,
        "fetched, but nothing has observed it"
    );

    // Nothing left to ask with, so this read is classified from the latch —
    // still unset, so `ProviderUnready` is the honest answer.
    assert_eq!(
        provider.bool_value(MISSING.key(), "subject", &attributes),
        Err(Unresolved::ProviderUnready),
    );

    // Resolving any assigned flag latches readiness for free, which is the
    // path a real process almost always takes.
    assert_eq!(
        provider.bool_value("bool", "subject", &attributes),
        Ok(true)
    );
    assert_eq!(provider.state(), ProviderState::Ready);

    // Now the steady state: latched, slot spent, probe suppressed.
    let details_before = provider.details_calls();
    assert_eq!(
        provider.bool_value(MISSING.key(), "subject", &attributes),
        Err(Unresolved::NoAssignment),
        "a served configuration with no allocation is not an unready provider"
    );
    assert_eq!(
        provider.details_calls(),
        details_before,
        "a latched provider with the slot spent and the probe suppressed must \
         answer from state; asking the SDK again on every steady-state read is \
         the amplification the interval exists to bound, and in the latched \
         case nothing else bounds it"
    );
}

/// Each typed read routes to the SDK's matching typed getter: a flag read as its
/// own type resolves, and one read as another type is an Eppo `EvaluationError`
/// — counted as an error rather than as a missing assignment, with the flag's
/// declared fallback left in force. Targeting attributes and the subject key
/// both reach the SDK's rule evaluation.
#[test]
fn eppo_provider_uses_typed_sdk_assignments_and_preserves_metrics() {
    let client = FeatureFlagClient::from_provider(configured_eppo_provider());

    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    assert!(client.bool_value(BoolFlag::new("bool", false), "subject"));
    assert_eq!(
        client.integer_value(IntegerFlag::new("integer", -1), "subject"),
        42
    );
    assert_eq!(
        client
            .float_value(FloatFlag::new("float", -1.0), "subject")
            .to_bits(),
        0.25_f64.to_bits(),
        "the NUMERIC assignment must survive the SDK round trip exactly"
    );
    assert_eq!(
        client.string_value(
            StringFlag::new("string", "fallback"),
            "subject",
            Attributes::default()
        ),
        "assigned"
    );
    assert_eq!(
        client.json_value(JsonFlag::new("json"), "subject", Attributes::default()),
        Some(serde_json::json!({"limit": 7}))
    );
    assert!(client.bool_value(BoolFlag::new("integer", true), "subject"));
    assert_eq!(
        client.integer_value(IntegerFlag::new("bool", -1), "subject"),
        -1
    );
    // A NUMERIC flag is not an INTEGER flag to the SDK either.
    assert_eq!(
        client.integer_value(IntegerFlag::new("float", -1), "subject"),
        -1
    );
    assert_eq!(
        client.string_value(
            StringFlag::new("json", "fallback"),
            "subject",
            Attributes::default()
        ),
        "fallback"
    );
    assert_eq!(
        client.json_value(JsonFlag::new("string"), "subject", Attributes::default()),
        None
    );
    assert!(client.bool_value(BoolFlag::new("missing", true), "subject"));
    assert!(client.bool_value_with_attributes(
        BoolFlag::new("targeted", false),
        "sandbox-1",
        targeting_attributes()
    ));
    // Same attributes, different subject: the `id` condition is the only thing
    // that changed, so the subject key reached the rule.
    assert!(!client.bool_value_with_attributes(
        BoolFlag::new("targeted", false),
        "sandbox-2",
        targeting_attributes()
    ));
    // Same subject, no attributes: the attributes reached the rule too.
    assert!(!client.bool_value(BoolFlag::new("targeted", false), "sandbox-1"));
    // `unset` carrying an actual value misses, which is the other half of the
    // null conversion: `IS_NULL` is really being evaluated against what
    // `sdk_attributes` produced, not passing vacuously.
    let mut present = targeting_attributes();
    present.insert("unset".into(), "now-has-a-value".into());
    assert!(!client.bool_value_with_attributes(
        BoolFlag::new("targeted", false),
        "sandbox-1",
        present
    ));
    let metrics = capture.finish();

    for (flag, outcome) in [
        ("bool", "assigned"),
        ("integer", "assigned"),
        ("float", "assigned"),
        ("string", "assigned"),
        ("json", "assigned"),
        ("integer", "error"),
        ("float", "error"),
        ("bool", "error"),
        ("json", "error"),
        ("string", "error"),
        ("missing", "unassigned"),
    ] {
        assert_eq!(
            metrics
                .iter()
                .filter(|metric| metric.name == "feature_flags.evaluation"
                    && metric.tags == [format!("flag_key:{flag}"), format!("outcome:{outcome}")])
                .count(),
            1,
            "{flag} should record one {outcome} evaluation: {metrics:?}"
        );
    }
    assert_eq!(
        metrics
            .iter()
            .filter(|metric| metric.name == "feature_flags.boolean_assignment"
                && metric.tags == ["flag_key:bool", "value:true"])
            .count(),
        1
    );
}
