use super::*;
use crate::AttributeValue;

const BOOL_ON: BoolFlag = BoolFlag::new("space-test-flag", true);
const BOOL_OFF: BoolFlag = BoolFlag::new("space-test-flag", false);
const INTEGER: IntegerFlag = IntegerFlag::new("space-test-flag", 17);
const FLOAT: FloatFlag = FloatFlag::new("space-test-flag", 0.5);
const STRING: StringFlag = StringFlag::new("space-test-flag", "fallback");
const JSON: JsonFlag = JsonFlag::new("space-test-flag");

/// Every attribute shape the provider-neutral vocabulary carries. One value,
/// used as both the provider's expectation and the call site's input, so a
/// drift between the two can't masquerade as a forwarding bug.
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

/// Every typed read of a declaration keyed on `flag_key` falls back to that
/// declaration, and the JSON read to `None`.
fn assert_every_read_falls_back(client: &FeatureFlagClient, flag_key: &'static str) {
    assert!(client.bool_value(BoolFlag::new(flag_key, true), "subject"));
    assert!(!client.bool_value(BoolFlag::new(flag_key, false), "subject"));
    assert_eq!(
        client.integer_value(IntegerFlag::new(flag_key, 17), "subject"),
        17
    );
    // Bit-compared, not value-compared: the property is that the declared
    // fallback came back unchanged, and `clippy::float_cmp` is on repo-wide.
    assert_eq!(
        client
            .float_value(FloatFlag::new(flag_key, 0.5), "subject")
            .to_bits(),
        0.5_f64.to_bits()
    );
    assert_eq!(
        client.string_value(
            StringFlag::new(flag_key, "fallback"),
            "subject",
            Attributes::default()
        ),
        "fallback"
    );
    assert_eq!(
        client.json_value(JsonFlag::new(flag_key), "subject", Attributes::default()),
        None
    );
}

/// Every typed read reports `expected` as the reason it resolved nothing. The
/// reason is what distinguishes a provider-free deployment from a broken one,
/// so it is asserted per read rather than only on the boolean path.
fn assert_every_read_is_unresolved(
    client: &FeatureFlagClient,
    flag_key: &'static str,
    expected: Unresolved,
) {
    assert_eq!(
        client.resolve_bool(
            BoolFlag::new(flag_key, true),
            "subject",
            Attributes::default()
        ),
        Err(expected),
    );
    assert_eq!(
        client.resolve_integer(
            IntegerFlag::new(flag_key, 1),
            "subject",
            Attributes::default()
        ),
        Err(expected),
    );
    assert_eq!(
        client.resolve_float(
            FloatFlag::new(flag_key, 1.0),
            "subject",
            Attributes::default()
        ),
        Err(expected),
    );
    assert_eq!(
        client.resolve_string(
            StringFlag::new(flag_key, "fallback"),
            "subject",
            Attributes::default()
        ),
        Err(expected),
    );
    assert_eq!(
        client.resolve_json(JsonFlag::new(flag_key), "subject", Attributes::default()),
        Err(expected),
    );
}

/// Answers every flag type with a distinct value, so a read that picked up
/// another type's answer would be visible rather than plausible.
struct EveryKindProvider;

impl FeatureFlagProvider for EveryKindProvider {
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
        Ok(true)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Ok(42)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Ok(0.25)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Ok("assigned".into())
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        Ok(serde_json::json!({"limit": 7}))
    }
}

/// Resolves nothing for any flag type, with a reason chosen per instance, so
/// the same matrix can be run for "not ready yet", "no assignment", and
/// "evaluation blew up".
struct ResolvesNothingProvider {
    state: ProviderState,
    reason: Unresolved,
}

impl FeatureFlagProvider for ResolvesNothingProvider {
    fn state(&self) -> ProviderState {
        self.state
    }

    fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
        Err(self.reason)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(self.reason)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(self.reason)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(self.reason)
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        Err(self.reason)
    }
}

/// Each typed read resolves its own flag type from the provider rather than the
/// declared fallback.
#[test]
fn each_typed_read_resolves_its_own_flag_type() {
    let client = FeatureFlagClient::from_provider(EveryKindProvider);

    assert_eq!(client.provider_state(), ProviderState::Ready);
    assert!(client.bool_value(BOOL_OFF, "subject"));
    assert_eq!(
        client.integer_value(IntegerFlag::new("space-test-flag", -1), "subject"),
        42
    );
    // A double is only reachable through its own read: an integer read of it
    // would convert through `as_i64()` in some backends and lose it.
    assert_eq!(
        client
            .float_value(FloatFlag::new("space-test-flag", -1.0), "subject")
            .to_bits(),
        0.25_f64.to_bits()
    );
    assert_eq!(
        client.string_value(STRING, "subject", Attributes::default()),
        "assigned"
    );
    assert_eq!(
        client.json_value(JSON, "subject", Attributes::default()),
        Some(serde_json::json!({"limit": 7}))
    );
}

/// The four situations that used to arrive at a call site as one `None`, each
/// reported as itself.
///
/// This is the matrix the whole state model exists for: before it, an OSS
/// deployment with no provider and a production deployment whose provider had
/// broken produced the same observable outcome, so one fallback value had to
/// serve both. Each row here is a distinct `Unresolved`, and the value still
/// lands on the declaration in every row — the reason changes, the fallback
/// does not. What *uses* the reason is `crate::policy`.
#[test]
fn every_reason_a_read_resolves_nothing_is_reported_as_itself() {
    let disabled = FeatureFlagClient::disabled();
    assert_eq!(disabled.provider_state(), ProviderState::Absent);
    assert!(!disabled.is_configured());
    assert_every_read_is_unresolved(&disabled, "space-test-flag", Unresolved::NoProvider);
    assert_every_read_falls_back(&disabled, "space-test-flag");

    for (state, reason) in [
        (ProviderState::Unready, Unresolved::ProviderUnready),
        (ProviderState::Ready, Unresolved::NoAssignment),
        (ProviderState::Ready, Unresolved::EvaluationFailed),
    ] {
        let client = FeatureFlagClient::from_provider(ResolvesNothingProvider { state, reason });

        assert_eq!(client.provider_state(), state);
        // Installed either way: a provider that cannot answer is still a
        // provider, which is exactly why `is_configured` alone was not enough
        // to tell these apart from the row above.
        assert!(client.is_configured());
        assert_every_read_is_unresolved(&client, "space-test-flag", reason);
        assert_every_read_falls_back(&client, "space-test-flag");
    }
}

/// An assignment of the wrong type is a provider-side "no assignment", not a
/// coercion: a JSON document that happens to be a JSON boolean is not a
/// boolean flag, and a boolean assignment is not an integer.
#[test]
fn an_assignment_of_another_type_does_not_satisfy_a_typed_read() {
    let fixed = FeatureFlagClient::fixed_bools([("space-test-flag", true)]);

    assert!(fixed.bool_value(BOOL_OFF, "subject"));
    assert_eq!(
        fixed.resolve_integer(INTEGER, "subject", Attributes::default()),
        Err(Unresolved::NoAssignment),
    );
    assert_eq!(
        fixed.resolve_string(STRING, "subject", Attributes::default()),
        Err(Unresolved::NoAssignment),
    );
    assert_eq!(
        fixed.resolve_json(JSON, "subject", Attributes::default()),
        Err(Unresolved::NoAssignment),
    );
}

/// The fallback a read lands on comes from the declaration, not from the call
/// site — two declarations of one key disagree exactly as their declarations
/// do, on the same client and the same unresolved read.
#[test]
fn the_declaration_supplies_the_fallback_not_the_call_site() {
    let disabled = FeatureFlagClient::disabled();

    assert!(disabled.bool_value(BOOL_ON, "subject"));
    assert!(!disabled.bool_value(BOOL_OFF, "subject"));
    assert_eq!(disabled.integer_value(INTEGER, "subject"), 17);
    assert_eq!(
        disabled.float_value(FLOAT, "subject").to_bits(),
        0.5_f64.to_bits()
    );
    assert_eq!(
        disabled.string_value(STRING, "subject", Attributes::default()),
        "fallback"
    );
}

/// `gate_enabled` reaches the provider and applies the policy in one call, so
/// a call site cannot read the flag and then forget to layer it. The policy
/// itself is covered in `crate::policy`; what is asserted here is that this
/// method is wired to it rather than re-deciding.
#[test]
fn gate_enabled_layers_the_env_master_over_a_real_read() {
    let assigned_off = FeatureFlagClient::fixed_bools([("space-test-flag", false)]);
    let assigned_on = FeatureFlagClient::fixed_bools([("space-test-flag", true)]);
    let unassigned = FeatureFlagClient::fixed_bools([("other-flag", true)]);
    let absent = FeatureFlagClient::disabled();

    // Env off is the hard off, including against a live `true` assignment.
    assert!(!assigned_on.gate_enabled(false, BOOL_ON, "subject"));
    // Env on: the assignment decides.
    assert!(assigned_on.gate_enabled(true, BOOL_OFF, "subject"));
    assert!(!assigned_off.gate_enabled(true, BOOL_ON, "subject"));
    // Env on and nothing resolved: the declaration decides, whether the
    // provider is installed and silent or not installed at all.
    assert!(!unassigned.gate_enabled(true, BOOL_OFF, "subject"));
    assert!(!absent.gate_enabled(true, BOOL_OFF, "subject"));
    assert!(unassigned.gate_enabled(true, BOOL_ON, "subject"));
    assert!(absent.gate_enabled(true, BOOL_ON, "subject"));
}

/// Neither test double reaches a backend, so both keep the same contract a
/// backend with those flags unset has: `fixed_bools` answers the booleans it
/// was given, and every other read on either client falls back.
#[test]
fn disabled_and_fixed_clients_keep_configuration_and_fallback_semantics() {
    let disabled = FeatureFlagClient::disabled();
    let fixed = FeatureFlagClient::fixed_bools([("on", true), ("off", false)]);

    assert_eq!(disabled.provider_state(), ProviderState::Absent);
    // A fixed map has nothing to fetch, so it is `Ready` rather than `Unready`
    // — a test double must not look like a provider that is still starting up.
    assert_eq!(fixed.provider_state(), ProviderState::Ready);
    assert!(fixed.bool_value(BoolFlag::new("on", false), "subject"));
    assert!(!fixed.bool_value(BoolFlag::new("off", true), "subject"));
    for client in [disabled, fixed] {
        assert_every_read_falls_back(&client, "missing");
        // `on` is a *set* boolean on the fixed client, so reading it as any
        // other type also has to resolve nothing rather than coerce.
        assert_eq!(
            client.integer_value(IntegerFlag::new("on", 17), "subject"),
            17
        );
        assert_eq!(
            client.string_value(
                StringFlag::new("on", "fallback"),
                "subject",
                Attributes::default()
            ),
            "fallback"
        );
        assert_eq!(
            client.json_value(JsonFlag::new("on"), "subject", Attributes::default()),
            None
        );
    }
}

/// The client forwards the flag key, subject key, and every targeting attribute
/// to the provider unchanged, and clones share one provider rather than each
/// installing their own.
#[test]
fn custom_provider_receives_targeting_inputs_and_is_shared_until_last_clone_drops() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TargetedProvider {
        calls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl FeatureFlagProvider for TargetedProvider {
        fn state(&self) -> ProviderState {
            ProviderState::Ready
        }

        fn bool_value(
            &self,
            flag_key: &str,
            subject_key: &str,
            attributes: &Attributes,
        ) -> Result<bool, Unresolved> {
            assert_eq!(
                (flag_key, subject_key, attributes),
                ("targeted", "sandbox-1", &targeting_attributes())
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(true)
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

        fn json_value(
            &self,
            _: &str,
            _: &str,
            _: &Attributes,
        ) -> Result<serde_json::Value, Unresolved> {
            Err(Unresolved::NoAssignment)
        }
    }
    impl Drop for TargetedProvider {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let client = FeatureFlagClient::from_provider(TargetedProvider {
        calls: calls.clone(),
        drops: drops.clone(),
    });
    let clone = client.clone();
    for flags in [&client, &clone] {
        assert!(flags.bool_value_with_attributes(
            BoolFlag::new("targeted", false),
            "sandbox-1",
            targeting_attributes()
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(client);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Main's string test double, kept: `fixed_strings` resolves string reads and
/// nothing else, so a non-string read of a set key still takes the
/// declaration's fallback. Three call sites outside this crate depend on it.
#[test]
fn string_assignments_use_fixed_values_and_preserve_fallbacks() {
    const MODE: StringFlag = StringFlag::new("space-test-string-flag", "api");

    for (client, expected) in [
        (FeatureFlagClient::disabled(), "api"),
        (
            FeatureFlagClient::fixed_bools([("space-test-string-flag", true)]),
            "api",
        ),
        (
            FeatureFlagClient::fixed_strings([("space-test-string-flag", "worker")]),
            "worker",
        ),
        (
            FeatureFlagClient::fixed_strings([("other-flag", "worker")]),
            "api",
        ),
    ] {
        assert_eq!(
            client.string_value(MODE, "subject-1", Attributes::default()),
            expected
        );
        assert_eq!(
            client.integer_value(IntegerFlag::new("space-test-string-flag", 17), "subject-1"),
            17
        );
        assert_eq!(
            client.json_value(
                JsonFlag::new("space-test-string-flag"),
                "subject-1",
                Attributes::default()
            ),
            None
        );
    }
}

/// A provider that reports [`ProviderState::Absent`] is not asked, and nothing
/// it would have said reaches the caller.
///
/// `Absent` means "I have no backend", and the trait promises the client then
/// treats it exactly like an empty slot. Without that check the state and the
/// reads disagree: `provider_state()` answers `Absent` while every read hands
/// back a value the provider invented, counted as `outcome:assigned`. The
/// double here answers `Ok(true)`, `99` and `"leaked"` for everything, so any
/// read that reached it shows up in the assertions below.
#[test]
fn a_provider_reporting_absent_is_never_asked() {
    struct ClaimsAbsent;
    impl FeatureFlagProvider for ClaimsAbsent {
        fn state(&self) -> ProviderState {
            ProviderState::Absent
        }

        fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
            Ok(true)
        }

        fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
            Ok(99)
        }

        fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
            Ok(99.0)
        }

        fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
            Ok("leaked".into())
        }

        fn json_value(
            &self,
            _: &str,
            _: &str,
            _: &Attributes,
        ) -> Result<serde_json::Value, Unresolved> {
            Ok(serde_json::json!("leaked"))
        }
    }

    let client = FeatureFlagClient::from_provider(ClaimsAbsent);

    assert_eq!(client.provider_state(), ProviderState::Absent);
    assert_every_read_is_unresolved(&client, "space-test-flag", Unresolved::NoProvider);
    assert_every_read_falls_back(&client, "space-test-flag");
    // And the gate consequence, which is why this matters: the declaration
    // decides. A client that asked the double anyway would take its `Ok(true)`
    // as a real assignment and answer `true` here.
    assert!(!client.gate_enabled(true, BOOL_OFF, "subject"));
}

/// No two reasons a read can resolve nothing may collapse onto the same
/// evaluation outcome.
///
/// The outcome is what a caller and an operator use to tell an OSS deployment
/// with no backend from a production one whose backend broke. If the mapping
/// in `evaluation_outcome` ever sent two reasons to one outcome, that
/// distinction would silently disappear for the collapsed pair while every
/// other test here still passed.
///
/// Injectivity is all this checks — it survives renaming an outcome or
/// permuting the mapping. Nothing asserts that a given reason emits a given
/// tag: the repository's test guidance rules out unit tests over telemetry
/// schemas, so `outcome:no_provider` in particular has no coverage and a
/// change that stopped emitting it would not fail here. And the array below is
/// hand-written: a fifth [`Unresolved`] variant is a compile error in
/// `evaluation_outcome`, which is what brings a developer to this file, but
/// nothing forces it to be added here. Add it.
#[test]
fn every_reason_maps_to_a_distinct_evaluation_outcome() {
    const EVERY_REASON: [Unresolved; 4] = [
        Unresolved::NoProvider,
        Unresolved::ProviderUnready,
        Unresolved::NoAssignment,
        Unresolved::EvaluationFailed,
    ];

    for (index, reason) in EVERY_REASON.iter().enumerate() {
        for other in &EVERY_REASON[index + 1..] {
            assert_ne!(
                evaluation_outcome(*reason),
                evaluation_outcome(*other),
                "{reason:?} and {other:?} became indistinguishable",
            );
        }
    }
}

/// Records each event's level and message so a test can assert on a log line.
/// Hand-rolled rather than reaching for `pplx-space-observability`'s visitor:
/// this crate's dependency list is the thing under test, even in dev.
#[derive(Clone, Default)]
struct StartupLogCapture(Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StartupLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }

        let mut message = Message::default();
        event.record(&mut message);
        self.0
            .lock()
            .expect("startup log capture lock poisoned")
            .push((*event.metadata().level(), message.0));
    }
}

/// When no provider ends up installed, this log line is the only
/// operator-facing evidence that every flag has gone inert — so pin its level
/// and its content. Downgrading it to `debug!` or deleting it leaves every
/// other gate in this repository green.
///
/// Deliberately ungated, because the state it describes is a runtime one and
/// reachable in *every* feature configuration: with nothing compiled in, with
/// `eppo` compiled in and no `EPPO_SDK_KEY`, and with `flagd` compiled in but
/// `SPACE_FLAGD_ENABLED` unset. A `cfg`-gated warning sees only the first.
///
/// The environment is cleared for both providers so the assertion is about the
/// no-provider-installed path rather than about whatever the host happens to
/// have exported.
///
/// What is asserted is the level and the consequence, not the provider names:
/// which backends a binary has is a `compiled_providers` field on the event,
/// and its value differs per build, so a message substring is the wrong place
/// to look for it. The capture below records only `message`.
#[test]
fn an_uninstalled_provider_warns_that_flags_are_inert() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let capture = StartupLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    {
        let vars: Vec<(&str, Option<&std::ffi::OsStr>)> = ["EPPO_SDK_KEY", "SPACE_FLAGD_ENABLED"]
            .iter()
            .map(|key| (*key, None))
            .collect();

        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        tracing::subscriber::with_default(subscriber, || {
            let client = FeatureFlagClient::from_env();
            assert_eq!(client.provider_state(), ProviderState::Absent);
        });
    };

    let events = capture
        .0
        .lock()
        .expect("startup log capture lock poisoned")
        .clone();
    assert!(
        events.iter().any(|(level, message)| {
            *level == tracing::Level::WARN
                && message.contains("no feature-flag provider was installed")
                && message.contains("fallback")
        }),
        "expected a WARN naming the consequence; captured {events:?}"
    );
}

/// Built without any provider SDK, `from_env` keeps the signature every
/// service's startup path already calls and answers with a disabled client: no
/// environment variable can configure a provider that was not compiled in.
///
/// Gated on both provider features, because that claim is what it asserts: a
/// build with either one compiled in *can* be configured by an environment
/// variable, and whether the host happens to have exported one would then
/// decide the outcome. The test above covers the runtime no-provider path in
/// those builds, with the environment cleared.
#[cfg(not(any(feature = "eppo", feature = "flagd")))]
#[test]
fn sdk_free_startup_keeps_the_existing_constructor_and_declared_fallbacks() {
    let client = FeatureFlagClient::from_env();

    assert_eq!(client.provider_state(), ProviderState::Absent);
    assert_every_read_is_unresolved(&client, "missing", Unresolved::NoProvider);
    assert_every_read_falls_back(&client, "missing");
}
