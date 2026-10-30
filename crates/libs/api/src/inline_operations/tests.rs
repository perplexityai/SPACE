use super::*;

fn runner(max_concurrent: usize, route_signing: bool) -> (InlineOperations, DrainSignal) {
    let drain_signal = DrainSignal::new();
    let runner = InlineOperations::new(
        InlineOperationsConfig {
            max_concurrent,
            ..Default::default()
        },
        TaskTracker::new(),
        drain_signal.clone(),
        OperationExecutionConfig::default(),
        route_signing,
    );
    (runner, drain_signal)
}

fn acquire(runner: &InlineOperations) -> Result<InlinePermit, InlineRefusal> {
    runner.try_acquire(InlineAdmissionPath::WarmPoolCreate)
}

#[test]
fn inline_switches_default_off_and_read_from_the_environment() {
    let config = |env: &pplx_space_test_env::TestEnv| InlineOperationsConfig::from_environment(env);
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(
        config(&env),
        InlineOperationsConfig {
            enabled: false,
            kinds: Vec::new(),
            max_concurrent: DEFAULT_INLINE_MAX_CONCURRENT,
            org_limit: None,
        }
    );

    env.set(INLINE_OPERATIONS_ENABLED_ENV, "true");
    // Kinds that cannot run inline, and names that are not kinds, are left
    // out; a repeat counts once.
    env.set(
        INLINE_KINDS_ENV,
        " wake_sandbox,create_sandbox, delete_sandbox,,not_a_kind,wake_sandbox",
    );
    env.set(INLINE_MAX_CONCURRENT_ENV, "8");
    assert_eq!(
        config(&env),
        InlineOperationsConfig {
            enabled: true,
            kinds: vec![OperationKind::WakeSandbox, OperationKind::CreateSandbox],
            max_concurrent: 8,
            org_limit: None,
        }
    );

    env.set(INLINE_MAX_CONCURRENT_ENV, "0");
    assert_eq!(config(&env).max_concurrent, 0);
    env.set(INLINE_MAX_CONCURRENT_ENV, "100000");
    assert_eq!(config(&env).max_concurrent, MAX_INLINE_MAX_CONCURRENT);
    env.set(INLINE_MAX_CONCURRENT_ENV, "lots");
    assert_eq!(config(&env).max_concurrent, DEFAULT_INLINE_MAX_CONCURRENT);

    env.set(INLINE_ORG_LIMIT_ENV, " 3 ");
    assert_eq!(config(&env).org_limit, NonZeroU32::new(3));
    for no_limit in ["0", "", "lots", "-1"] {
        env.set(INLINE_ORG_LIMIT_ENV, no_limit);
        assert_eq!(config(&env).org_limit, None, "{no_limit:?}");
    }
}

#[test]
fn a_kind_runs_inline_only_with_the_master_on_and_in_the_allowlist() {
    let runner = |enabled, kinds: &[OperationKind]| {
        InlineOperations::new(
            InlineOperationsConfig {
                enabled,
                kinds: kinds.to_vec(),
                ..Default::default()
            },
            TaskTracker::new(),
            DrainSignal::new(),
            OperationExecutionConfig::default(),
            true,
        )
    };
    let wake = OperationKind::WakeSandbox;
    assert!(runner(true, &[wake]).kind_enabled(wake));
    assert!(!runner(false, &[wake]).kind_enabled(wake));
    assert!(!runner(true, &[OperationKind::CreateSandbox]).kind_enabled(wake));
    assert!(!runner(true, &[]).kind_enabled(wake));
}

#[test]
fn without_route_signing_nothing_runs_inline() {
    let (runner, _) = runner(4, false);
    assert_eq!(
        acquire(&runner).err(),
        Some(InlineRefusal::RouteSigningMissing)
    );
    let (runner, _) = self::runner(4, true);
    drop(acquire(&runner).expect("a permit with route signing"));
}

#[test]
fn permits_bound_inline_work_and_refuse_while_draining() {
    let (runner, drain_signal) = runner(1, true);
    let held = acquire(&runner).expect("a free permit");
    assert_eq!(acquire(&runner).err(), Some(InlineRefusal::Saturated));
    drop(held);
    let held = acquire(&runner).expect("the permit is free again");
    drop(held);

    drain_signal.begin();
    assert_eq!(acquire(&runner).err(), Some(InlineRefusal::Draining));

    let (off, _) = self::runner(0, true);
    assert_eq!(acquire(&off).err(), Some(InlineRefusal::Disabled));
}

/// Never runs: the gate tests offer nothing for execution.
struct NoExecution;

impl InlineExecutor for NoExecution {
    fn execute(&self, _: ClaimedPublicOperation) -> InlineExecution {
        unreachable!("nothing is executed")
    }
}

/// A flag backend that turns the inline flag off for one subject and has no
/// assignment for any other.
struct HoldsBack(String);

impl pplx_space_feature_flags::FeatureFlagProvider for HoldsBack {
    fn state(&self) -> pplx_space_feature_flags::ProviderState {
        pplx_space_feature_flags::ProviderState::Ready
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        _: &pplx_space_feature_flags::Attributes,
    ) -> Result<bool, pplx_space_feature_flags::Unresolved> {
        if flag_key == SPACE_NATIVE_INLINE_OPERATIONS_FLAG.key() && subject_key == self.0 {
            Ok(false)
        } else {
            Err(pplx_space_feature_flags::Unresolved::NoAssignment)
        }
    }

    fn integer_value(
        &self,
        _: &str,
        _: &str,
        _: &pplx_space_feature_flags::Attributes,
    ) -> Result<i64, pplx_space_feature_flags::Unresolved> {
        Err(pplx_space_feature_flags::Unresolved::NoAssignment)
    }

    fn float_value(
        &self,
        _: &str,
        _: &str,
        _: &pplx_space_feature_flags::Attributes,
    ) -> Result<f64, pplx_space_feature_flags::Unresolved> {
        Err(pplx_space_feature_flags::Unresolved::NoAssignment)
    }

    fn string_value(
        &self,
        _: &str,
        _: &str,
        _: &pplx_space_feature_flags::Attributes,
    ) -> Result<String, pplx_space_feature_flags::Unresolved> {
        Err(pplx_space_feature_flags::Unresolved::NoAssignment)
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &pplx_space_feature_flags::Attributes,
    ) -> Result<serde_json::Value, pplx_space_feature_flags::Unresolved> {
        Err(pplx_space_feature_flags::Unresolved::NoAssignment)
    }
}

/// An organization gets inline lifecycle work only with the env master on,
/// the kind listed, and the flag, read with the organization as its
/// subject, allowing it. With no flag backend, or no assignment, the master
/// decides; a backend can hold one organization back but never turn inline
/// execution on under a master that is off.
#[test]
fn the_flag_holds_an_organization_back_under_the_env_master() {
    let wake = OperationKind::WakeSandbox;
    let held_back = OrgId::from_uuid(uuid::Uuid::now_v7());
    let other = OrgId::from_uuid(uuid::Uuid::now_v7());
    let lifecycle = |enabled, kinds: &[OperationKind], flags| {
        InlineLifecycle::new(
            InlineOperations::new(
                InlineOperationsConfig {
                    enabled,
                    kinds: kinds.to_vec(),
                    ..Default::default()
                },
                TaskTracker::new(),
                DrainSignal::new(),
                OperationExecutionConfig::default(),
                true,
            ),
            Arc::new(NoExecution),
            flags,
        )
    };
    let holds_back = || FeatureFlagClient::from_provider(HoldsBack(held_back.to_string()));
    let all_on =
        || FeatureFlagClient::fixed_bools([(SPACE_NATIVE_INLINE_OPERATIONS_FLAG.key(), true)]);
    let no_backend = FeatureFlagClient::disabled;
    let start = OperationKind::StartSandbox;
    for (enabled, kinds, flags, org, expected) in [
        (true, &[wake][..], no_backend(), held_back, true),
        (true, &[wake], holds_back(), held_back, false),
        (true, &[wake], holds_back(), other, true),
        (true, &[start], holds_back(), other, false),
        (false, &[wake], all_on(), other, false),
        (false, &[wake], no_backend(), other, false),
    ] {
        assert_eq!(
            lifecycle(enabled, kinds, flags).enabled_for(org, wake),
            expected,
            "master {enabled}, kinds {kinds:?}, held back {}",
            org == held_back
        );
    }
}
