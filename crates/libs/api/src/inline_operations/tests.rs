use super::*;

fn runner(max_concurrent: usize, route_signing: bool) -> (InlineOperations, DrainSignal) {
    let drain_signal = DrainSignal::new();
    let runner = InlineOperations::new(
        InlineOperationsConfig {
            max_concurrent,
            ..InlineOperationsConfig::default()
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
        }
    );

    env.set(INLINE_MAX_CONCURRENT_ENV, "0");
    assert_eq!(config(&env).max_concurrent, 0);
    env.set(INLINE_MAX_CONCURRENT_ENV, "100000");
    assert_eq!(config(&env).max_concurrent, MAX_INLINE_MAX_CONCURRENT);
    env.set(INLINE_MAX_CONCURRENT_ENV, "lots");
    assert_eq!(config(&env).max_concurrent, DEFAULT_INLINE_MAX_CONCURRENT);
}

#[test]
fn a_kind_runs_inline_only_with_the_master_on_and_in_the_allowlist() {
    let runner = |enabled, kinds: &[OperationKind]| {
        InlineOperations::new(
            InlineOperationsConfig {
                enabled,
                kinds: kinds.to_vec(),
                ..InlineOperationsConfig::default()
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
