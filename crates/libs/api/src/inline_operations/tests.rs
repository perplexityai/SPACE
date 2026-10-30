use super::*;

fn runner(max_concurrent: usize, route_signing: bool) -> (InlineOperations, DrainSignal) {
    let drain_signal = DrainSignal::new();
    let runner = InlineOperations::new(
        InlineOperationsConfig { max_concurrent },
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
fn inline_permits_read_from_the_environment() {
    let config = |env: &pplx_space_test_env::TestEnv| InlineOperationsConfig::from_environment(env);
    let mut env = pplx_space_test_env::TestEnv::default();
    assert_eq!(config(&env).max_concurrent, DEFAULT_INLINE_MAX_CONCURRENT);

    env.set(INLINE_MAX_CONCURRENT_ENV, "8");
    assert_eq!(config(&env).max_concurrent, 8);

    env.set(INLINE_MAX_CONCURRENT_ENV, "0");
    assert_eq!(config(&env).max_concurrent, 0);
    env.set(INLINE_MAX_CONCURRENT_ENV, "100000");
    assert_eq!(config(&env).max_concurrent, MAX_INLINE_MAX_CONCURRENT);
    env.set(INLINE_MAX_CONCURRENT_ENV, "lots");
    assert_eq!(config(&env).max_concurrent, DEFAULT_INLINE_MAX_CONCURRENT);
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
