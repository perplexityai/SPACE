use std::sync::{Arc, Barrier};

use pplx_space_test_env::TestEnv;

use super::{
    TestDepsPolicy, append_skip_record, report_dependency_skip_to, unavailable_dependency_outcome,
};

#[test]
fn test_deps_are_required_unless_the_run_opts_out() {
    const OPTED_OUT: TestDepsPolicy = TestDepsPolicy::Optional {
        opted_out_by: "SPACE_TEST_DEPS_OPTIONAL is set",
    };
    const LEGACY_OPT_OUT: TestDepsPolicy = TestDepsPolicy::Optional {
        opted_out_by: "SPACE_CI_REQUIRE_TEST_DEPS is empty or 0",
    };
    const BY_DEFAULT: TestDepsPolicy = TestDepsPolicy::Required { required_by: None };
    const BY_CI: TestDepsPolicy = TestDepsPolicy::Required {
        required_by: Some("SPACE_CI_REQUIRE_TEST_DEPS is set"),
    };
    // (SPACE_CI_REQUIRE_TEST_DEPS, SPACE_TEST_DEPS_OPTIONAL, policy)
    for (require, optional, expected) in [
        (None, None, BY_DEFAULT),
        (None, Some("1"), OPTED_OUT),
        (None, Some("true"), OPTED_OUT),
        (None, Some("0"), BY_DEFAULT),
        (None, Some(""), BY_DEFAULT),
        // Every value CI may already set keeps its meaning: a requirement
        // outranks an opt-out inherited from the surrounding shell, and empty
        // or 0 stays optional, now saying so in each skip marker.
        (Some("1"), None, BY_CI),
        (Some("true"), None, BY_CI),
        (Some("1"), Some("1"), BY_CI),
        (Some("0"), None, LEGACY_OPT_OUT),
        (Some(""), None, LEGACY_OPT_OUT),
        (Some("0"), Some("1"), OPTED_OUT),
        (Some("0"), Some("0"), LEGACY_OPT_OUT),
    ] {
        assert_eq!(
            TestDepsPolicy::from_values(require, optional),
            expected,
            "SPACE_CI_REQUIRE_TEST_DEPS={require:?} SPACE_TEST_DEPS_OPTIONAL={optional:?}"
        );
    }
}

#[test]
fn the_policy_reads_each_variable_by_its_own_name() {
    let mut env = TestEnv::default();
    env.set("SPACE_CI_REQUIRE_TEST_DEPS", "1");
    assert_eq!(
        TestDepsPolicy::from_env(&env),
        TestDepsPolicy::Required {
            required_by: Some("SPACE_CI_REQUIRE_TEST_DEPS is set"),
        }
    );

    let mut env = TestEnv::default();
    env.set("SPACE_TEST_DEPS_OPTIONAL", "1");
    assert_eq!(
        TestDepsPolicy::from_env(&env),
        TestDepsPolicy::Optional {
            opted_out_by: "SPACE_TEST_DEPS_OPTIONAL is set",
        }
    );
}

#[test]
fn an_unavailable_dependency_fails_unless_opted_out_and_then_counts_a_skip() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("skips.log");

    let error = unavailable_dependency_outcome(
        TestDepsPolicy::Required { required_by: None },
        "redis",
        &"connection refused",
        Some(&log),
    )
    .unwrap_err()
    .to_string();
    // The failure has to tell whoever reads it what is missing and how to opt out.
    assert!(
        error.contains("redis") && error.contains("SPACE_TEST_DEPS_OPTIONAL=1"),
        "{error}"
    );
    // Required by the CI variable, the opt-out cannot help, so it is not offered.
    let error = unavailable_dependency_outcome(
        TestDepsPolicy::Required {
            required_by: Some("SPACE_CI_REQUIRE_TEST_DEPS is set"),
        },
        "redis",
        &"connection refused",
        Some(&log),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("SPACE_CI_REQUIRE_TEST_DEPS is set")
            && !error.contains("SPACE_TEST_DEPS_OPTIONAL=1"),
        "{error}"
    );
    assert!(!log.exists(), "a required dependency must not count a skip");

    unavailable_dependency_outcome(
        TestDepsPolicy::Optional {
            opted_out_by: "SPACE_TEST_DEPS_OPTIONAL is set",
        },
        "redis",
        &"connection refused",
        Some(&log),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(&log).unwrap(),
        "SPACE-TEST-DEP-SKIP: redis unavailable (SPACE_TEST_DEPS_OPTIONAL is set): \
         connection refused\n"
    );
}

#[test]
fn report_dependency_skip_appends_marker_lines_to_the_log_file() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("skips.log");

    report_dependency_skip_to("postgres unavailable: first", Some(&log)).unwrap();
    report_dependency_skip_to("redis unavailable: second", Some(&log)).unwrap();

    let contents = std::fs::read_to_string(&log).unwrap();
    assert_eq!(
        contents,
        "SPACE-TEST-DEP-SKIP: postgres unavailable: first\n\
         SPACE-TEST-DEP-SKIP: redis unavailable: second\n"
    );
}

#[test]
fn report_dependency_skip_without_log_env_only_writes_stderr() {
    // Must not panic or create files; stderr output is captured by the harness.
    report_dependency_skip_to("postgres unavailable: no log configured", None).unwrap();
}

#[test]
fn report_dependency_skip_keeps_each_record_on_one_line() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("skips.log");

    report_dependency_skip_to(
        "docker failed\ncaused by: daemon\r\nunavailable",
        Some(&log),
    )
    .unwrap();

    assert_eq!(
        std::fs::read_to_string(log).unwrap(),
        "SPACE-TEST-DEP-SKIP: docker failed caused by: daemon unavailable\n"
    );
}

#[test]
fn append_skip_record_preserves_concurrent_records() {
    const WRITERS: usize = 16;
    const RECORDS_PER_WRITER: usize = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("skips.log");
    let barrier = Arc::new(Barrier::new(WRITERS));

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let log = log.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for record in 0..RECORDS_PER_WRITER {
                    let line = format!("SPACE-TEST-DEP-SKIP: writer={writer} record={record}\n");
                    append_skip_record(&log, line.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }

    let mut actual: Vec<_> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let mut expected: Vec<_> = (0..WRITERS)
        .flat_map(|writer| {
            (0..RECORDS_PER_WRITER)
                .map(move |record| format!("SPACE-TEST-DEP-SKIP: writer={writer} record={record}"))
        })
        .collect();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn report_dependency_skip_surfaces_configured_log_errors() {
    let dir = tempfile::tempdir().unwrap();
    // A directory cannot be opened as the configured append-only log file.
    let result = report_dependency_skip_to("postgres unavailable: broken log", Some(dir.path()));

    let error = result.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::IsADirectory);
    assert!(
        error
            .to_string()
            .contains("failed to append SPACE_TEST_DEP_SKIP_LOG at")
    );
    assert!(error.to_string().contains(dir.path().to_str().unwrap()));
}

#[test]
fn report_dependency_skip_rejects_relative_log_paths() {
    let result = report_dependency_skip_to(
        "postgres unavailable: relative log",
        Some(std::path::Path::new("skips.log")),
    );

    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
}
