use pplx_space_content_digest::Digest;

use super::*;

const NAME: &str = "seed";
const HASH: &str = "sha256:5555555555555555555555555555555555555555555555555555555555555555";

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

fn partition(environment: EnvironmentReference) -> TemplateCachePartition {
    pplx_space_node_runtime_core::catalog::template_cache_partition(Some(environment))
}

/// A ready build of the digest bound to `environment`, in the partition its
/// binding maps to; `None` is an unbound build, in the legacy partition.
fn cache(db: &crate::db::SharedConnection, environment: Option<EnvironmentReference>) {
    use pplx_space_execution_spec::{
        ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion,
        ExecutionMetadata,
    };
    let execution = ExecutionMetadata::initial_firecracker(
        pplx_space_cpu_arch::Arch::X86_64,
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: None,
            version: Some(ArtifactVersion::parse("6.1.155").unwrap()),
            digest: Some(Digest::sha256([4; 32])),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    );
    let execution = match environment {
        Some(environment) => execution.with_environment(environment),
        None => execution,
    };
    let mut manifest = pplx_space_template_manifest::TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    manifest.set_execution_metadata(execution);
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    crate::db::upsert_build(
        &db.lock().unwrap(),
        &crate::db::BuildUpsert {
            name: NAME,
            content_hash: HASH,
            cache_partition: pplx_space_node_runtime_core::catalog::template_cache_partition(
                environment,
            ),
            build_date: "2026-11-01T00:00:00Z",
            manifest_json: &manifest_json,
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
}

#[tokio::test]
async fn a_twin_is_pulled_once_per_tick_into_its_own_partition() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let twin = environment(1);
    let mut queued = HashSet::new();

    // The original's build in the legacy partition doesn't warm the twin.
    cache(&db, None);
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(twin),
            twin,
            partition(twin)
        )
        .await,
        Outcome::Dispatched
    );
    // The tag and latest-N passes resolving the same twin pull it once.
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(twin),
            twin,
            partition(twin)
        )
        .await,
        Outcome::Coalesced
    );
    // Another environment's twin of the digest is its own pull.
    let other = environment(2);
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(other),
            other,
            partition(other)
        )
        .await,
        Outcome::Dispatched
    );

    let mut queued = HashSet::new();
    cache(&db, Some(twin));
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(twin),
            twin,
            partition(twin)
        )
        .await,
        Outcome::AlreadyCached
    );
}

/// In the legacy partition a release served bound to no environment is the
/// environment-less passes' to warm: one registered before releases
/// recorded their binding can land in another partition by its manifest,
/// so pulling it here would fetch and refuse it again every tick. A release
/// bound to the environment is still pulled there.
#[tokio::test]
async fn an_unbound_release_in_the_legacy_partition_is_left_to_the_environment_less_passes() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let mut queued = HashSet::new();
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            None,
            initial,
            TemplateCachePartition::Legacy
        )
        .await,
        Outcome::Unbound
    );
    assert!(queued.is_empty(), "nothing queued");
    assert_eq!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(initial),
            initial,
            TemplateCachePartition::Legacy
        )
        .await,
        Outcome::Dispatched
    );
}

/// The cache lookup waits for the catalog lock off the async worker: on a
/// single-threaded runtime, another task still runs while a pull holds that
/// lock, and it's what lets the holder go.
#[tokio::test(flavor = "current_thread")]
async fn the_cache_lookup_waits_for_the_catalog_lock_off_the_async_worker() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let twin = environment(1);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn({
        let db = db.clone();
        move || {
            let _guard = db.lock().unwrap();
            locked_tx.send(()).unwrap();
            // A lookup blocking the only worker keeps the releasing task from
            // running, so this gives up instead of hanging the test.
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok()
        }
    });
    locked_rx.recv().unwrap();

    let mut queued = HashSet::new();
    let (outcome, ()) = tokio::join!(
        decide(
            &db,
            &mut queued,
            NAME,
            HASH,
            Some(twin),
            twin,
            partition(twin)
        ),
        async move {
            // Runs only once the lookup has had its first poll.
            tokio::task::yield_now().await;
            release_tx.send(()).unwrap();
        },
    );
    assert_eq!(outcome, Outcome::Dispatched);
    assert!(
        holder.join().unwrap(),
        "the lookup blocked the async worker while the catalog lock was held"
    );
}

#[test]
fn tag_pins_survive_in_the_legacy_partition_and_each_runnable_one() {
    let (twin, other) = (environment(1), environment(2));
    assert_eq!(
        tag_pin_partitions(&BTreeSet::new()),
        vec![TemplateCachePartition::Legacy]
    );
    let kept = tag_pin_partitions(&BTreeSet::from([twin, other]));
    assert_eq!(kept.len(), 3);
    for expected in [
        TemplateCachePartition::Legacy,
        partition(twin),
        partition(other),
    ] {
        assert!(kept.contains(&expected), "{expected:?}");
    }
    // The initial environment's builds are the legacy partition's.
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    assert_eq!(
        tag_pin_partitions(&BTreeSet::from([initial])),
        vec![TemplateCachePartition::Legacy]
    );
}

#[test]
fn an_org_specific_verdict_reached_by_the_drain_suppresses_queued_twin_pulls() {
    let pull = |template: &str| EnvironmentPull {
        template: template.to_string(),
        content_hash: HASH.to_string(),
        environment: environment(1),
        pass: Pass::Tag,
    };
    let dispatchable = dispatchable_environment_pulls(
        vec![pull("seed"), pull("private")],
        &BTreeSet::from(["private".to_string()]),
    );
    assert_eq!(dispatchable, vec![pull("seed")]);
    assert_eq!(dispatchable[0].trigger(), PullTrigger::PrefetchTag);
}

/// With an older, refusing or unreachable node-manager the node's
/// environments are unknown: the environment pass doesn't run and no
/// environment's tag pin is pruned, so prefetch is today's legacy-only pass.
#[test]
fn unknown_node_environments_warm_nothing_more_and_prune_nothing() {
    assert_eq!(environments_to_warm(None), None);
    assert_eq!(
        tag_pin_partitions_to_keep(EnvironmentAwareness::On, None, true),
        None
    );
    // Past the pin grace, unknown environments keep only the legacy
    // partition's tag pins: nothing rewrites the others.
    assert_eq!(
        tag_pin_partitions_to_keep(EnvironmentAwareness::On, None, false),
        Some(vec![TemplateCachePartition::Legacy])
    );

    // A node known to run none warms nothing more, but its environment pins
    // are stale.
    let none = NodeEnvironments::default();
    assert_eq!(environments_to_warm(Some(&none)), None);
    assert_eq!(
        tag_pin_partitions_to_keep(EnvironmentAwareness::On, Some(&none), true),
        Some(vec![TemplateCachePartition::Legacy])
    );

    let twin = environment(1);
    let runs_twin = NodeEnvironments {
        runnable: BTreeSet::from([twin]),
    };
    assert_eq!(
        environments_to_warm(Some(&runs_twin)),
        Some(&BTreeSet::from([twin]))
    );
}

/// With environment awareness off, a tick reads no environments and runs no
/// environment pass, and keeps only the legacy partition's tag pins, which a
/// database that never had it on holds alone; on, it reads them whenever it
/// has tags to resolve.
#[test]
fn environment_awareness_off_reads_nothing_and_keeps_only_legacy_tag_pins() {
    let tags = super::super::parse_prefetch_tags(r#"["seed:latest"]"#).unwrap();
    assert!(!reads_environments(EnvironmentAwareness::Off, &tags));
    assert!(reads_environments(EnvironmentAwareness::On, &tags));
    assert!(!reads_environments(EnvironmentAwareness::On, &[]));

    let runs_twin = NodeEnvironments {
        runnable: BTreeSet::from([environment(1)]),
    };
    for node in [None, Some(&runs_twin)] {
        assert_eq!(
            tag_pin_partitions_to_keep(EnvironmentAwareness::Off, node, true),
            Some(vec![TemplateCachePartition::Legacy])
        );
    }
}

/// Off, a tick never reads the node's environments; on, it reads them, and a
/// failed read keeps what the last one returned.
#[tokio::test]
async fn a_tick_reads_environments_only_when_on_and_keeps_the_last_read() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tags = super::super::parse_prefetch_tags(r#"["seed:latest"]"#).unwrap();
    let node = NodeEnvironments {
        runnable: BTreeSet::from([environment(1)]),
    };
    let reads = AtomicUsize::new(0);
    let mut last = None;

    let off = tick_environments(EnvironmentAwareness::Off, &tags, &mut last, async || {
        reads.fetch_add(1, Ordering::SeqCst);
        Some(node.clone())
    })
    .await;
    assert_eq!(
        (off, reads.load(Ordering::SeqCst)),
        (None, 0),
        "off reads nothing"
    );

    let on = tick_environments(EnvironmentAwareness::On, &tags, &mut last, async || {
        reads.fetch_add(1, Ordering::SeqCst);
        Some(node.clone())
    })
    .await;
    assert_eq!(on, Some(node.clone()));
    let failed = tick_environments(EnvironmentAwareness::On, &tags, &mut last, async || None).await;
    assert_eq!(failed, Some(node), "a failed read keeps the last one");
}

/// A failed read keeps the last one for the pin grace period only: after
/// that the node's environments are unknown again, as before the first read.
#[tokio::test]
async fn a_failed_read_keeps_the_last_one_only_within_the_grace_period() {
    let tags = super::super::parse_prefetch_tags(r#"["seed:latest"]"#).unwrap();
    let node = NodeEnvironments {
        runnable: BTreeSet::from([environment(1)]),
    };
    let read_at = |age| std::time::Instant::now().checked_sub(age).unwrap();
    let mut last = Some(LastEnvironments {
        environments: node.clone(),
        read_at: read_at(std::time::Duration::from_secs(1)),
    });
    let kept = tick_environments(EnvironmentAwareness::On, &tags, &mut last, async || None).await;
    assert_eq!(kept, Some(node.clone()));

    let mut last = Some(LastEnvironments {
        environments: node,
        read_at: read_at(super::super::ENVIRONMENT_PIN_GRACE + std::time::Duration::from_secs(1)),
    });
    let expired =
        tick_environments(EnvironmentAwareness::On, &tags, &mut last, async || None).await;
    assert_eq!(expired, None);
}

/// A legacy-partition twin pull waits for a pin covering its digest, which
/// the environment-less pass writes; an unreadable pin table covers nothing.
#[test]
fn a_legacy_partition_twin_pull_waits_for_a_pin_on_its_digest() {
    let pinned = vec![HASH.to_string()];
    assert!(digest_is_pinned(Ok(&pinned), HASH));
    assert!(!digest_is_pinned(Ok(&[]), HASH));
    let error = "locked".to_string();
    assert!(!digest_is_pinned(Err(&error), HASH));
}

/// Only a generic template with no releases bound to the environment has
/// nothing bound: unknown visibility keeps its pins, and an org-specific
/// verdict is handled as one.
#[test]
fn only_a_generic_empty_answer_has_nothing_bound() {
    let none: [(); 0] = [];
    assert!(nothing_bound(&none, TemplateVisibility::Generic));
    assert!(!nothing_bound(&none, TemplateVisibility::Unspecified));
    assert!(!nothing_bound(&none, TemplateVisibility::OrgSpecific));
    assert!(!nothing_bound(&[()], TemplateVisibility::Generic));
}

/// The environment pass leaves the legacy partition's tag pin, which the
/// environment-less pass owns, alone, the initial environment's included.
#[test]
fn the_environment_pass_owns_only_environment_partitions_tag_pins() {
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    assert!(!owns_tag_pin(partition(initial)));
    assert!(!owns_tag_pin(TemplateCachePartition::Legacy));
    assert!(owns_tag_pin(partition(environment(1))));
}

const CHILD_ENDPOINT: &str = "SPACE_TEST_ENVIRONMENT_PASS_CATALOG_ENDPOINT";

/// With latest-N warming off, the pass still reads each template's
/// visibility from its environment's latest-releases answer: a template that
/// turned org-specific after its configured tag went away gets its verdict,
/// so the tick drops its pins, and the pass warms and pins nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_n_zero_still_reads_the_environment_s_visibility_verdict()
-> pplx_space_test_env::TestResult {
    use std::ffi::OsStr;

    use crate::volume::tests::catalog::{Catalog, TEMPLATE, TOKEN};
    if std::env::var_os(CHILD_ENDPOINT).is_some() {
        assert!(pplx_space_test_env::enter_test_process(&[]));
        let db = crate::db::SharedConnection::new(crate::db::test_conn());
        let pins = PinnedDigestsStore::new(db.clone());
        let tag_tuples =
            crate::prefetch::parse_prefetch_tags(&format!(r#"["{TEMPLATE}:retired"]"#)).unwrap();
        let templates = vec![TEMPLATE.to_string()];
        let environments = BTreeSet::from([
            pplx_space_node_runtime_core::catalog::initial_environment_reference(),
        ]);
        let (mut org_specific_tag_tuples, mut org_specific_templates) =
            (HashSet::new(), BTreeSet::new());
        let result = run(EnvironmentPass {
            pins: &pins,
            db: &db,
            tag_tuples: &tag_tuples,
            templates: &templates,
            latest_n: 0,
            environments: &environments,
            org_specific_tag_tuples: &mut org_specific_tag_tuples,
            org_specific_templates: &mut org_specific_templates,
            pinned_releases: &HashSet::new(),
        })
        .await;
        assert_eq!(
            org_specific_templates,
            BTreeSet::from([TEMPLATE.to_string()])
        );
        assert!(result.pulls.is_empty());
        assert!(result.release_pins_keep.is_empty());
        return Ok(());
    }

    let catalog = Catalog::start(TemplateVisibility::OrgSpecific).await?;
    assert!(!pplx_space_test_env::enter_test_process(&[
        (CHILD_ENDPOINT, Some(OsStr::new(&catalog.endpoint))),
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(OsStr::new(&catalog.endpoint))
        ),
        (
            "SPACE_CLUSTER_API_TOKEN_PATH",
            Some(catalog.token_file.path().as_os_str())
        ),
        (
            "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
            Some(OsStr::new("off"))
        ),
        ("SPACE_INTERNAL_TOKEN", Some(OsStr::new(TOKEN))),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
    ]));
    Ok(())
}

const OVERLAP_CHILD: &str = "SPACE_TEST_ENVIRONMENT_PASS_OVERLAP";

/// One environment's lookups overlap rather than wait in turn: the catalog
/// holds each request until both tag lookups have arrived, which run in turn
/// they never do together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_environment_s_lookups_overlap_rather_than_wait_in_turn()
-> pplx_space_test_env::TestResult {
    use std::ffi::OsStr;

    use crate::volume::tests::catalog::{Catalog, TEMPLATE, TOKEN};
    if std::env::var_os(OVERLAP_CHILD).is_some() {
        assert!(pplx_space_test_env::enter_test_process(&[]));
        let db = crate::db::SharedConnection::new(crate::db::test_conn());
        let pins = PinnedDigestsStore::new(db.clone());
        let tag_tuples = crate::prefetch::parse_prefetch_tags(&format!(
            r#"["{TEMPLATE}:latest", "{TEMPLATE}:retired"]"#
        ))
        .unwrap();
        let environments = BTreeSet::from([
            pplx_space_node_runtime_core::catalog::initial_environment_reference(),
        ]);
        let (mut org_specific_tag_tuples, mut org_specific_templates) =
            (HashSet::new(), BTreeSet::new());
        run(EnvironmentPass {
            pins: &pins,
            db: &db,
            tag_tuples: &tag_tuples,
            templates: &[],
            latest_n: 0,
            environments: &environments,
            org_specific_tag_tuples: &mut org_specific_tag_tuples,
            org_specific_templates: &mut org_specific_templates,
            pinned_releases: &HashSet::new(),
        })
        .await;
        return Ok(());
    }

    let catalog = Catalog::start_with_rendezvous(TemplateVisibility::Generic, 2).await?;
    assert!(!pplx_space_test_env::enter_test_process(&[
        (OVERLAP_CHILD, Some(OsStr::new("1"))),
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(OsStr::new(&catalog.endpoint))
        ),
        (
            "SPACE_CLUSTER_API_TOKEN_PATH",
            Some(catalog.token_file.path().as_os_str())
        ),
        (
            "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
            Some(OsStr::new("off"))
        ),
        ("SPACE_INTERNAL_TOKEN", Some(OsStr::new(TOKEN))),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
    ]));
    assert!(catalog.rendezvous.arrived() >= 2);
    assert!(
        !catalog.rendezvous.waited_out(),
        "a lookup waited alone for the other"
    );
    Ok(())
}

const ANSWERED_CHILD: &str = "SPACE_TEST_ENVIRONMENT_PASS_ANSWERED";

/// A tag or latest-releases lookup the cluster answers with the selector
/// applied restarts that lookup's own pin grace, whether or not a release is
/// bound there: the tick ages each ignored lookup from its own last answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_applied_lookup_is_recorded_as_answered() -> pplx_space_test_env::TestResult {
    use std::ffi::OsStr;

    use crate::volume::tests::catalog::{Catalog, TEMPLATE, TOKEN};
    if std::env::var_os(ANSWERED_CHILD).is_some() {
        assert!(pplx_space_test_env::enter_test_process(&[]));
        let db = crate::db::SharedConnection::new(crate::db::test_conn());
        let pins = PinnedDigestsStore::new(db.clone());
        let tag_tuples =
            crate::prefetch::parse_prefetch_tags(&format!(r#"["{TEMPLATE}:latest"]"#)).unwrap();
        let environments = BTreeSet::from([environment(2)]);
        let (mut org_specific_tag_tuples, mut org_specific_templates) =
            (HashSet::new(), BTreeSet::new());
        let result = run(EnvironmentPass {
            pins: &pins,
            db: &db,
            tag_tuples: &tag_tuples,
            templates: &[TEMPLATE.to_string()],
            latest_n: 0,
            environments: &environments,
            org_specific_tag_tuples: &mut org_specific_tag_tuples,
            org_specific_templates: &mut org_specific_templates,
            pinned_releases: &HashSet::new(),
        })
        .await;
        // A generic fn, not a closure: closures can't be generic, and this
        // reads both kinds of key.
        fn keys<K: Clone>(answered: &[(K, std::time::Instant)]) -> Vec<K> {
            answered.iter().map(|(key, _)| key.clone()).collect()
        }
        assert_eq!(
            keys(&result.answered_tag_lookups),
            vec![(
                TEMPLATE.to_string(),
                "latest".to_string(),
                partition(environment(2))
            )]
        );
        assert!(result.ignored_tag_lookups.is_empty());
        assert_eq!(
            keys(&result.answered_release_lookups),
            vec![(TEMPLATE.to_string(), environment(2))]
        );
        assert!(result.ignored_release_lookups.is_empty());
        return Ok(());
    }

    let catalog = Catalog::start(TemplateVisibility::Generic).await?;
    assert!(!pplx_space_test_env::enter_test_process(&[
        (ANSWERED_CHILD, Some(OsStr::new("1"))),
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(OsStr::new(&catalog.endpoint))
        ),
        (
            "SPACE_CLUSTER_API_TOKEN_PATH",
            Some(catalog.token_file.path().as_os_str())
        ),
        (
            "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
            Some(OsStr::new("off"))
        ),
        ("SPACE_INTERNAL_TOKEN", Some(OsStr::new(TOKEN))),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
    ]));
    Ok(())
}

const IGNORED_CHILD: &str = "SPACE_TEST_ENVIRONMENT_PASS_SELECTOR_IGNORED";

/// A cluster that predates twins ignores the environment selector. Its
/// answer still carries the template's visibility verdict, which doesn't
/// depend on the selector: an org-specific one marks the template for the
/// tick's pin drop, from a tag lookup or a latest-releases one, and records
/// the lookup neither answered nor ignored. A generic one leaves the lookup
/// ignored, so its pins age out from its own last answer. Neither warms
/// anything. Each case runs one kind of lookup: an org-specific verdict
/// from the tag pass would skip the template's latest-releases lookup.
#[rstest::rstest]
#[case::tag_org_specific(true, TemplateVisibility::OrgSpecific)]
#[case::tag_generic(true, TemplateVisibility::Generic)]
#[case::release_org_specific(false, TemplateVisibility::OrgSpecific)]
#[case::release_generic(false, TemplateVisibility::Generic)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_environment_lookup_whose_selector_is_ignored_keeps_only_its_verdict(
    #[case] tag_lookup: bool,
    #[case] visibility: TemplateVisibility,
) -> pplx_space_test_env::TestResult {
    use std::ffi::OsStr;

    use crate::volume::tests::catalog::{Catalog, TEMPLATE, TOKEN};
    if std::env::var_os(IGNORED_CHILD).is_some() {
        assert!(pplx_space_test_env::enter_test_process(&[]));
        let db = crate::db::SharedConnection::new(crate::db::test_conn());
        let pins = PinnedDigestsStore::new(db.clone());
        let tag_tuples = if tag_lookup {
            crate::prefetch::parse_prefetch_tags(&format!(r#"["{TEMPLATE}:latest"]"#)).unwrap()
        } else {
            Vec::new()
        };
        let templates = if tag_lookup {
            Vec::new()
        } else {
            vec![TEMPLATE.to_string()]
        };
        let environments = BTreeSet::from([environment(2)]);
        let (mut org_specific_tag_tuples, mut org_specific_templates) =
            (HashSet::new(), BTreeSet::new());
        let result = run(EnvironmentPass {
            pins: &pins,
            db: &db,
            tag_tuples: &tag_tuples,
            templates: &templates,
            latest_n: 1,
            environments: &environments,
            org_specific_tag_tuples: &mut org_specific_tag_tuples,
            org_specific_templates: &mut org_specific_templates,
            pinned_releases: &HashSet::new(),
        })
        .await;
        let org_specific = visibility == TemplateVisibility::OrgSpecific;
        assert_eq!(
            org_specific_templates,
            if org_specific {
                BTreeSet::from([TEMPLATE.to_string()])
            } else {
                BTreeSet::new()
            }
        );
        assert!(result.pulls.is_empty());
        assert!(result.release_pins_keep.is_empty());
        assert!(result.answered_tag_lookups.is_empty());
        assert!(result.answered_release_lookups.is_empty());
        let ignored = !org_specific;
        assert_eq!(
            result.ignored_tag_lookups,
            if ignored && tag_lookup {
                vec![(
                    TEMPLATE.to_string(),
                    "latest".to_string(),
                    partition(environment(2)),
                )]
            } else {
                Vec::new()
            }
        );
        assert_eq!(
            result.ignored_release_lookups,
            if ignored && !tag_lookup {
                vec![(TEMPLATE.to_string(), environment(2))]
            } else {
                Vec::new()
            }
        );
        return Ok(());
    }

    let catalog = Catalog::start_ignoring_selector(visibility).await?;
    assert!(!pplx_space_test_env::enter_test_process(&[
        (IGNORED_CHILD, Some(OsStr::new("1"))),
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(OsStr::new(&catalog.endpoint))
        ),
        (
            "SPACE_CLUSTER_API_TOKEN_PATH",
            Some(catalog.token_file.path().as_os_str())
        ),
        (
            "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
            Some(OsStr::new("off"))
        ),
        ("SPACE_INTERNAL_TOKEN", Some(OsStr::new(TOKEN))),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
    ]));
    // The lookup reached the catalog, so its answer is the one the pass read.
    assert_eq!(catalog.rendezvous.arrived(), 1);
    Ok(())
}
