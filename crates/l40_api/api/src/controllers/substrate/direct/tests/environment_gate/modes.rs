//! Each statement in every mode, against every kind of seeded node.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn create_candidates_by_mode() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let nodes = seed_nodes(&fixture.pool, "9.0.0").await?;
    inventory_templates(
        &fixture.pool,
        &nodes.ready().into_iter().chain([nodes.degraded]).collect(),
        "9.0.0",
    )
    .await?;

    for (template, environment) in TEMPLATES {
        let (off, off_counted) = creates(&fixture, Off, template).await?;
        let (shadow, shadow_counted) = creates(&fixture, Shadow, template).await?;
        let (on, on_counted) = creates(&fixture, On, template).await?;
        let mut expected_counts = Refusals::new();
        for (path, candidates) in &off {
            assert_eq!(*candidates, nodes.ready(), "{path} {template}, off");
            assert_eq!(
                shadow[path], *candidates,
                "{path} {template}: shadow places as off"
            );
            assert_eq!(
                on[path],
                nodes.admitted(environment),
                "{path} {template}, on"
            );
            expected_counts.extend(nodes.refusals(path, candidates, environment));
        }
        assert_eq!(
            off_counted,
            Refusals::new(),
            "{template}: off counts nothing"
        );
        assert_eq!(on_counted, Refusals::new(), "{template}: on counts nothing");
        // What on leaves out is exactly what shadow counts here because every
        // seeded node passes the legacy checks the environment match replaces,
        // so on adds no candidate off lacks.
        assert_eq!(
            shadow_counted, expected_counts,
            "{template}: shadow counts what on refuses"
        );
        // The count is exactly the candidates on leaves out.
        for (path, candidates) in &off {
            let refused = candidates.difference(&on[path]).count() as u64;
            let counted: u64 = shadow_counted
                .iter()
                .filter(|((counted_path, _), _)| counted_path == path)
                .map(|(_, count)| count)
                .sum();
            assert_eq!(counted, refused, "{path} {template}");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_last_known_fallback_by_mode() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // No node is cached-ready: a stale one listing both environments, one
    // that never reported, and a degraded one with evidence for both.
    let both = [E1, E0];
    let stale = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed {
            stale_in_secs: -1.0,
            ..SnapshotSeed::default()
        },
        &both,
        "9.0.0",
    )
    .await?;
    let unreported = NodeId::new();
    insert_ready_worker(&fixture.pool, unreported, "127.0.0.1:2", "9.0.0").await?;
    sqlx::query(sql!("DELETE FROM node_status_snapshots WHERE node_id = $1"))
        .bind(unreported)
        .execute(&fixture.pool)
        .await?;
    let degraded = seed_node(
        &fixture.pool,
        "127.0.0.1:3",
        SnapshotSeed::default(),
        &both,
        "9.0.0",
    )
    .await?;
    degrade(&fixture.pool, degraded).await?;
    let all = BTreeSet::from([stale, unreported, degraded]);
    inventory_templates(&fixture.pool, &all, "9.0.0").await?;

    for (template, environment) in TEMPLATES {
        let off = cached_create(&gated(&fixture, Off), template, CreateBootMode::Cold).await?;
        let shadow_substrate = gated(&fixture, Shadow);
        let shadow = cached_create(&shadow_substrate, template, CreateBootMode::Cold).await?;
        let shadow_counted = counted(&shadow_substrate);
        let on = cached_create(&gated(&fixture, On), template, CreateBootMode::Cold).await?;
        assert_eq!(off, all, "{template}, off");
        assert_eq!(shadow, all, "{template}: shadow places as off");
        let (expected_on, expected_counts) = if environment.is_some() {
            (
                BTreeSet::from([degraded]),
                Refusals::from([
                    (("last_known_create".to_string(), "stale".to_string()), 1),
                    (
                        ("last_known_create".to_string(), "no_evidence".to_string()),
                        1,
                    ),
                ]),
            )
        } else {
            (all.clone(), Refusals::new())
        };
        assert_eq!(on, expected_on, "{template}, on");
        assert_eq!(shadow_counted, expected_counts, "{template}, shadow");
    }

    // Base snapshots are format 12 here; the nodes load format 9.
    sqlx::query(sql!(
        "UPDATE node_template_inventory SET state_format_version = '12.0.0'"
    ))
    .execute(&fixture.pool)
    .await?;
    for (template, environment) in TEMPLATES {
        assert_eq!(
            cached_create(
                &gated(&fixture, Off),
                template,
                CreateBootMode::BaseSnapshot
            )
            .await?,
            BTreeSet::new(),
            "{template}: off keeps the major-version check"
        );
        assert_eq!(
            cached_create(&gated(&fixture, On), template, CreateBootMode::BaseSnapshot).await?,
            if environment.is_some() {
                BTreeSet::from([degraded])
            } else {
                BTreeSet::new()
            },
            "{template}: on, evidence listing the environment stands in for the format check"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_candidates_by_mode() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let nodes = seed_nodes(&fixture.pool, "9.0.0").await?;

    // Restoring from each ready node in turn checks the source statement
    // against every kind of node, and the remote one against the rest.
    for source in nodes.ready() {
        let others: BTreeSet<NodeId> = nodes.ready().into_iter().filter(|n| *n != source).collect();
        for (_, environment) in TEMPLATES {
            let sandbox_id = suspended_sandbox(&fixture.pool, source, environment, "9.0.0").await?;
            let why = format!("{environment:?} from the node at {source}");
            let (off, off_counted) = restore(&fixture, Off, sandbox_id).await?;
            let (shadow, shadow_counted) = restore(&fixture, Shadow, sandbox_id).await?;
            let (on, on_counted) = restore(&fixture, On, sandbox_id).await?;
            assert_eq!(off, (Some(source), others.clone()), "{why}, off");
            assert_eq!(shadow, off, "{why}: shadow places as off");
            let admitted = nodes.admitted(environment);
            assert_eq!(
                on,
                (
                    admitted.contains(&source).then_some(source),
                    others.intersection(&admitted).copied().collect()
                ),
                "{why}, on"
            );
            assert_eq!(off_counted, Refusals::new(), "{why}: off counts nothing");
            assert_eq!(on_counted, Refusals::new(), "{why}: on counts nothing");
            let mut expected = nodes.refusals("restore_source", &[source].into(), environment);
            expected.extend(nodes.refusals("restore_remote", &others, environment));
            assert_eq!(
                shadow_counted, expected,
                "{why}: shadow counts what on refuses"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shadow_counts_restores_held_back_for_want_of_cold_boot_evidence() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let nodes = seed_nodes(&fixture.pool, "9.0.0").await?;
    // An E1 checkpoint held on a node whose evidence lists only E0: on holds
    // its source restore back, though the node might load it.
    let sandbox_id = suspended_sandbox(&fixture.pool, nodes.good_e0, Some(E1), "9.0.0").await?;
    let (_, counted) = restore(&fixture, Shadow, sandbox_id).await?;
    assert_eq!(
        counted.get(&(
            "restore_source".to_string(),
            "restore_not_cold_bootable".to_string()
        )),
        Some(&1)
    );
    assert!(
        !counted
            .keys()
            .any(|(path, label)| path.starts_with("restore_") && label == "environment_not_usable"),
        "restores aren't counted under the create label: {counted:?}"
    );
    Ok(())
}
