//! The legacy checks the environment match replaces under on, and the ones it keeps.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn on_replaces_the_format_check_for_bound_work() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // Every node loads format 9; the templates' base snapshots are format 12.
    let nodes = seed_nodes(&fixture.pool, "9.0.0").await?;
    inventory_templates(
        &fixture.pool,
        &nodes.ready().into_iter().chain([nodes.degraded]).collect(),
        "12.0.0",
    )
    .await?;

    for (template, environment) in TEMPLATES {
        for gate in [Off, Shadow] {
            assert_eq!(
                cached_create(
                    &gated(&fixture, gate),
                    template,
                    CreateBootMode::BaseSnapshot
                )
                .await?,
                BTreeSet::new(),
                "{template}: {gate} keeps the major-version check"
            );
        }
        let expected = if environment.is_some() {
            nodes.admitted(environment)
        } else {
            BTreeSet::new()
        };
        assert_eq!(
            cached_create(&gated(&fixture, On), template, CreateBootMode::BaseSnapshot).await?,
            expected,
            "{template}, on"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_restores_a_bound_checkpoint_without_the_format_check() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // Every node loads format 9; the checkpoint is format 12.
    let nodes = seed_nodes(&fixture.pool, "9.0.0").await?;

    for (source, environment, expected_on) in [
        (
            nodes.good_e1,
            Some(E1),
            (Some(nodes.good_e1), BTreeSet::new()),
        ),
        (
            nodes.old_build,
            Some(E1),
            (None, BTreeSet::from([nodes.good_e1])),
        ),
        (
            nodes.old_build,
            Some(E0),
            (None, BTreeSet::from([nodes.good_e0])),
        ),
        (nodes.good_e1, None, (None, BTreeSet::new())),
    ] {
        let sandbox_id = suspended_sandbox(&fixture.pool, source, environment, "12.0.0").await?;
        for gate in [Off, Shadow] {
            assert_eq!(
                restore(&fixture, gate, sandbox_id).await?.0,
                (None, BTreeSet::new()),
                "{environment:?}: {gate} keeps the major-version check"
            );
        }
        assert_eq!(
            restore(&fixture, On, sandbox_id).await?.0,
            expected_on,
            "{environment:?} from {source}, on"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_replaces_the_component_check_for_bound_work() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The node has evidence for E1, but its legacy component inventory lacks
    // the kernel every template's build records, as a managed environment's
    // kernel is.
    // A degraded node with the same evidence and inventory is one only the
    // last-known fallback returns, so each statement's bypass shows on its own.
    let node = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let degraded = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    degrade(&fixture.pool, degraded).await?;
    inventory_templates(&fixture.pool, &BTreeSet::from([node, degraded]), "9.0.0").await?;
    for holder in [node, degraded] {
        for (template, _) in TEMPLATES {
            sqlx::query(sql!(
                "INSERT INTO node_template_components
                     (node_id, template_name, content_hash, kind, role_key, version)
                 VALUES ($1, $2, $3, 'kernel', '', '6.18.44')"
            ))
            .bind(holder)
            .bind(template)
            .bind(HASH)
            .execute(&fixture.pool)
            .await?;
        }
    }

    for gate in [Off, Shadow] {
        for (template, _) in TEMPLATES {
            assert_eq!(
                cached_create(&gated(&fixture, gate), template, CreateBootMode::Cold).await?,
                BTreeSet::new(),
                "{template}: {gate} keeps the component check"
            );
            assert_eq!(
                pooled(&gated(&fixture, gate), template).await?,
                BTreeSet::new(),
                "{template} pool: {gate} keeps the component check"
            );
        }
    }
    let on = gated(&fixture, On);
    assert_eq!(
        cached_create(&on, BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::from([node]),
        "evidence listing E1 stands in for the component check"
    );
    assert_eq!(pooled(&on, BOUND_E1).await?, BTreeSet::from([node]));
    assert_eq!(
        cached_create(&on, UNBOUND, CreateBootMode::Cold).await?,
        BTreeSet::new(),
        "unbound work keeps the component check"
    );
    assert_eq!(pooled(&on, UNBOUND).await?, BTreeSet::new());

    // Without the ready node, only the last-known fallback can place it.
    sqlx::query(sql!(
        "DELETE FROM node_template_inventory WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        cached_create(&on, BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::from([degraded]),
        "the last-known fallback's environment match stands in for it too"
    );
    assert_eq!(
        cached_create(&gated(&fixture, Off), BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::new()
    );
    Ok(())
}

/// A part of the legacy runtime projection a node lacks.
#[derive(Clone, Copy, Debug)]
enum Missing {
    /// Runtime installations, none of them Firecracker.
    FcInstallation,
    /// A guest row for the other runtime the work asks for.
    Guest,
}

impl Missing {
    /// The runtime the work asks for.
    fn runtime(self) -> &'static str {
        match self {
            Self::FcInstallation => "fc",
            Self::Guest => "other",
        }
    }

    async fn apply(self, pool: &PgPool, node: NodeId) -> TestResult {
        // Only another runtime is installed; deleting Firecracker's row
        // cascades to its guest and state-format rows.
        sqlx::query(sql!(
            "INSERT INTO node_runtime_installations (node_id, runtime, runtime_version)
             VALUES ($1, 'other', '1.0.0')"
        ))
        .bind(node)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "DELETE FROM node_runtime_installations WHERE node_id = $1 AND runtime = 'fc'"
        ))
        .bind(node)
        .execute(pool)
        .await?;
        if let Self::Guest = self {
            sqlx::query(sql!(
                "UPDATE node_template_inventory SET runtime = 'other' WHERE node_id = $1"
            ))
            .bind(node)
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

async fn cached_for(
    substrate: &DirectSubstrate,
    template: &str,
    runtime: &str,
) -> TestResult<BTreeSet<NodeId>> {
    match substrate
        .cached_worker_create_candidates(template, Some(HASH), runtime, CreateBootMode::Cold)
        .await
    {
        Ok(nodes) => Ok(nodes.into_iter().map(|node| node.id).collect()),
        Err(SubstrateError::Unavailable { .. }) => Ok(BTreeSet::new()),
        Err(error) => Err(error.into()),
    }
}

async fn pooled_for(
    substrate: &DirectSubstrate,
    template: &str,
    runtime: &str,
) -> TestResult<BTreeSet<NodeId>> {
    Ok(substrate
        .pooled_worker_candidates(template, Some(HASH), runtime)
        .await?
        .into_iter()
        .map(|candidate| candidate.id)
        .collect())
}

async fn pull_for(
    substrate: &DirectSubstrate,
    template: &str,
    runtime: &str,
) -> TestResult<BTreeSet<NodeId>> {
    Ok(substrate
        .pull_on_create_worker_candidates(template, Some(HASH), runtime)
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect())
}

#[tokio::test(flavor = "current_thread")]
async fn on_replaces_the_runtime_and_guest_projections_for_bound_work() -> TestResult {
    for missing in [Missing::FcInstallation, Missing::Guest] {
        let Some(fixture) = postgres_fixture().await? else {
            return Ok(());
        };
        // Both nodes have evidence for E1 but lack part of the legacy runtime
        // projection; the degraded one only the last-known fallback returns.
        let node = seed_node(
            &fixture.pool,
            "127.0.0.1:1",
            SnapshotSeed::default(),
            &[E1],
            "9.0.0",
        )
        .await?;
        let degraded = seed_node(
            &fixture.pool,
            "127.0.0.1:2",
            SnapshotSeed::default(),
            &[E1],
            "9.0.0",
        )
        .await?;
        degrade(&fixture.pool, degraded).await?;
        inventory_templates(&fixture.pool, &BTreeSet::from([node, degraded]), "9.0.0").await?;
        for holder in [node, degraded] {
            missing.apply(&fixture.pool, holder).await?;
        }
        let runtime = missing.runtime();

        for gate in [Off, Shadow] {
            for (template, _) in TEMPLATES {
                assert_eq!(
                    cached_for(&gated(&fixture, gate), template, runtime).await?,
                    BTreeSet::new(),
                    "{missing:?} {template}: {gate} keeps the projection check"
                );
                assert_eq!(
                    pooled_for(&gated(&fixture, gate), template, runtime).await?,
                    BTreeSet::new(),
                    "{missing:?} {template} pool: {gate} keeps the projection check"
                );
                assert_eq!(
                    pull_for(&gated(&fixture, gate), template, runtime).await?,
                    BTreeSet::new(),
                    "{missing:?} {template} pull: {gate} keeps the projection check"
                );
            }
        }
        let on = gated(&fixture, On);
        assert_eq!(
            cached_for(&on, BOUND_E1, runtime).await?,
            BTreeSet::from([node]),
            "{missing:?}: evidence listing E1 stands in for the projection"
        );
        assert_eq!(
            pooled_for(&on, BOUND_E1, runtime).await?,
            BTreeSet::from([node]),
            "{missing:?} pool"
        );
        assert_eq!(
            cached_for(&on, UNBOUND, runtime).await?,
            BTreeSet::new(),
            "{missing:?}: unbound work keeps the projection check"
        );
        assert_eq!(
            pooled_for(&on, UNBOUND, runtime).await?,
            BTreeSet::new(),
            "{missing:?} unbound pool"
        );
        assert_eq!(
            pull_for(&on, BOUND_E1, runtime).await?,
            BTreeSet::from([node]),
            "{missing:?} pull"
        );
        assert_eq!(
            pull_for(&on, UNBOUND, runtime).await?,
            BTreeSet::new(),
            "{missing:?} unbound pull"
        );

        sqlx::query(sql!(
            "DELETE FROM node_template_inventory WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&fixture.pool)
        .await?;
        assert_eq!(
            cached_for(&on, BOUND_E1, runtime).await?,
            BTreeSet::from([degraded]),
            "{missing:?}: the last-known fallback's match stands in for it too"
        );
        assert_eq!(
            cached_for(&gated(&fixture, Off), BOUND_E1, runtime).await?,
            BTreeSet::new(),
            "{missing:?}: off keeps it in the fallback"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_keeps_the_check_for_a_missing_component_other_than_the_kernel() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // Evidence for E1, and a build that records a kernel and a rootfs the
    // nodes' legacy component inventory lacks. The environment match stands
    // in only for the kernel.
    let node = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let degraded = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    degrade(&fixture.pool, degraded).await?;
    inventory_templates(&fixture.pool, &BTreeSet::from([node, degraded]), "9.0.0").await?;
    for holder in [node, degraded] {
        for kind in ["kernel", "rootfs"] {
            sqlx::query(sql!(
                "INSERT INTO node_template_components
                     (node_id, template_name, content_hash, kind, role_key, version)
                 VALUES ($1, $2, $3, $4, '', '1.0.0')"
            ))
            .bind(holder)
            .bind(BOUND_E1)
            .bind(HASH)
            .bind(kind)
            .execute(&fixture.pool)
            .await?;
        }
    }

    for gate in EnvironmentPlacementGate::ALL {
        assert_eq!(
            cached_create(&gated(&fixture, gate), BOUND_E1, CreateBootMode::Cold).await?,
            BTreeSet::new(),
            "{gate}: a missing rootfs excludes the ready node and the fallback's"
        );
        assert_eq!(
            pooled(&gated(&fixture, gate), BOUND_E1).await?,
            BTreeSet::new(),
            "{gate}: pool"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_replaces_the_projections_for_a_holder_a_pull_reaches_by_its_own_binding() -> TestResult
{
    for missing in [Missing::FcInstallation, Missing::Guest] {
        let Some(fixture) = postgres_fixture().await? else {
            return Ok(());
        };
        // No release binding, and the holders disagree, so only each one's own
        // row binds the build. One holds it bound to E1, has evidence for E1
        // and lacks part of the legacy projection; the other holds it bound to
        // E0 with evidence for E0 and a Firecracker projection.
        let lacking = seed_node(
            &fixture.pool,
            "127.0.0.1:1",
            SnapshotSeed::default(),
            &[E1],
            "9.0.0",
        )
        .await?;
        let projected = seed_node(
            &fixture.pool,
            "127.0.0.1:2",
            SnapshotSeed::default(),
            &[E0],
            "9.0.0",
        )
        .await?;
        let template = "held-projection";
        inventory(&fixture.pool, lacking, template, Some(E1), "9.0.0").await?;
        inventory(&fixture.pool, projected, template, Some(E0), "9.0.0").await?;
        missing.apply(&fixture.pool, lacking).await?;
        let runtime = missing.runtime();
        assert_eq!(
            pull_for(&gated(&fixture, On), template, runtime).await?,
            BTreeSet::from([lacking, projected]),
            "{missing:?}: each holder's binding stands in for the projection"
        );
        let off = match missing {
            Missing::FcInstallation => BTreeSet::from([projected]),
            Missing::Guest => BTreeSet::new(),
        };
        assert_eq!(
            pull_for(&gated(&fixture, Off), template, runtime).await?,
            off,
            "{missing:?}: off keeps the projection check"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_pull_keeps_the_projection_check_for_a_holder_of_the_unbound_build() -> TestResult {
    for missing in [Missing::FcInstallation, Missing::Guest] {
        let Some(fixture) = postgres_fixture().await? else {
            return Ok(());
        };
        // No release binding. One holder holds the build bound to E1, so the
        // holders agree on E1; the other holds it unbound and lacks part of
        // the legacy projection. Both have evidence for E1.
        let bound = seed_node(
            &fixture.pool,
            "127.0.0.1:1",
            SnapshotSeed::default(),
            &[E1],
            "9.0.0",
        )
        .await?;
        let unbound = seed_node(
            &fixture.pool,
            "127.0.0.1:2",
            SnapshotSeed::default(),
            &[E1],
            "9.0.0",
        )
        .await?;
        let template = "held-unbound";
        inventory(&fixture.pool, bound, template, Some(E1), "9.0.0").await?;
        inventory(&fixture.pool, unbound, template, None, "9.0.0").await?;
        missing.apply(&fixture.pool, unbound).await?;
        let runtime = missing.runtime();
        let on = pull_for(&gated(&fixture, On), template, runtime).await?;
        assert!(
            !on.contains(&unbound),
            "{missing:?}: the unbound build keeps the projection check, whatever \
             the other holders agree on"
        );
    }
    Ok(())
}
