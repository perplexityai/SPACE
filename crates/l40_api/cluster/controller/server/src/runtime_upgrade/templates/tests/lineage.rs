//! Derived builds and their bases: base-first order, deep lineages, cycles,
//! and a base that can't move or goes away.

use super::*;

/// A derived template's twin is made after its base's, even when the derived
/// release is leased first: its item makes the base's twin, and the base's own
/// item then finds it in place and ends done.
#[tokio::test(flavor = "multi_thread")]
async fn a_derived_template_is_twinned_after_its_base() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("base").await?;
    fixture.template("derived").await?;
    // Registered first, so its id sorts first and the canary leases it.
    let base_hash = format!("sha256:{}", "0b".repeat(32));
    let (derived, derived_hash) = fixture
        .release("derived", 0x0d, Some(("base", &base_hash)))
        .await?;
    let (base, base_hash) = fixture.release("base", 0x0b, None).await?;
    assert!(derived < base);
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Canary, 1).await?;
    let reconciler = fixture.reconciler();
    for _ in 0..3 {
        fixture.tick(&reconciler).await;
    }

    let (_, base_created) = fixture.twin("base", &base_hash).await?.expect("base twin");
    let (_, derived_created) = fixture
        .twin("derived", &derived_hash)
        .await?
        .expect("derived twin");
    assert!(
        base_created < derived_created,
        "the base's twin comes first"
    );

    store::resume_campaign(
        &Db::from(fixture.pool().clone()),
        campaign,
        &PhaseChange::default(),
        false,
    )
    .await?;
    for _ in 0..3 {
        fixture.tick(&reconciler).await;
    }
    assert_eq!(fixture.counts(campaign).await?.done, 2);
    assert_eq!(fixture.phase(campaign).await?, "completed");
    let _ = base;
    Ok(())
}

/// Builds don't cap a lineage's depth, so a deep one moves too, even when its
/// deepest descendant is leased first: its item twins every base on the way.
#[tokio::test(flavor = "multi_thread")]
async fn a_deep_lineage_is_twinned_from_its_deepest_descendant() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    // Deeper than any real lineage: the walk is iterative, so depth costs
    // no stack.
    const DEPTH: u8 = 24;
    let name = |level: u8| format!("level-{level}");
    let hash = |level: u8| format!("sha256:{}", format!("{:02x}", level + 1).repeat(32));
    // The deepest first, so its item has the lowest id and is leased first.
    for level in (0..DEPTH).rev() {
        fixture.template(&name(level)).await?;
    }
    for level in (0..DEPTH).rev() {
        let base = level.checked_sub(1).map(|base| (name(base), hash(base)));
        let (_, made) = fixture
            .release(
                &name(level),
                level + 1,
                base.as_ref()
                    .map(|(name, hash)| (name.as_str(), hash.as_str())),
            )
            .await?;
        assert_eq!(made, hash(level));
    }
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    for _ in 0..(3 * usize::from(DEPTH)) {
        fixture.tick(&reconciler).await;
        if fixture.phase(campaign).await? == "completed" {
            break;
        }
    }
    assert_eq!(fixture.phase(campaign).await?, "completed");
    let mut created = Vec::new();
    for level in 0..DEPTH {
        let (_, at) = fixture
            .twin(&name(level), &hash(level))
            .await?
            .ok_or_else(|| format!("level {level} has no twin"))?;
        created.push(at);
    }
    // The deepest descendant's item made them all, root first: each base's
    // twin committed before its derived build's.
    for level in 1..usize::from(DEPTH) {
        assert!(
            created[level - 1] < created[level],
            "level {} was twinned before its base",
            level
        );
    }
    assert_eq!(fixture.counts(campaign).await?.failed, 0);
    Ok(())
}

/// A build whose `base_resolved` names its own release is a corrupt lineage,
/// like a longer cycle: its item fails on the first lease, with no twin.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_naming_itself_as_its_base_fails() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("selfie").await?;
    let own_hash = format!("sha256:{}", "11".repeat(32));
    let (release, hash) = fixture
        .release("selfie", 0x11, Some(("selfie", &own_hash)))
        .await?;
    assert_eq!(hash, own_hash);
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, attempts): (String, i64) = sqlx::query_as(
        "SELECT phase, attempts FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((phase.as_str(), attempts), ("failed", 1));
    assert_eq!(fixture.twin("selfie", &hash).await?, None);
    Ok(())
}

/// A base that can't be moved fails its derived build's item, which names the
/// base: no later lease moves the base either.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_that_cant_move_fails_its_derived_item() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("child").await?;
    fixture.template("base").await?;
    // The child first, so its item is leased first.
    let base_hash = format!("sha256:{}", "01".repeat(32));
    let (child, child_hash) = fixture
        .release("child", 2, Some(("base", &base_hash)))
        .await?;
    // The base records no execution metadata and is an aarch64 image, which
    // the target can't run.
    let (base, made) = fixture.release("base", 1, None).await?;
    assert_eq!(made, base_hash);
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(base)
        .execute(fixture.pool())
        .await?;
    let prefix = format!("space/templates/base/{base_hash}");
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/metadata.json"),
        serde_json::to_vec(&TemplateMetadata {
            id: "base".to_owned(),
            name: "base".to_owned(),
            base: "scratch".to_owned(),
            arch: Some("aarch64".to_owned()),
            ..TemplateMetadata::default()
        })?,
    );
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/manifest.json"),
        b"{\"arch\": \"\"}".to_vec(),
    );
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(child)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed");
    let error = error.unwrap_or_default();
    assert!(
        error.contains(&format!("the base base@{base_hash} can't be moved:"))
            && error.contains("aarch64"),
        "{error}"
    );
    assert_eq!(fixture.twin("child", &child_hash).await?, None);
    assert_eq!(fixture.twin("base", &base_hash).await?, None);
    Ok(())
}

/// A base whose template is marked for deletion between its twin's
/// preparation and its commit gets no twin, so its derived build mustn't get
/// one in that lease: the item is retried, to resolve the base again, rather
/// than committing the derived twin over a base with none.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_skipped_at_its_commit_holds_back_its_derived_twin() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("child").await?;
    fixture.template("base").await?;
    let base_hash = format!("sha256:{}", "01".repeat(32));
    let (child, child_hash) = fixture
        .release("child", 2, Some(("base", &base_hash)))
        .await?;
    fixture.release("base", 1, None).await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler_with(Arc::new(HookedObjects {
        inner: Arc::clone(&fixture.objects),
        pool: fixture.pool().clone(),
        template: "base",
        on_write: OnWrite::MarkDeleting,
    }));
    // One lease: the next resolves the base again, and finds it no longer
    // the campaign's to move.
    fixture.tick(&reconciler).await;

    let (phase, attempts, error): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase, attempts, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(child)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(fixture.twin("base", &base_hash).await?, None);
    assert_eq!(fixture.twin("child", &child_hash).await?, None);
    assert_eq!((phase.as_str(), attempts), ("pending", 1), "retried");
    let error = error.unwrap_or_default();
    assert!(
        error.contains(&format!("the base base@{base_hash}: the base was skipped")),
        "{error}"
    );
    Ok(())
}

/// A derived build that can't run under the target fails its item before its
/// base moves: the base's twin would take the base's creates as soon as it
/// committed, for a build that never gets one.
#[tokio::test(flavor = "multi_thread")]
async fn a_derived_build_the_target_cant_run_leaves_its_base_alone() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("child").await?;
    fixture.template("base").await?;
    let base_hash = format!("sha256:{}", "01".repeat(32));
    // The child first, so its item is leased first: an aarch64 image that
    // records no execution metadata, which the target can't run.
    let (child, child_hash) = fixture
        .release("child", 2, Some(("base", &base_hash)))
        .await?;
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(child)
        .execute(fixture.pool())
        .await?;
    let prefix = format!("space/templates/child/{child_hash}");
    let document = TemplateMetadata {
        id: "child".to_owned(),
        name: "child".to_owned(),
        base: "base".to_owned(),
        base_resolved: Some(format!("base@{base_hash}")),
        arch: Some("aarch64".to_owned()),
        ..TemplateMetadata::default()
    };
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/metadata.json"),
        serde_json::to_vec(&document)?,
    );
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/manifest.json"),
        b"{\"arch\": \"\"}".to_vec(),
    );
    let (_, made) = fixture.release("base", 1, None).await?;
    assert_eq!(made, base_hash);
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    // Until the child's item has its outcome. One item at a time, so the
    // base's own item hasn't been leased yet.
    let child_outcome = || async {
        sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT phase, last_error FROM runtime_upgrade_items
             WHERE campaign_id = $1 AND subject_id = $2",
        )
        .bind(campaign)
        .bind(child)
        .fetch_optional(fixture.pool())
        .await
    };
    for _ in 0..5 {
        fixture.tick(&reconciler).await;
        if child_outcome()
            .await?
            .is_some_and(|(phase, _)| phase != "pending" && phase != "in_flight")
        {
            break;
        }
    }

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(child)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("aarch64")),
        "{error:?}"
    );
    assert_eq!(fixture.twin("child", &child_hash).await?, None);
    assert_eq!(
        fixture.twin("base", &base_hash).await?,
        None,
        "the base's twin waits for the base's own item"
    );
    Ok(())
}

/// Two builds naming each other as their base are a corrupt lineage, not one
/// a later lease can order: each item fails on its first lease rather than
/// spend its attempts retrying.
#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_of_bases_fails_without_retrying() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("left").await?;
    fixture.template("right").await?;
    let right_hash = format!("sha256:{}", "0e".repeat(32));
    let left_hash = format!("sha256:{}", "0f".repeat(32));
    let (left, _) = fixture
        .release("left", 0x0f, Some(("right", &right_hash)))
        .await?;
    let (right, _) = fixture
        .release("right", 0x0e, Some(("left", &left_hash)))
        .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 2).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    for release in [left, right] {
        let (phase, attempts): (String, i64) = sqlx::query_as(
            "SELECT phase, attempts FROM runtime_upgrade_items
             WHERE campaign_id = $1 AND subject_id = $2",
        )
        .bind(campaign)
        .bind(release)
        .fetch_one(fixture.pool())
        .await?;
        assert_eq!((phase.as_str(), attempts), ("failed", 1));
    }
    assert_eq!(fixture.twin("left", &left_hash).await?, None);
    Ok(())
}
