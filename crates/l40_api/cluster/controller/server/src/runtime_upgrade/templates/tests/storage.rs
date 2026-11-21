//! A release's storage locations and documents: which location serves,
//! encryption, and the execution metadata a build records.

use super::*;

/// A release whose storage locations are encrypted under its owner's key is
/// still the campaign's to move but can't be yet: its item fails, counting
/// against the budget, rather than being skipped, so the campaign doesn't
/// complete as if it had moved it.
#[tokio::test(flavor = "multi_thread")]
async fn an_encrypted_release_fails_its_item() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    let org: Uuid = sqlx::query_scalar(
        "INSERT INTO organizations (name, slug) VALUES ('owner', 'owner') RETURNING id",
    )
    .fetch_one(fixture.pool())
    .await?;
    let master_key: Uuid = sqlx::query_scalar(
        "INSERT INTO org_master_keys (org_id, provider, key_identifier)
         VALUES ($1, 'in_memory', 'owner-key')
         RETURNING id",
    )
    .bind(org)
    .fetch_one(fixture.pool())
    .await?;
    sqlx::query("UPDATE templates SET org_id = $1 WHERE name = 'seed'")
        .bind(org)
        .execute(fixture.pool())
        .await?;
    sqlx::query("UPDATE template_releases SET org_id = $2 WHERE id = $1")
        .bind(release)
        .bind(org)
        .execute(fixture.pool())
        .await?;
    let location: Uuid = sqlx::query_scalar(
        "UPDATE template_release_storage_locations SET org_id = $2
         WHERE template_release_id = $1
         RETURNING id",
    )
    .bind(release)
    .bind(org)
    .fetch_one(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO template_release_storage_location_encrypted_fields (
             template_release_storage_location_id, org_id, master_key_id, wrapped_dek,
             storage_connection_string
         ) VALUES ($1, $2, $3, '\\x00', '\\x00')",
    )
    .bind(location)
    .bind(org)
    .bind(master_key)
    .execute(fixture.pool())
    .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items WHERE campaign_id = $1",
    )
    .bind(campaign)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("encrypted under its owner's key")),
        "{error:?}"
    );
    assert_eq!(fixture.twin("seed", &hash).await?, None);
    Ok(())
}

/// A release recording no execution metadata needs the build's
/// `manifest.json` too. When the location that served `metadata.json` lacks
/// it, a later location that has it serves it, as a pull would fall back.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_location_serves_the_manifest_the_first_lacks() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("legacy").await?;
    let (release, hash) = fixture.release("legacy", 3, None).await?;
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(release)
        .execute(fixture.pool())
        .await?;
    // Older than the fixture's location: it serves metadata.json, but no
    // manifest.json.
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, created_at)
         VALUES ($1, $2, now() - INTERVAL '1 day')",
    )
    .bind(release)
    .bind(format!(
        "s3://partial-bucket/space/templates/legacy/{hash}?region=us-east-1"
    ))
    .execute(fixture.pool())
    .await?;
    let prefix = format!("space/templates/legacy/{hash}");
    let document = serde_json::to_vec(&TemplateMetadata {
        id: "legacy".to_owned(),
        name: "legacy".to_owned(),
        base: "scratch".to_owned(),
        ..TemplateMetadata::default()
    })?;
    for bucket in ["partial-bucket", BUCKET] {
        fixture
            .objects
            .put(bucket, &format!("{prefix}/metadata.json"), document.clone());
    }
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

    assert_eq!(fixture.counts(campaign).await?.done, 1);
    // The location without manifest.json can't serve a pull, so the twin
    // doesn't get it.
    let (twin, _) = fixture.twin("legacy", &hash).await?.expect("twin");
    assert_eq!(
        releases::storage_locations(fixture.pool(), twin).await?,
        vec![format!(
            "s3://{BUCKET}/space/templates/legacy/{hash}?region=us-east-1"
        )]
    );
    Ok(())
}

/// With the only location that serves the build's manifest.json refusing the
/// twin's write, no location can serve the twin: a location that serves
/// only metadata.json isn't enough, so no twin commits and the item retries.
#[tokio::test(flavor = "multi_thread")]
async fn no_twin_commits_without_a_location_serving_the_manifest() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 4, None).await?;
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, $2)",
    )
    .bind(release)
    .bind(format!(
        "s3://partial-bucket/space/templates/seed/{hash}?region=us-east-1"
    ))
    .execute(fixture.pool())
    .await?;
    let prefix = format!("space/templates/seed/{hash}");
    let document = fixture
        .objects
        .get(BUCKET, &format!("{prefix}/metadata.json"))
        .expect("the fixture's metadata.json");
    fixture.objects.put(
        "partial-bucket",
        &format!("{prefix}/metadata.json"),
        document,
    );
    fixture.objects.refuse_writes_to(BUCKET);
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;

    assert_eq!(fixture.twin("seed", &hash).await?, None);
    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "pending", "retried, not failed: {error:?}");
    let error = error.unwrap_or_default();
    assert!(
        error.contains("doesn't serve the build's manifest.json"),
        "{error}"
    );
    Ok(())
}

/// A location that accepts the twin's `metadata.json` but no longer serves
/// the build's own isn't given to the twin: a create resolved to the twin
/// would pull from it and find no build.
#[tokio::test(flavor = "multi_thread")]
async fn a_location_that_lost_the_build_isnt_the_twins() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    let lost = format!("s3://lost-bucket/space/templates/seed/{hash}?region=us-east-1");
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, $2)",
    )
    .bind(release)
    .bind(&lost)
    .execute(fixture.pool())
    .await?;
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    assert_eq!(fixture.counts(campaign).await?.done, 1);
    let (twin, _) = fixture.twin("seed", &hash).await?.expect("twin");
    assert_eq!(
        releases::storage_locations(fixture.pool(), twin).await?,
        vec![format!(
            "s3://{BUCKET}/space/templates/seed/{hash}?region=us-east-1"
        )]
    );
    assert!(
        fixture
            .objects
            .get(
                "lost-bucket",
                &format!(
                    "space/templates/seed/{hash}/environments/{}/metadata.json",
                    fixture.e1
                ),
            )
            .is_none(),
        "nothing is written where the build is gone"
    );
    Ok(())
}

/// An unbound release counts as the initial environment's: a campaign from
/// E0 twins it, and one toward E0 finds it already there and adds no
/// initial-bound row beside it.
#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_release_is_the_initial_environments() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(release)
        .execute(fixture.pool())
        .await?;
    let driver = fixture.driver();
    let page = driver
        .find_subjects(
            &Campaign {
                id: CampaignId(Uuid::now_v7()),
                from: fixture.e0,
                to: fixture.e1,
                kind: CampaignKind::Templates,
                phase: CampaignPhase::Rolling,
                max_in_flight_per_node: 1,
                max_in_flight_per_cluster: 1,
                failure_budget: 1,
                include_running: false,
                generation: 0,
            },
            ScanPlan {
                moves: true,
                inputs: 0,
            },
            None,
            10,
        )
        .await?;
    assert_eq!(page.on_source, vec![release]);

    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;
    assert_eq!(fixture.counts(campaign).await?.done, 1);
    assert!(fixture.twin("seed", &hash).await?.is_some());

    let toward_e0 = Campaign {
        id: CampaignId(Uuid::now_v7()),
        from: fixture.e1,
        to: fixture.e0,
        kind: CampaignKind::Templates,
        phase: CampaignPhase::Rolling,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 1,
        failure_budget: 1,
        include_running: false,
        generation: 0,
    };
    let (twin, _) = fixture.twin("seed", &hash).await?.expect("twin");
    assert!(matches!(
        driver.move_release(&toward_e0, twin).await,
        Ok(Moved::Done)
    ));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM template_releases")
        .fetch_one(fixture.pool())
        .await?;
    assert_eq!(rows, 2, "no initial-bound row beside the unbound one");
    Ok(())
}

/// A release whose oldest storage location doesn't serve its metadata.json is
/// read from the next one, as a pull would. The twin's document is written
/// beside every copy that takes it, and the twin gets only those locations: a
/// pull from any other, such as a copy that serves the build but refuses the
/// write, would find the source's document.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_storage_location_serves_when_the_first_doesnt() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    // Older than the location that has the build, and empty.
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, created_at)
         VALUES ($1, $2, now() - INTERVAL '1 day')",
    )
    .bind(release)
    .bind(format!(
        "s3://empty-bucket/space/templates/seed/{hash}?region=us-east-1"
    ))
    .execute(fixture.pool())
    .await?;
    // Newer, serving the build, and refusing the twin's document.
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, created_at)
         VALUES ($1, $2, now() + INTERVAL '1 day')",
    )
    .bind(release)
    .bind(format!(
        "s3://read-only-bucket/space/templates/seed/{hash}?region=us-east-1"
    ))
    .execute(fixture.pool())
    .await?;
    // It serves both of the build's documents, so only its refused write
    // keeps it from the twin.
    for document in ["metadata.json", "manifest.json"] {
        let shared = format!("space/templates/seed/{hash}/{document}");
        fixture.objects.put(
            "read-only-bucket",
            &shared,
            fixture
                .objects
                .get(BUCKET, &shared)
                .expect("the build's document"),
        );
    }
    fixture.objects.refuse_writes_to("read-only-bucket");
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    assert_eq!(fixture.counts(campaign).await?.done, 1);
    let (twin, _) = fixture.twin("seed", &hash).await?.expect("twin");
    assert_eq!(
        releases::storage_locations(fixture.pool(), twin).await?,
        vec![format!(
            "s3://{BUCKET}/space/templates/seed/{hash}?region=us-east-1"
        )],
        "the replica that refused the twin's document isn't the twin's"
    );
    assert!(
        fixture
            .objects
            .get(
                BUCKET,
                &format!(
                    "space/templates/seed/{hash}/environments/{}/metadata.json",
                    fixture.e1
                ),
            )
            .is_some()
    );
    Ok(())
}

/// A release recording no execution metadata anywhere, in its row or its
/// metadata.json, is resolved as template-manager resolves its architecture.
/// One recording none boots as x86_64 under the initial environment and gets
/// its twin; an aarch64 image fails rather than get an x86_64 binding it
/// can't boot.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_recording_no_execution_is_twinned_only_as_x86_64() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let mut legacy = Vec::new();
    for (name, byte, arch) in [("plain", 1u8, None), ("arm", 2u8, Some("aarch64"))] {
        fixture.template(name).await?;
        let (release, hash) = fixture.release(name, byte, None).await?;
        sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
            .bind(release)
            .execute(fixture.pool())
            .await?;
        let prefix = format!("space/templates/{name}/{hash}");
        let document = TemplateMetadata {
            id: name.to_owned(),
            name: name.to_owned(),
            base: "scratch".to_owned(),
            arch: arch.map(str::to_owned),
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
        legacy.push((name, release, hash));
    }
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 2).await?;
    let reconciler = fixture.reconciler();
    for _ in 0..3 {
        fixture.tick(&reconciler).await;
    }

    let (_, plain, plain_hash) = &legacy[0];
    let (_, arm, arm_hash) = &legacy[1];
    assert!(fixture.twin("plain", plain_hash).await?.is_some());
    assert_eq!(fixture.twin("arm", arm_hash).await?, None);
    let pool = fixture.pool().clone();
    let outcome = |release: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT phase, last_error FROM runtime_upgrade_items
                 WHERE campaign_id = $1 AND subject_id = $2",
            )
            .bind(campaign)
            .bind(release)
            .fetch_one(&pool)
            .await
        }
    };
    assert_eq!(outcome(*plain).await?.0, "done");
    let (phase, error) = outcome(*arm).await?;
    assert_eq!(phase, "failed");
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("aarch64")),
        "{error:?}"
    );
    Ok(())
}

/// A build recording its execution metadata only in its `manifest.json` is
/// checked against that, as template-manager reads it, not against the
/// initial environment's: one whose manifest records an aarch64 guest fails
/// rather than be twinned as an x86_64 build.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_recording_execution_only_in_its_manifest_is_checked_against_it() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(release)
        .execute(fixture.pool())
        .await?;
    let prefix = format!("space/templates/seed/{hash}");
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/metadata.json"),
        serde_json::to_vec(&TemplateMetadata {
            id: "seed".to_owned(),
            name: "seed".to_owned(),
            base: "scratch".to_owned(),
            ..TemplateMetadata::default()
        })?,
    );
    let mut execution = serde_json::to_value(requirements(&fixture.catalog, fixture.e0))?;
    execution["guest"]["architecture"] = serde_json::json!("aarch64");
    let mut manifest = pplx_space_template_manifest::TemplateManifest::default();
    manifest.set_execution_metadata(serde_json::from_value(execution)?);
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/manifest.json"),
        serde_json::to_vec(&manifest)?,
    );
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;

    assert_eq!(fixture.twin("seed", &hash).await?, None);
    let (phase, error): (String, Option<String>) = sqlx::query_as(
        "SELECT phase, last_error FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2",
    )
    .bind(campaign)
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "failed", "{error:?}");
    // Refused by the cold-boot check against the manifest's execution, not
    // for some other reason a manifest-only build could fail.
    let error = error.unwrap_or_default();
    assert!(error.contains("doesn't cold boot under"), "{error}");
    Ok(())
}

/// An empty metadata.json is no metadata, as a node's pull reads it: the
/// build's execution metadata comes from its manifest, and the build is
/// twinned with a document carrying only the target's execution metadata.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_metadata_document_is_read_as_none() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    sqlx::query("UPDATE template_releases SET execution_metadata = NULL WHERE id = $1")
        .bind(release)
        .execute(fixture.pool())
        .await?;
    let prefix = format!("space/templates/seed/{hash}");
    fixture
        .objects
        .put(BUCKET, &format!("{prefix}/metadata.json"), Vec::new());
    let mut manifest = pplx_space_template_manifest::TemplateManifest::default();
    manifest.set_execution_metadata(requirements(&fixture.catalog, fixture.e0));
    fixture.objects.put(
        BUCKET,
        &format!("{prefix}/manifest.json"),
        serde_json::to_vec(&manifest)?,
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
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "done", "{error:?}");
    assert!(
        fixture.twin("seed", &hash).await?.is_some(),
        "the build gets its twin"
    );
    let document = fixture
        .objects
        .get(
            BUCKET,
            &format!("{prefix}/environments/{}/metadata.json", fixture.e1),
        )
        .expect("the twin's metadata.json");
    let document: TemplateMetadata = serde_json::from_slice(&document)?;
    assert_eq!(document.environment()?, Some(fixture.e1));
    Ok(())
}

/// A metadata.json whose schema version and execution metadata disagree, or
/// whose schema this build doesn't know, isn't rewritten for a twin.
#[test]
fn an_inconsistent_metadata_document_is_refused() {
    for document in [
        // Version 3 binds an environment, which this document lacks.
        serde_json::json!({"id": "seed", "name": "seed", "base": "scratch", "schema_version": 3}),
        serde_json::json!({"id": "seed", "name": "seed", "base": "scratch", "schema_version": 99}),
    ] {
        let parsed =
            super::metadata::TemplateDocument::parse(&serde_json::to_vec(&document).unwrap());
        assert!(matches!(parsed, Err(MoveError::Failed(_))), "{document}");
    }
}

/// A manifest is read as template-manager reads it: a legacy one, without a
/// `schema_version`, records no execution metadata; one whose
/// `schema_version` requires an `execution` block it lacks is refused rather
/// than taken as legacy.
#[test]
fn a_manifest_is_validated_before_its_execution_is_read() {
    assert!(matches!(
        metadata::manifest_execution(br#"{"arch": ""}"#),
        Ok(None)
    ));
    assert!(matches!(
        metadata::manifest_execution(br#"{"schema_version": 3, "arch": "x86_64"}"#),
        Err(MoveError::Failed(_))
    ));
    let mut manifest = pplx_space_template_manifest::TemplateManifest::default();
    manifest.set_execution_metadata(requirements(
        &environments().0,
        initial_environment_reference(),
    ));
    assert!(matches!(
        metadata::manifest_execution(&serde_json::to_vec(&manifest).expect("manifest")),
        Ok(Some(_))
    ));
}

/// A release recording the initial environment's runtime and guest but no
/// boot components, as builds did before components were recorded, is
/// twinned: its source stands for the initial environment's requirements, as
/// template-manager reads it, rather than failing the cold-boot check for the
/// components it never recorded.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_recorded_before_boot_components_is_twinned() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let (release, hash) = fixture.release("seed", 1, None).await?;
    let mut execution = serde_json::to_value(requirements(&fixture.catalog, fixture.e0))?;
    let object = execution
        .as_object_mut()
        .expect("execution metadata is an object");
    object.remove("environment");
    object.insert("components".to_owned(), serde_json::json!([]));
    sqlx::query("UPDATE template_releases SET execution_metadata = $2 WHERE id = $1")
        .bind(release)
        .bind(&execution)
        .execute(fixture.pool())
        .await?;
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
    .bind(release)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(phase, "done", "{error:?}");
    assert!(fixture.twin("seed", &hash).await?.is_some());
    Ok(())
}
