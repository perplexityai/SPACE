use pplx_space_accounts::{NodeId, OrgId, SystemPublicationGrantId, TenantPublicationGrantId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_template_model::{
    BuildRunId, TemplateDesiredState, TemplateId, TemplateName, TemplateTag, TemplateVisibility,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    base_snapshot_policies, build_jobs, pool_policies, publication_grants, release_tags, templates,
};

fn seed_org() -> OrgId {
    OrgId::from_uuid(SEED_ORG_ID)
}

fn name(value: &str) -> TemplateName {
    TemplateName::parse(value).expect("a valid template name")
}

async fn insert_org(pool: &PgPool) -> TestResult<OrgId> {
    let org = OrgId::new();
    sqlx::query("INSERT INTO organizations (id, name, slug) VALUES ($1, 'Other', $2)")
        .bind(org.as_uuid())
        .bind(format!("other-{org}"))
        .execute(pool)
        .await?;
    Ok(org)
}

async fn register(pool: &PgPool, template: &str, org_id: Option<OrgId>) -> TestResult {
    let visibility = if org_id.is_some() {
        TemplateVisibility::OrgSpecific
    } else {
        TemplateVisibility::Generic
    };
    templates::insert_if_absent(
        pool,
        templates::NewTemplate {
            id: TemplateId::new(),
            name: template,
            description: None,
            org_id,
            visibility,
            network_policy: &NetworkPolicyV1::deny_all(),
            desired_state: TemplateDesiredState::Present,
        },
    )
    .await?
    .expect("the template is new");
    Ok(())
}

async fn set_desired_state(pool: &PgPool, template: &str, state: &str) -> TestResult {
    sqlx::query("UPDATE templates SET desired_state = $2 WHERE name = $1")
        .bind(template)
        .bind(state)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_release(pool: &PgPool, template: &str) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash) VALUES ($1, $2, 'fc', $3)",
    )
    .bind(id)
    .bind(template)
    .bind(format!("sha256:{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn registration_inserts_once_and_fills_only_a_missing_description() -> TestResult {
    let db = pg_test_db().await?;
    let new = templates::NewTemplate {
        id: TemplateId::new(),
        name: "registered",
        description: None,
        org_id: Some(seed_org()),
        visibility: TemplateVisibility::OrgSpecific,
        network_policy: &NetworkPolicyV1::deny_all(),
        desired_state: TemplateDesiredState::Present,
    };
    let row = templates::insert_if_absent(&db.pool, new)
        .await?
        .expect("the first insert wins");
    assert_eq!(row.name, "registered");
    assert_eq!(row.description, None);
    assert_eq!(row.network_policy_generation, 1);
    assert_eq!(row.network_policy_json.0, NetworkPolicyV1::deny_all());

    let again = templates::NewTemplate {
        id: TemplateId::new(),
        ..new
    };
    assert!(
        templates::insert_if_absent(&db.pool, again)
            .await?
            .is_none(),
        "a second insert of the name is an outcome, not an error"
    );

    let filled = templates::fill_description(&db.pool, "registered", Some("first")).await?;
    assert_eq!(filled.description.as_deref(), Some("first"));
    let kept = templates::fill_description(&db.pool, "registered", Some("second")).await?;
    assert_eq!(kept.description.as_deref(), Some("first"));
    assert!(matches!(
        templates::fill_description(&db.pool, "missing", Some("x")).await,
        Err(sqlx::Error::RowNotFound)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_reads_follow_desired_state_and_ownership() -> TestResult {
    let db = pg_test_db().await?;
    let other = insert_org(&db.pool).await?;
    register(&db.pool, "owned", Some(seed_org())).await?;
    register(&db.pool, "generic", None).await?;
    register(&db.pool, "gone", None).await?;
    set_desired_state(&db.pool, "gone", "deleted").await?;

    assert_eq!(
        templates::desired_state(&db.pool, &name("gone"))
            .await?
            .as_deref(),
        Some("deleted")
    );
    assert_eq!(
        templates::desired_state(&db.pool, &name("missing")).await?,
        None
    );
    assert!(templates::exists_not_deleted(&db.pool, &name("owned")).await?);
    assert!(!templates::exists_not_deleted(&db.pool, &name("gone")).await?);
    assert!(!templates::exists_not_deleted(&db.pool, &name("missing")).await?);
    assert_eq!(templates::count_deleted(&db.pool).await?, 1);

    for (template, org, expected) in [
        ("owned", seed_org(), true),
        ("owned", other, false),
        ("generic", other, true),
        ("missing", other, true),
    ] {
        assert_eq!(
            templates::not_owned_by_another_org(&db.pool, &name(template), org).await?,
            expected,
            "{template} for {org}"
        );
    }

    let names = |rows: Vec<templates::VisibleTemplate>| {
        rows.into_iter().map(|row| row.name).collect::<Vec<_>>()
    };
    assert_eq!(
        names(templates::list_visible(&db.pool, seed_org(), None, 10).await?),
        ["generic", "owned"]
    );
    assert_eq!(
        names(templates::list_visible(&db.pool, other, None, 10).await?),
        ["generic"]
    );
    assert_eq!(
        names(templates::list_visible(&db.pool, seed_org(), Some("generic"), 10).await?),
        ["owned"]
    );
    assert_eq!(
        names(templates::list_visible(&db.pool, seed_org(), None, 1).await?),
        ["generic"]
    );
    assert!(
        templates::get_visible(&db.pool, seed_org(), "owned")
            .await?
            .is_some()
    );
    assert!(
        templates::get_visible(&db.pool, other, "owned")
            .await?
            .is_none()
    );
    assert!(
        templates::get_visible(&db.pool, other, "gone")
            .await?
            .is_none()
    );

    assert_eq!(
        templates::network_policy(&db.pool, "owned").await?,
        Some(NetworkPolicyV1::deny_all())
    );
    assert_eq!(templates::network_policy(&db.pool, "missing").await?, None);

    templates::update_visibility(
        &db.pool,
        "generic",
        TemplateVisibility::OrgSpecific,
        Some(other),
    )
    .await?;
    assert!(
        templates::get_visible(&db.pool, other, "generic")
            .await?
            .is_some()
    );
    assert!(
        templates::get_visible(&db.pool, seed_org(), "generic")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_job_writes_refuse_another_orgs_row() -> TestResult {
    let db = pg_test_db().await?;
    let other = insert_org(&db.pool).await?;
    let template = name("built");
    let tags = vec!["latest".to_owned()];
    let spec = build_jobs::BuildSpec {
        tags: &tags,
        cpu_count: Some(2),
        memory_mb: Some(512),
    };
    assert!(build_jobs::record_spec(&db.pool, &template, "b1", seed_org(), spec).await?);
    assert!(
        !build_jobs::record_spec(&db.pool, &template, "b1", other, spec).await?,
        "another org's spec write matches nothing"
    );
    let stored = build_jobs::spec(&db.pool, &template, "b1", seed_org())
        .await?
        .expect("the spec");
    assert_eq!(stored.tags, tags);
    assert_eq!((stored.cpu_count, stored.memory_mb), (Some(2), Some(512)));
    assert!(
        build_jobs::spec(&db.pool, &template, "b1", other)
            .await?
            .is_none()
    );
    assert_eq!(
        build_jobs::daemon_job_id(&db.pool, &template, "b1", seed_org()).await?,
        None,
        "a row the spec wrote has no job yet"
    );

    let job = BuildRunId::new();
    let accepted = |org_id| build_jobs::AcceptedBuild {
        template: &template,
        build_id: "b1",
        daemon_job_id: job,
        builder_node_id: None,
        org_id,
    };
    assert!(!build_jobs::record_accepted(&db.pool, accepted(other)).await?);
    assert!(build_jobs::record_accepted(&db.pool, accepted(seed_org())).await?);
    assert_eq!(
        build_jobs::daemon_job_id(&db.pool, &template, "b1", seed_org()).await?,
        Some(job.to_string())
    );
    assert_eq!(
        build_jobs::daemon_job_id(&db.pool, &template, "b1", other).await?,
        None
    );
    assert_eq!(build_jobs::builder_node(&db.pool, job).await?, None);

    let newer = BuildRunId::new();
    assert!(
        build_jobs::record_accepted(
            &db.pool,
            build_jobs::AcceptedBuild {
                build_id: "b2",
                daemon_job_id: newer,
                ..accepted(seed_org())
            },
        )
        .await?
    );
    sqlx::query("UPDATE template_build_jobs SET created_at = created_at + INTERVAL '1 hour' WHERE build_id = 'b2'")
        .execute(&db.pool)
        .await?;
    let recent = build_jobs::recent_accepted(&db.pool, &template, 1).await?;
    assert_eq!(
        recent
            .iter()
            .map(|row| row.daemon_job_id.clone())
            .collect::<Vec<_>>(),
        [newer.to_string()]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publication_grants_touch_only_the_named_grant() -> TestResult {
    let db = pg_test_db().await?;
    let template = name("published");
    let kept = SystemPublicationGrantId::new();
    let revoked = SystemPublicationGrantId::new();
    publication_grants::insert_system(&db.pool, kept, &template).await?;
    publication_grants::insert_system(&db.pool, revoked, &template).await?;
    publication_grants::delete_system(&db.pool, revoked).await?;
    let system: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM template_system_publication_grants")
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(system, [kept.as_uuid()]);

    let pinned = TenantPublicationGrantId::new();
    let revoked = TenantPublicationGrantId::new();
    publication_grants::insert_tenant(&db.pool, pinned, &template, seed_org()).await?;
    publication_grants::insert_tenant(&db.pool, revoked, &template, seed_org()).await?;
    let node = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node).await?;
    publication_grants::pin_tenant_to_builder(&db.pool, pinned, node).await?;
    let revoked_pin: Option<Uuid> = sqlx::query_scalar(
        "SELECT builder_node_id FROM template_tenant_publication_grants WHERE id = $1",
    )
    .bind(revoked.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(revoked_pin, None, "the pin touches only the named grant");
    publication_grants::delete_tenant(&db.pool, revoked).await?;
    let tenant: Vec<(Uuid, String, Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT id, template_name, org_id, builder_node_id FROM template_tenant_publication_grants",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        tenant,
        [(
            pinned.as_uuid(),
            "published".to_owned(),
            SEED_ORG_ID,
            Some(node.as_uuid())
        )]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tag_and_policy_deletes_reach_only_their_template_or_release() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "kept", None).await?;
    register(&db.pool, "dropped", None).await?;
    let kept = insert_release(&db.pool, "kept").await?;
    let dropped = insert_release(&db.pool, "dropped").await?;
    let other_release = insert_release(&db.pool, "dropped").await?;
    for (release, template, tag) in [
        (kept, "kept", "latest"),
        (kept, "kept", "prod"),
        (dropped, "dropped", "latest"),
        (other_release, "dropped", "canary"),
    ] {
        sqlx::query(
            "INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, $3)",
        )
        .bind(release)
        .bind(template)
        .bind(tag)
        .execute(&db.pool)
        .await?;
    }
    let tags = |rows: Vec<release_tags::ReleaseTag>| {
        rows.into_iter()
            .map(|row| (row.template_release_id, row.tag))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        tags(release_tags::list_for_releases(&db.pool, &[kept]).await?),
        [(kept, "latest".to_owned()), (kept, "prod".to_owned())]
    );
    release_tags::delete(&db.pool, kept, &TemplateTag::parse("prod")?).await?;
    release_tags::delete_for_release(&db.pool, dropped).await?;
    assert_eq!(
        tags(release_tags::list_for_releases(&db.pool, &[kept, dropped, other_release]).await?),
        [
            (kept, "latest".to_owned()),
            (other_release, "canary".to_owned())
        ]
    );
    release_tags::delete_for_template(&db.pool, &name("dropped")).await?;
    assert_eq!(
        tags(release_tags::list_for_releases(&db.pool, &[kept, dropped, other_release]).await?),
        [(kept, "latest".to_owned())]
    );

    for template in ["kept", "dropped"] {
        sqlx::query(
            "INSERT INTO template_pool_policies (template, target_count, boot_mode)
             VALUES ($1, 2, 'cold')",
        )
        .bind(template)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_base_snapshot_policies (template, ensure_base_snapshot)
             VALUES ($1, TRUE)",
        )
        .bind(template)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_default_auto_lifecycle_policies
                 (template, default_auto_lifecycle_json)
             VALUES ($1, '{}'::JSONB)",
        )
        .bind(template)
        .execute(&db.pool)
        .await?;
    }
    let dropped_name = name("dropped");
    pool_policies::delete(&db.pool, &dropped_name).await?;
    base_snapshot_policies::delete(&db.pool, &dropped_name).await?;
    super::default_auto_lifecycle_policies::delete(&db.pool, &dropped_name).await?;
    assert!(pool_policies::get(&db.pool, &dropped_name).await?.is_none());
    let pool = pool_policies::get(&db.pool, &name("kept"))
        .await?
        .expect("the kept pool policy");
    assert_eq!((pool.target_count, pool.boot_mode.as_str()), (2, "cold"));
    assert!(
        base_snapshot_policies::get_present(&db.pool, &dropped_name)
            .await?
            .is_none()
    );
    let snapshot = base_snapshot_policies::get_present(&db.pool, &name("kept"))
        .await?
        .expect("the kept snapshot policy");
    assert!(snapshot.ensure_base_snapshot);
    set_desired_state(&db.pool, "kept", "deleting").await?;
    assert!(
        base_snapshot_policies::get_present(&db.pool, &name("kept"))
            .await?
            .is_none(),
        "a template that is not present reads no policy"
    );
    let auto_lifecycle: Vec<String> =
        sqlx::query_scalar("SELECT template FROM template_default_auto_lifecycle_policies")
            .fetch_all(&db.pool)
            .await?;
    assert_eq!(auto_lifecycle, ["kept"]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_delete_keeps_the_first_requests_actor_and_operation() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "retired", None).await?;
    let template = name("retired");
    let actor = pplx_space_accounts::UserId::from_uuid(pplx_space_db_test_fixtures::SEED_USER_ID);
    let mut operations = Vec::new();
    for _ in 0..3 {
        let operation = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO public_operations
                 (operation_id, org_id, actor_user_id, kind, target_type, target_id, state)
             VALUES ($1, $2, $3, 'delete_template', 'template', 'retired', 'running')",
        )
        .bind(operation)
        .bind(SEED_ORG_ID)
        .bind(actor.as_uuid())
        .execute(&db.pool)
        .await?;
        operations.push(operation);
    }
    let second = pplx_space_accounts::UserId::new();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, 'second-actor@example.test', 'unused', 'Second Actor', $2, 'member')",
    )
    .bind(second.as_uuid())
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let requested_at = |pool: PgPool| async move {
        sqlx::query_scalar::<_, Option<time::OffsetDateTime>>(
            "SELECT delete_requested_at FROM templates WHERE name = 'retired'",
        )
        .fetch_one(&pool)
        .await
    };
    let first = operations[0];
    templates::mark_deleting(&db.pool, &template, Some(actor), first).await?;
    let first_requested_at = requested_at(db.pool.clone()).await?;
    templates::mark_deleting(&db.pool, &template, Some(second), operations[1]).await?;
    assert_eq!(
        requested_at(db.pool.clone()).await?,
        first_requested_at,
        "a retry keeps the recorded request time"
    );
    let read = |pool: PgPool| async move {
        sqlx::query_as::<_, (String, Option<Uuid>, Option<Uuid>, bool)>(
            "SELECT desired_state, delete_requested_by, delete_operation_id,
                    delete_requested_at IS NOT NULL
             FROM templates WHERE name = 'retired'",
        )
        .fetch_one(&pool)
        .await
    };
    assert_eq!(
        read(db.pool.clone()).await?,
        (
            "deleting".to_owned(),
            Some(actor.as_uuid()),
            Some(first),
            true
        )
    );
    let state = templates::delete_state(&db.pool, &template)
        .await?
        .expect("the row");
    assert_eq!(
        (
            state.desired_state.as_str(),
            state.delete_operation_id,
            state.delete_operation_live
        ),
        ("deleting", Some(first), true)
    );
    sqlx::query("UPDATE public_operations SET state = 'succeeded' WHERE operation_id = $1")
        .bind(first)
        .execute(&db.pool)
        .await?;
    assert!(
        !templates::delete_state(&db.pool, &template)
            .await?
            .expect("the row")
            .delete_operation_live,
        "a finished delete operation is not live"
    );
    assert!(
        templates::delete_state(&db.pool, &name("missing"))
            .await?
            .is_none()
    );
    templates::mark_deleted(&db.pool, &template, operations[2]).await?;
    assert_eq!(
        read(db.pool.clone()).await?,
        (
            "deleted".to_owned(),
            Some(actor.as_uuid()),
            Some(first),
            true
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_default_auto_lifecycle_upsert_replaces_the_policy() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "lifecycled", None).await?;
    let template = name("lifecycled");
    let first = pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled();
    let second = pplx_space_auto_lifecycle::AutoLifecyclePolicy {
        wake: pplx_space_auto_lifecycle::AutoLifecycleWakePolicy {
            on: vec![pplx_space_auto_lifecycle::AutoLifecycleWakeSource::Access],
        },
        ..pplx_space_auto_lifecycle::AutoLifecyclePolicy::disabled()
    };
    super::default_auto_lifecycle_policies::upsert(&db.pool, &template, &first).await?;
    super::default_auto_lifecycle_policies::upsert(&db.pool, &template, &second).await?;
    let stored: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT default_auto_lifecycle_json FROM template_default_auto_lifecycle_policies",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(stored, [serde_json::to_value(&second)?]);
    let for_create = super::default_auto_lifecycle_policies::for_create(
        &db.pool,
        "lifecycled",
        "sha256:any",
        "fc",
    )
    .await?;
    assert_eq!(for_create.map(|policy| policy.0), Some(second));
    assert!(
        super::default_auto_lifecycle_policies::for_create(&db.pool, "other", "sha256:any", "fc")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_inputs_are_scoped_to_their_grant_and_org() -> TestResult {
    let db = pg_test_db().await?;
    let other = insert_org(&db.pool).await?;
    register(&db.pool, "base", None).await?;
    let template = name("derived");
    let base = name("base");
    let pinned = Uuid::now_v7();
    let unpinned = Uuid::now_v7();
    super::build_base_grants::insert(&db.pool, pinned, &template, &base, seed_org()).await?;
    super::build_base_grants::insert(&db.pool, unpinned, &template, &base, seed_org()).await?;
    let node = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node).await?;
    super::build_base_grants::pin_to_builder(&db.pool, pinned, node).await?;
    let grants: Vec<(Uuid, String, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT id, template_id, base_template_name, builder_node_id
         FROM template_build_base_grants ORDER BY id",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        grants,
        [
            (
                pinned,
                "derived".to_owned(),
                "base".to_owned(),
                Some(node.as_uuid())
            ),
            (unpinned, "derived".to_owned(), "base".to_owned(), None),
        ]
    );

    for (files_hash, org_id) in [("owned", Some(seed_org())), ("shared", None)] {
        sqlx::query(
            "INSERT INTO template_build_blobs (template_id, files_hash, blob_sha256, org_id)
             VALUES ('derived', $1, $2, $3)",
        )
        .bind(files_hash)
        .bind(format!("blob-{files_hash}"))
        .bind(org_id.map(OrgId::as_uuid))
        .execute(&db.pool)
        .await?;
    }
    let blob = |files_hash: &'static str, org_id| {
        super::build_blobs::blob_sha256(&db.pool, &template, files_hash, org_id)
    };
    assert_eq!(
        blob("owned", seed_org()).await?.as_deref(),
        Some("blob-owned")
    );
    assert_eq!(blob("owned", other).await?, None);
    assert_eq!(blob("shared", other).await?.as_deref(), Some("blob-shared"));
    assert_eq!(blob("missing", seed_org()).await?, None);

    let record = |files_hash: &'static str, blob_sha256: &'static str, org_id| {
        super::build_blobs::record(&db.pool, &template, files_hash, blob_sha256, org_id)
    };
    assert!(
        !record("owned", "blob-taken", other).await?,
        "another org's mapping is an outcome, not an error"
    );
    assert_eq!(
        blob("owned", seed_org()).await?.as_deref(),
        Some("blob-owned")
    );
    assert!(record("owned", "blob-reupload", seed_org()).await?);
    assert_eq!(
        blob("owned", seed_org()).await?.as_deref(),
        Some("blob-reupload")
    );
    assert!(
        record("shared", "blob-claimed", other).await?,
        "an ownerless mapping is claimed"
    );
    assert_eq!(blob("shared", seed_org()).await?, None);
    assert_eq!(
        blob("shared", other).await?.as_deref(),
        Some("blob-claimed")
    );
    assert!(record("new", "blob-new", seed_org()).await?);
    assert_eq!(blob("new", seed_org()).await?.as_deref(), Some("blob-new"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_reads_see_access_in_every_state_and_policy_only_when_present() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "owned", Some(seed_org())).await?;
    set_desired_state(&db.pool, "owned", "deleting").await?;

    let access = templates::access(&db.pool, "owned")
        .await?
        .expect("the row, whatever its state");
    assert_eq!(
        (
            access.visibility.as_str(),
            access.org_id,
            access.desired_state.as_str()
        ),
        ("org_specific", Some(SEED_ORG_ID), "deleting")
    );
    assert!(templates::access(&db.pool, "missing").await?.is_none());

    assert!(
        templates::present_network_policy(&db.pool, "owned")
            .await?
            .is_none()
    );
    set_desired_state(&db.pool, "owned", "present").await?;
    let policy = templates::present_network_policy(&db.pool, "owned")
        .await?
        .expect("a present template's policy");
    assert_eq!(policy.network_policy_json.0, NetworkPolicyV1::deny_all());
    assert_eq!(policy.network_policy_generation, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn visibility_is_read_with_the_database_clock() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "owned", Some(seed_org())).await?;
    let before: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&db.pool)
        .await?;
    let mut tx = db.pool.begin().await?;
    let owned = templates::observed_visibility(&mut *tx, "owned").await?;
    sqlx::query("SELECT pg_sleep(0.01)")
        .execute(&mut *tx)
        .await?;
    let missing = templates::observed_visibility(&mut *tx, "missing").await?;
    tx.rollback().await?;
    assert_eq!(owned.visibility.as_deref(), Some("org_specific"));
    assert_eq!(
        missing.visibility, None,
        "a missing row still returns the instant"
    );
    // `now()` would return the transaction's start for both reads.
    assert!(before <= owned.observed_at && owned.observed_at < missing.observed_at);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_tag_upsert_creates_then_retargets_and_retarget_needs_the_tag() -> TestResult {
    let db = pg_test_db().await?;
    register(&db.pool, "tagged", None).await?;
    let first = insert_release(&db.pool, "tagged").await?;
    let second = insert_release(&db.pool, "tagged").await?;
    let tagged = |pool: PgPool| async move {
        sqlx::query_scalar::<_, Uuid>(
            "SELECT template_release_id FROM template_release_tags
             WHERE template_name = 'tagged' AND tag = 'latest'",
        )
        .fetch_optional(&pool)
        .await
    };

    let latest = TemplateTag::parse("latest")?;
    assert!(
        !release_tags::retarget(&db.pool, first, "tagged", &latest).await?,
        "a missing tag is an outcome"
    );
    assert_eq!(tagged(db.pool.clone()).await?, None);

    let created = release_tags::upsert(&db.pool, first, "tagged", "latest").await?;
    assert_eq!(created.tag, "latest");
    assert_eq!(tagged(db.pool.clone()).await?, Some(first));
    let moved = release_tags::upsert(&db.pool, second, "tagged", "latest").await?;
    assert_eq!(
        (moved.id, moved.created_at),
        (created.id, created.created_at),
        "the upsert keeps the tag row"
    );
    assert_eq!(tagged(db.pool.clone()).await?, Some(second));

    assert!(release_tags::retarget(&db.pool, first, "tagged", &latest).await?);
    assert_eq!(tagged(db.pool.clone()).await?, Some(first));

    assert!(
        !release_tags::insert_if_absent(&db.pool, second, "tagged", &latest).await?,
        "an existing tag is an outcome"
    );
    assert_eq!(tagged(db.pool.clone()).await?, Some(first));
    let stable = TemplateTag::parse("stable")?;
    assert!(release_tags::insert_if_absent(&db.pool, second, "tagged", &stable).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn owner_and_org_summary_read_the_rows_an_org_owns() -> TestResult {
    let db = pg_test_db().await?;
    let org = insert_org(&db.pool).await?;
    assert!(
        templates::owner(&db.pool, &name("missing"))
            .await?
            .is_none()
    );
    let retention = std::time::Duration::from_secs(60);
    let none = templates::org_templates(&db.pool, org, retention).await?;
    assert_eq!(
        (none.any_live, none.any_held, none.retained_until),
        (None, None, None)
    );

    register(&db.pool, "ownerless", None).await?;
    assert_eq!(
        templates::owner(&db.pool, &name("ownerless")).await?,
        Some(None)
    );
    register(&db.pool, "owned", Some(org)).await?;
    assert_eq!(
        templates::owner(&db.pool, &name("owned")).await?,
        Some(Some(org))
    );
    let live = templates::org_templates(&db.pool, org, retention).await?;
    assert_eq!((live.any_live, live.any_held), (Some(true), Some(false)));

    set_desired_state(&db.pool, "owned", "deleted").await?;
    let deleted = templates::org_templates(&db.pool, org, retention).await?;
    assert_eq!(
        (deleted.any_live, deleted.any_held),
        (Some(false), Some(false))
    );
    let updated_at: time::OffsetDateTime =
        sqlx::query_scalar("SELECT updated_at FROM templates WHERE name = 'owned'")
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(deleted.retained_until, Some(updated_at + retention));
    insert_release(&db.pool, "owned").await?;
    let held = templates::org_templates(&db.pool, org, retention).await?;
    assert_eq!(held.any_held, Some(true));
    Ok(())
}
