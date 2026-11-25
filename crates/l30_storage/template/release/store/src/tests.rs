use std::path::Path;
use std::sync::Mutex;

use bytes::Bytes;
use pplx_space_artifact_model::{StorageOriginRejection, TrustedStorageBuckets};
use pplx_space_content_digest::Digest;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_object_store::{
    ObjectCredentials, ObjectMeta, ObjectStore, ObjectStoreError, ObjectTarget,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

const TEMPLATE: &str = "seed";
const HASH: &str = "sha256:abababababababababababababababababababababababababababababababab";
const LOCATION: &str = "s3://templates-bucket/space/templates/seed/sha256:abababababababababababababababababababababababababababababababab?region=us-east-1";

pub(crate) fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// The locations a twin's metadata was written to: the seeded one.
fn prepared() -> Vec<String> {
    vec![LOCATION.to_owned()]
}

fn bound(environment: EnvironmentReference) -> PartitionSelector {
    PartitionSelector {
        environment,
        includes_unbound: false,
    }
}

pub(crate) fn metadata(environment: EnvironmentReference) -> serde_json::Value {
    serde_json::json!({ "environment": environment.to_string() })
}

/// A migrated database without `idx_template_releases_template_name_hash_unique`,
/// so twins can be inserted. Dropping it here keeps these tests independent of
/// whether the migration that drops it is in the tree; `IF EXISTS` makes the
/// drop a no-op once it is.
async fn twin_ready_db() -> TestResult<Option<pplx_space_db_test_fixtures::TestDb>> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    sqlx::query("DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique")
        .execute(&db.pool)
        .await?;
    Ok(Some(db))
}

pub(crate) async fn insert_template(pool: &PgPool, name: &str) -> TestResult {
    sqlx::query("INSERT INTO templates (id, name) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_release(
    pool: &PgPool,
    name: &str,
    hash: &str,
    execution_metadata: Option<serde_json::Value>,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         VALUES ($1, 'fc', $2, $3, clock_timestamp())
         RETURNING id",
    )
    .bind(name)
    .bind(hash)
    .bind(execution_metadata)
    .fetch_one(pool)
    .await?)
}

async fn insert_location(pool: &PgPool, release: Uuid, location: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, $2)",
    )
    .bind(release)
    .bind(location)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seeded(pool: &PgPool, original_metadata: Option<serde_json::Value>) -> TestResult<Uuid> {
    insert_template(pool, TEMPLATE).await?;
    let original = insert_release(pool, TEMPLATE, HASH, original_metadata).await?;
    insert_location(pool, original, LOCATION).await?;
    Ok(original)
}

/// Record `environment`'s policy state, as the policy store leaves it.
async fn set_state(pool: &PgPool, environment: EnvironmentReference, state: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, $2, 'test')
         ON CONFLICT (environment_ref) DO UPDATE SET state = EXCLUDED.state",
    )
    .bind(environment.to_string())
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// Make `environment` the cluster default, as the policy store's
/// `set_default` leaves the rows: allowed, and the policy's default.
pub(crate) async fn set_default(pool: &PgPool, environment: EnvironmentReference) -> TestResult {
    set_state(pool, environment, "allowed").await?;
    sqlx::query(
        "UPDATE runtime_environment_policy
         SET default_environment_ref = $1, generation = generation + 1
         WHERE singleton",
    )
    .bind(environment.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Catalog `environment` with the runtime version sort key `version`.
pub(crate) async fn catalog(
    pool: &PgPool,
    environment: EnvironmentReference,
    version: &[i32],
) -> TestResult {
    sqlx::query(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key, kernel_version,
             state_format_name, state_format_version
         )
         VALUES ($1, 'firecracker', 'test', $2, 'test', 'test', 'test')",
    )
    .bind(environment.to_string())
    .bind(version)
    .execute(pool)
    .await?;
    Ok(())
}

/// A twin of `original` bound to `environment`, registered after it.
async fn twin_of(
    pool: &PgPool,
    original: Uuid,
    environment: EnvironmentReference,
) -> TestResult<Uuid> {
    match insert_twin(
        pool,
        original,
        bound(environment),
        &metadata(environment),
        &prepared(),
    )
    .await?
    {
        InsertTwin::Inserted(twin) => Ok(twin),
        other => panic!("the twin should be inserted, not {other:?}"),
    }
}

/// The release a create of the seeded build resolves to.
async fn resolved(pool: &PgPool) -> TestResult<Option<Uuid>> {
    Ok(resolved_release(pool, TEMPLATE, HASH, environment(0))
        .await?
        .map(|(release, _)| release))
}

async fn tag_count(pool: &PgPool, release: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM template_release_tags WHERE template_release_id = $1",
    )
    .bind(release)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn a_twin_takes_its_originals_identity_and_locations_and_no_tags() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let org_id = Uuid::now_v7();
    sqlx::query("INSERT INTO organizations (id, name, slug) VALUES ($1, 'owner', 'owner')")
        .bind(org_id)
        .execute(&db.pool)
        .await?;
    let original = seeded(&db.pool, Some(metadata(e0))).await?;
    for statement in [
        "UPDATE templates SET org_id = $1",
        "UPDATE template_releases SET org_id = $1",
        "UPDATE template_release_storage_locations SET org_id = $1",
    ] {
        sqlx::query(statement)
            .bind(org_id)
            .execute(&db.pool)
            .await?;
    }
    // A replica the twin's metadata wasn't written to stays the original's.
    sqlx::query(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)",
    )
    .bind(original)
    .bind("s3://replica-bucket/space/templates/seed/x?region=us-east-1")
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    // A tag on the original, so a twin that copied tags would show one.
    sqlx::query(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, 'stable')",
    )
    .bind(original)
    .bind(TEMPLATE)
    .execute(&db.pool)
    .await?;

    let InsertTwin::Inserted(twin) =
        insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?
    else {
        panic!("the twin should be inserted");
    };
    let subject = release_subject(&db.pool, twin).await?.expect("twin");
    assert_eq!(subject.template_name, TEMPLATE);
    assert_eq!(subject.hash, HASH);
    assert_eq!(subject.environment, Some(e1.to_string()));
    assert!(subject.template_present);
    let twin_org: Option<Uuid> =
        sqlx::query_scalar("SELECT org_id FROM template_releases WHERE id = $1")
            .bind(twin)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(twin_org, Some(org_id));
    let location_orgs: Vec<Option<Uuid>> = sqlx::query_scalar(
        "SELECT org_id FROM template_release_storage_locations WHERE template_release_id = $1",
    )
    .bind(twin)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(location_orgs, vec![Some(org_id)]);
    assert_eq!(
        storage_locations(&db.pool, twin).await?,
        vec![LOCATION.to_owned()]
    );
    assert_eq!(tag_count(&db.pool, twin).await?, 0);
    assert_eq!(tag_count(&db.pool, original).await?, 1);

    // Retried, and with the partition already filled by anyone, it's a no-op.
    assert_eq!(
        insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?,
        InsertTwin::Existing(twin)
    );
    assert_eq!(
        release_of_hash_in_partition(&db.pool, TEMPLATE, HASH, bound(e0)).await?,
        Some(original)
    );
    assert_eq!(
        release_of_hash_in_partition(&db.pool, TEMPLATE, HASH, bound(e1)).await?,
        Some(twin),
        "a later campaign's source finds the twin, not the original"
    );
    Ok(())
}

/// What's left of `idx_template_releases_template_name_hash_unique` on
/// `public.template_releases`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OldIndex {
    Standing,
    Dropped,
    /// Left by a `DROP INDEX CONCURRENTLY` that failed after marking the index
    /// invalid: query plans skip it, while inserts still check it.
    InvalidReady,
    /// Left by a concurrent build or drop once inserts no longer check it.
    InvalidNotReady,
    /// Dropped from `public`, while an unrelated index of the same name sits
    /// earlier on `search_path`.
    DroppedButShadowed,
}

#[tokio::test(flavor = "current_thread")]
async fn the_old_unique_index_refuses_a_twin() -> TestResult {
    for state in [
        OldIndex::Standing,
        OldIndex::Dropped,
        OldIndex::InvalidReady,
        OldIndex::InvalidNotReady,
        OldIndex::DroppedButShadowed,
    ] {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_template_releases_template_name_hash_unique
                 ON public.template_releases (template_name, hash)",
        )
        .execute(&db.pool)
        .await?;
        match state {
            OldIndex::Standing => {}
            OldIndex::Dropped | OldIndex::DroppedButShadowed => {
                sqlx::query(
                    "DROP INDEX IF EXISTS public.idx_template_releases_template_name_hash_unique",
                )
                .execute(&db.pool)
                .await?;
                if state == OldIndex::DroppedButShadowed {
                    for statement in [
                        "DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$",
                        "DO $$ BEGIN EXECUTE format(
                             'CREATE TABLE %I.decoy (template_name text, hash text)',
                             current_user); END $$",
                        "DO $$ BEGIN EXECUTE format(
                             'CREATE UNIQUE INDEX idx_template_releases_template_name_hash_unique
                              ON %I.decoy (template_name, hash)',
                             current_user); END $$",
                    ] {
                        sqlx::query(statement).execute(&db.pool).await?;
                    }
                }
            }
            OldIndex::InvalidReady => {
                sqlx::query(
                    "UPDATE pg_index SET indisvalid = false
                     WHERE indexrelid = 'public.idx_template_releases_template_name_hash_unique'::regclass",
                )
                .execute(&db.pool)
                .await?;
            }
            OldIndex::InvalidNotReady => {
                sqlx::query(
                    "UPDATE pg_index SET indisvalid = false, indisready = false
                     WHERE indexrelid = 'public.idx_template_releases_template_name_hash_unique'::regclass",
                )
                .execute(&db.pool)
                .await?;
            }
        }

        let (e0, e1) = (environment(0), environment(1));
        let original = seeded(&db.pool, Some(metadata(e0))).await?;
        let outcome =
            insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?;
        if matches!(state, OldIndex::Standing | OldIndex::InvalidReady) {
            assert_eq!(outcome, InsertTwin::Refused, "{state:?}");
            assert_eq!(release_count(&db.pool).await?, 1, "{state:?}");
        } else {
            assert!(
                matches!(outcome, InsertTwin::Inserted(_)),
                "{state:?}: {outcome:?}"
            );
            assert_eq!(release_count(&db.pool).await?, 2, "{state:?}");
        }
    }
    Ok(())
}

async fn release_count(pool: &PgPool) -> TestResult<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM template_releases")
        .fetch_one(pool)
        .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn a_twin_whose_metadata_binds_another_environment_is_refused() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    let original = seeded(&db.pool, Some(metadata(e0))).await?;

    for execution_metadata in [metadata(e2), serde_json::json!({})] {
        let refused = insert_twin(
            &db.pool,
            original,
            bound(e1),
            &execution_metadata,
            &prepared(),
        )
        .await;
        assert!(
            matches!(refused, Err(sqlx::Error::InvalidArgument(_))),
            "{execution_metadata}: {refused:?}"
        );
    }
    assert_eq!(release_count(&db.pool).await?, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_twin_with_none_of_its_originals_locations_is_rolled_back() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let original = seeded(&db.pool, Some(metadata(e0))).await?;

    for locations in [
        vec![],
        vec!["s3://other-bucket/x?region=us-east-1".to_owned()],
    ] {
        let refused = insert_twin(&db.pool, original, bound(e1), &metadata(e1), &locations).await;
        assert!(
            matches!(refused, Err(sqlx::Error::InvalidArgument(_))),
            "{locations:?}: {refused:?}"
        );
    }
    assert_eq!(
        release_count(&db.pool).await?,
        1,
        "no twin without a location"
    );
    // A retry with a written location then inserts the twin, rather than
    // hitting a left-behind location-less row and refusing.
    assert!(matches!(
        insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?,
        InsertTwin::Inserted(_)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_twin_keeps_its_originals_location_order_and_sorts_after_it() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let original = seeded(&db.pool, Some(metadata(e0))).await?;
    const REPLICA: &str = "s3://replica-bucket/space/templates/seed/x?region=us-east-1";
    insert_location(&db.pool, original, REPLICA).await?;
    // Registration records a release's locations in one transaction, so
    // they share a timestamp and only their random ids order them. The
    // original is an hour ahead of this clock, as after the database clock
    // stepped back.
    sqlx::query(
        "UPDATE template_release_storage_locations
         SET created_at = now() - interval '1 hour'
         WHERE template_release_id = $1",
    )
    .bind(original)
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "UPDATE template_releases SET created_at = now() + interval '1 hour' WHERE id = $1",
    )
    .bind(original)
    .execute(&db.pool)
    .await?;

    let original_locations = storage_locations(&db.pool, original).await?;
    let reversed_locations: Vec<String> = original_locations.iter().rev().cloned().collect();
    assert_ne!(reversed_locations, original_locations);

    let InsertTwin::Inserted(twin) = insert_twin(
        &db.pool,
        original,
        bound(e1),
        &metadata(e1),
        &reversed_locations,
    )
    .await?
    else {
        panic!("the twin should be inserted");
    };
    assert_eq!(storage_locations(&db.pool, twin).await?, original_locations);
    let twin_after_original: bool = sqlx::query_scalar(
        "SELECT t.created_at > o.created_at
         FROM template_releases t, template_releases o
         WHERE t.id = $1 AND o.id = $2",
    )
    .bind(twin)
    .bind(original)
    .fetch_one(&db.pool)
    .await?;
    assert!(twin_after_original);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unbound_release_fills_the_legacy_partition() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let e0 = environment(0);
    let original = seeded(&db.pool, None).await?;
    let legacy = PartitionSelector {
        environment: e0,
        includes_unbound: true,
    };

    // The initial environment's partition already holds the unbound release,
    // so no initial-bound twin is added beside it.
    assert_eq!(
        insert_twin(&db.pool, original, legacy, &metadata(e0), &prepared()).await?,
        InsertTwin::Existing(original)
    );
    assert_eq!(
        source_releases_page(&db.pool, legacy, None, 10).await?,
        vec![original]
    );
    assert!(
        source_releases_page(&db.pool, bound(e0), None, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

async fn wait_until_blocked_by(holder: &mut sqlx::PgConnection) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        sqlx::query("SELECT pg_stat_clear_snapshot()")
            .execute(&mut *holder)
            .await?;
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND pg_backend_pid() = ANY(pg_blocking_pids(pid))
             )",
        )
        .fetch_one(&mut *holder)
        .await?;
        if blocked {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err("nothing waited for the holder's lock".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_deleting_template_gets_no_twin() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let original = seeded(&db.pool, Some(metadata(e0))).await?;

    // An admin release delete holds `FOR NO KEY UPDATE` on the template row
    // without touching `desired_state`, so `insert_twin` waits on that lock,
    // sees 0 rows inserted once the original is gone, and reports
    // `OriginalGone` rather than `Refused`.
    let mut delete_tx = db.pool.begin().await?;
    sqlx::query("SELECT 1 FROM templates WHERE name = $1 FOR NO KEY UPDATE")
        .bind(TEMPLATE)
        .execute(&mut *delete_tx)
        .await?;
    let pool = db.pool.clone();
    let raced = tokio::spawn(async move {
        insert_twin(&pool, original, bound(e1), &metadata(e1), &prepared()).await
    });
    wait_until_blocked_by(&mut delete_tx).await?;
    assert!(
        !raced.is_finished(),
        "insert_twin waits on the template lock"
    );
    sqlx::query("DELETE FROM template_release_storage_locations WHERE template_release_id = $1")
        .bind(original)
        .execute(&mut *delete_tx)
        .await?;
    sqlx::query("DELETE FROM template_releases WHERE id = $1")
        .bind(original)
        .execute(&mut *delete_tx)
        .await?;
    delete_tx.commit().await?;
    assert_eq!(raced.await??, InsertTwin::OriginalGone);
    assert_eq!(release_count(&db.pool).await?, 0);

    let original = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e0))).await?;
    insert_location(&db.pool, original, LOCATION).await?;
    sqlx::query("UPDATE templates SET desired_state = 'deleting' WHERE name = $1")
        .bind(TEMPLATE)
        .execute(&db.pool)
        .await?;

    assert_eq!(
        insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?,
        InsertTwin::OriginalGone
    );
    assert_eq!(
        insert_twin(
            &db.pool,
            Uuid::now_v7(),
            bound(e1),
            &metadata(e1),
            &prepared()
        )
        .await?,
        InsertTwin::OriginalGone
    );
    assert!(
        !release_subject(&db.pool, original)
            .await?
            .expect("original")
            .template_present
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn source_pages_select_one_environment_in_id_order() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    insert_template(&db.pool, TEMPLATE).await?;
    let mut on_e0 = Vec::new();
    for byte in 0..3u8 {
        let hash = format!("sha256:{}", format!("{byte:02x}").repeat(32));
        on_e0.push(insert_release(&db.pool, TEMPLATE, &hash, Some(metadata(e0))).await?);
    }
    let hash = format!("sha256:{}", "ff".repeat(32));
    insert_release(&db.pool, TEMPLATE, &hash, Some(metadata(e1))).await?;
    on_e0.sort();

    assert_eq!(
        source_releases_page(&db.pool, bound(e0), None, 2).await?,
        on_e0[..2].to_vec()
    );
    assert_eq!(
        source_releases_page(&db.pool, bound(e0), Some(on_e0[1]), 2).await?,
        on_e0[2..].to_vec()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_release_of_a_hash_matches_either_spelling() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let original = insert_release(&db.pool, TEMPLATE, bare, None).await?;
    let legacy = PartitionSelector {
        environment: environment(0),
        includes_unbound: true,
    };
    assert_eq!(
        release_of_hash_in_partition(&db.pool, TEMPLATE, HASH, legacy).await?,
        Some(original)
    );
    assert_eq!(
        release_of_hash_in_partition(&db.pool, "other", HASH, legacy).await?,
        None
    );
    let blake3 = format!("blake3:{bare}");
    insert_release(&db.pool, TEMPLATE, &format!("sha256:{blake3}"), None).await?;
    assert_eq!(
        release_of_hash_in_partition(&db.pool, TEMPLATE, &blake3, legacy).await?,
        None
    );
    assert_eq!(
        resolved_release(&db.pool, TEMPLATE, &blake3, environment(0)).await?,
        None
    );
    Ok(())
}

/// An original's twins are its build's releases in other environments,
/// under either spelling of the hash, and never a release in its own
/// environment: an unbound release of the other spelling counts as the
/// initial environment's, as the original does.
#[tokio::test(flavor = "current_thread")]
async fn an_originals_twins_are_its_builds_releases_in_other_environments() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let original = seeded(&db.pool, None).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let other_spelling = insert_release(&db.pool, TEMPLATE, bare, None).await?;
    let bound_to_initial =
        insert_release(&db.pool, TEMPLATE, bare, Some(metadata(environment(0)))).await?;
    let e1 = twin_of(&db.pool, original, environment(1)).await?;
    let e2_other_spelling =
        insert_release(&db.pool, TEMPLATE, bare, Some(metadata(environment(2)))).await?;
    insert_template(&db.pool, "other").await?;
    insert_release(&db.pool, "other", HASH, Some(metadata(environment(1)))).await?;

    let mut expected = vec![e1, e2_other_spelling];
    expected.sort();
    assert_eq!(
        twin_releases(&db.pool, original, environment(0)).await?,
        expected,
        "neither {other_spelling} nor {bound_to_initial} is in another environment"
    );
    assert_eq!(
        twin_releases(&db.pool, Uuid::now_v7(), environment(0)).await?,
        Vec::<Uuid>::new()
    );
    Ok(())
}

/// A twin's original is the one registration finds: of the earliest release
/// under each spelling of the hash, one owning a storage location over one
/// without. Releases in that original's environment, under either spelling,
/// have none.
#[tokio::test(flavor = "current_thread")]
async fn a_twins_original_spans_both_hash_spellings() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let unattached =
        insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(environment(1)))).await?;
    let original = insert_release(&db.pool, TEMPLATE, bare, None).await?;
    insert_location(&db.pool, original, LOCATION).await?;
    let sibling = insert_release(&db.pool, TEMPLATE, HASH, None).await?;
    insert_location(&db.pool, sibling, LOCATION).await?;
    let twin = twin_of(&db.pool, original, environment(2)).await?;

    for (release, expected) in [
        (unattached, Some(original)),
        (twin, Some(original)),
        (original, None),
        (sibling, None),
        (Uuid::now_v7(), None),
    ] {
        assert_eq!(
            twin_original(&db.pool, release, environment(0)).await?,
            expected,
            "release {release}"
        );
    }
    Ok(())
}

/// A twin with a storage location its original lacks is still a twin: it
/// copies the original's hash, so it's never the earliest release under it.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_with_storage_its_original_lacks_is_still_a_twin() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let original = seeded(&db.pool, None).await?;
    let twin = twin_of(&db.pool, original, environment(1)).await?;
    sqlx::query("DELETE FROM template_release_storage_locations WHERE template_release_id = $1")
        .bind(original)
        .execute(&db.pool)
        .await?;
    assert!(
        !storage_locations(&db.pool, twin).await?.is_empty(),
        "the twin copies its original's storage locations"
    );
    assert_eq!(
        twin_original(&db.pool, twin, environment(0)).await?,
        Some(original)
    );
    assert_eq!(
        twin_original(&db.pool, original, environment(0)).await?,
        None
    );
    Ok(())
}

/// Records every write; reads answer from `objects`.
#[derive(Default)]
struct RecordingStore {
    objects: Mutex<Vec<(String, String, Bytes)>>,
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn head(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        Ok(None)
    }

    async fn put_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        self.objects.lock().expect("lock").push((
            target.bucket.to_owned(),
            target.key.to_owned(),
            body,
        ));
        Ok(())
    }

    async fn get_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.objects
            .lock()
            .expect("lock")
            .iter()
            .rev()
            .find(|(bucket, key, _)| bucket == target.bucket && key == target.key)
            .map(|(_, _, body)| body.to_vec())
            .ok_or_else(|| ObjectStoreError::NotFound {
                key: target.key.to_owned(),
            })
    }

    async fn put_file(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        _src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        Err(ObjectStoreError::Io(std::io::Error::other(
            "metadata is never a file upload",
        )))
    }

    async fn get_file(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        _dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        Err(ObjectStoreError::Io(std::io::Error::other(
            "metadata is never a file download",
        )))
    }
}

/// A storage location of the seeded release.
fn target(location: &str) -> Result<TwinMetadataTarget, ObjectAccessError> {
    TwinMetadataTarget::parse(location, TEMPLATE, HASH)
}

fn credentials() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "test".to_owned(),
        secret_access_key: "test".to_owned(),
        session_token: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn twin_metadata_goes_under_its_environment_and_never_over_the_shared_one() -> TestResult {
    let store = RecordingStore::default();
    let target = target(LOCATION)?;
    let shared = format!("space/templates/{TEMPLATE}/{HASH}/metadata.json");
    store.objects.lock().expect("lock").push((
        "templates-bucket".to_owned(),
        shared.clone(),
        Bytes::from_static(b"{\"id\":\"seed\"}"),
    ));
    assert_eq!(
        objects::read_release_metadata(&store, &credentials(), &target).await?,
        b"{\"id\":\"seed\"}".to_vec()
    );

    let e1 = environment(1);
    objects::write_twin_metadata(
        &store,
        &credentials(),
        &target,
        e1,
        Bytes::from_static(b"{}"),
    )
    .await?;
    let written = store.objects.lock().expect("lock");
    assert_eq!(written.len(), 2);
    assert_eq!(written[0].1, shared, "the shared metadata is untouched");
    assert_eq!(
        written[1].1,
        format!("space/templates/{TEMPLATE}/{HASH}/environments/{e1}/metadata.json")
    );
    assert_eq!(written[1].0, "templates-bucket");
    Ok(())
}

#[test]
fn a_location_that_is_no_object_store_uri_is_refused() {
    assert!(matches!(
        target("https://example.invalid/seed"),
        Err(ObjectAccessError::Location { .. })
    ));
}

/// A location that doesn't parse is reported without its connection string,
/// which can carry endpoint credentials.
#[test]
fn a_refused_location_keeps_its_connection_string_out_of_the_error() {
    let error = target(
        "s3://templates-bucket/seed?region=us-east-1&endpoint=https://user:hunter2@object-store.example&unknown=1",
    )
    .expect_err("an unsupported query parameter is refused");
    let mut text = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    assert!(!text.contains("hunter2"), "{text}");
    assert!(text.contains("unsupported query parameter"), "{text}");
}

/// After an E0 to E1 campaign, moving the default to E2 keeps creates on the
/// E1 twin until the E2 twin exists, rather than sending them back to the E0
/// original; once it exists, it takes over. Among allowed twins, the newest
/// catalog entry wins.
#[tokio::test(flavor = "current_thread")]
async fn a_second_campaign_keeps_creates_on_the_first_ones_twin() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2, e3) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
    );
    for (environment, version) in [(e0, [1, 15]), (e1, [1, 17]), (e2, [1, 18]), (e3, [1, 16])] {
        catalog(&db.pool, environment, &version).await?;
    }
    let original = seeded(&db.pool, Some(metadata(e0))).await?;
    set_default(&db.pool, e0).await?;
    assert_eq!(resolved(&db.pool).await?, Some(original));

    set_default(&db.pool, e1).await?;
    let e1_twin = twin_of(&db.pool, original, e1).await?;
    assert_eq!(resolved(&db.pool).await?, Some(e1_twin));
    // An older allowed twin doesn't displace the newer one.
    set_state(&db.pool, e3, "allowed").await?;
    twin_of(&db.pool, original, e3).await?;

    set_default(&db.pool, e2).await?;
    assert_eq!(
        resolved(&db.pool).await?,
        Some(e1_twin),
        "with no E2 twin yet, the E1 twin keeps serving"
    );
    let e2_twin = twin_of(&db.pool, original, e2).await?;
    assert_eq!(resolved(&db.pool).await?, Some(e2_twin));
    Ok(())
}

/// A twin in a deprecated environment comes after any allowed one and before
/// the original; a blocked environment's release is never resolved, the
/// original included, and with nothing left there's no resolved release. A
/// twin whose environment has no policy state isn't resolved either.
#[tokio::test(flavor = "current_thread")]
async fn deprecated_twins_trail_allowed_ones_and_blocked_releases_never_resolve() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2, e3) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
    );
    for (environment, version) in [(e0, [1, 15]), (e1, [1, 17]), (e2, [1, 18])] {
        catalog(&db.pool, environment, &version).await?;
    }
    let original = seeded(&db.pool, Some(metadata(e0))).await?;
    set_default(&db.pool, e0).await?;
    set_state(&db.pool, e1, "deprecated").await?;
    let e1_twin = twin_of(&db.pool, original, e1).await?;
    // No state for E3: a twin there isn't a candidate.
    twin_of(&db.pool, original, e3).await?;
    set_default(&db.pool, e2).await?;
    assert_eq!(
        resolved(&db.pool).await?,
        Some(e1_twin),
        "a deprecated twin before the original"
    );
    // An allowed twin comes before a deprecated one, whatever their
    // catalog order.
    let e4 = environment(4);
    catalog(&db.pool, e4, &[1, 10]).await?;
    set_state(&db.pool, e4, "allowed").await?;
    let e4_twin = twin_of(&db.pool, original, e4).await?;
    assert_eq!(resolved(&db.pool).await?, Some(e4_twin));
    set_state(&db.pool, e4, "blocked").await?;
    assert_eq!(resolved(&db.pool).await?, Some(e1_twin));

    set_state(&db.pool, e1, "blocked").await?;
    assert_eq!(
        resolved(&db.pool).await?,
        Some(original),
        "the blocked twin is passed over for the original"
    );
    set_state(&db.pool, e0, "blocked").await?;
    assert_eq!(resolved(&db.pool).await?, None, "nothing blocked resolves");
    Ok(())
}

/// A release recorded without an environment counts as the initial
/// environment's: with that as the default it's the default-bound release,
/// and a block of it blocks the release.
#[tokio::test(flavor = "current_thread")]
async fn an_unbound_original_counts_as_the_initial_environments() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    catalog(&db.pool, e1, &[1, 17]).await?;
    let original = seeded(&db.pool, None).await?;
    set_default(&db.pool, e1).await?;
    let twin = twin_of(&db.pool, original, e1).await?;
    assert_eq!(resolved(&db.pool).await?, Some(twin));

    // Back to the initial environment, which the unbound original is in.
    set_default(&db.pool, e0).await?;
    assert_eq!(resolved(&db.pool).await?, Some(original));

    set_default(&db.pool, e1).await?;
    set_state(&db.pool, e0, "blocked").await?;
    assert_eq!(resolved(&db.pool).await?, Some(twin));
    sqlx::query(
        "UPDATE runtime_environment_policy SET default_environment_ref = NULL WHERE singleton",
    )
    .execute(&db.pool)
    .await?;
    set_state(&db.pool, e1, "blocked").await?;
    assert_eq!(resolved(&db.pool).await?, None);
    Ok(())
}

/// A release of the build already in the target's partition counts whichever
/// spelling of the hash it was registered under: an original stored bare
/// gets no second release beside one registered there as `sha256:`.
#[tokio::test(flavor = "current_thread")]
async fn a_release_under_the_other_hash_spelling_fills_the_partition() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let original = insert_release(&db.pool, TEMPLATE, bare, Some(metadata(e0))).await?;
    let registered = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e1))).await?;
    assert_eq!(
        insert_twin(&db.pool, original, bound(e1), &metadata(e1), &prepared()).await?,
        InsertTwin::Existing(registered)
    );
    assert_eq!(
        release_of_hash_in_partition(&db.pool, TEMPLATE, bare, bound(e1)).await?,
        Some(registered)
    );
    Ok(())
}

/// Releases of a build stored under bare and `sha256:` spellings share one
/// resolution order: only the first registered release's environment counts as
/// the original, so a `sha256:` release in an `allowed` environment beats a
/// `deprecated` bare twin, while one in an unlisted environment never resolves
/// as a second original.
#[tokio::test(flavor = "current_thread")]
async fn resolution_spans_both_hash_spellings() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2, e3, e4) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
        environment(4),
    );
    catalog(&db.pool, e0, &[1, 15]).await?;
    catalog(&db.pool, e4, &[1, 16]).await?;
    catalog(&db.pool, e1, &[1, 17]).await?;
    catalog(&db.pool, e2, &[1, 18]).await?;
    catalog(&db.pool, e3, &[1, 19]).await?;
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let original = insert_release(&db.pool, TEMPLATE, bare, Some(metadata(e0))).await?;
    insert_location(&db.pool, original, LOCATION).await?;
    let canonical = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e1))).await?;
    insert_location(&db.pool, canonical, LOCATION).await?;
    set_state(&db.pool, e1, "allowed").await?;
    set_default(&db.pool, e0).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(original),
            "{hash}: the bare original is the build's original"
        );
    }
    set_default(&db.pool, e1).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(canonical),
            "{hash}"
        );
    }

    let _deprecated_twin = twin_of(&db.pool, original, e2).await?;
    set_state(&db.pool, e2, "deprecated").await?;
    set_default(&db.pool, e3).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(canonical),
            "{hash}: allowed release under the other spelling beats deprecated twin"
        );
    }

    let bare_other = "cd".repeat(32);
    let prefixed_other = format!("sha256:{bare_other}");
    let original_other =
        insert_release(&db.pool, TEMPLATE, &bare_other, Some(metadata(e0))).await?;
    let _unlisted_other =
        insert_release(&db.pool, TEMPLATE, &prefixed_other, Some(metadata(e4))).await?;
    for hash in [bare_other.as_str(), prefixed_other.as_str()] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(original_other),
            "{hash}: an unlisted environment under the other spelling is no original"
        );
    }
    Ok(())
}

/// An unbound release takes the initial environment's catalog entry too:
/// while the initial environment is the default, it ties with an initial-bound
/// release of the build under the other hash spelling, and the digest
/// original's spelling breaks the tie whichever spelling the query names, so
/// creates by either spelling run the release the warm pool warms. Once the
/// default moves on, both initial-environment releases fall to the
/// original-environment fallback tier (even with `e0` still `allowed`), so the
/// `deprecated` campaign twin serves both spellings.
#[tokio::test(flavor = "current_thread")]
async fn an_unbound_release_ranks_with_the_initial_environments_catalog_entry() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    catalog(&db.pool, e0, &[1, 15]).await?;
    catalog(&db.pool, e1, &[1, 17]).await?;
    catalog(&db.pool, e2, &[1, 18]).await?;
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let unbound = insert_release(&db.pool, TEMPLATE, bare, None).await?;
    insert_location(&db.pool, unbound, LOCATION).await?;
    let bound_e0 = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e0))).await?;
    insert_location(&db.pool, bound_e0, LOCATION).await?;
    set_default(&db.pool, e0).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(unbound),
            "{hash}: the older storage-backed spelling is the digest original, not {bound_e0}"
        );
    }
    let twin_e1 = twin_of(&db.pool, unbound, e1).await?;
    set_state(&db.pool, e1, "deprecated").await?;
    set_default(&db.pool, e2).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(twin_e1),
            "{hash}: second initial-environment spelling stays behind campaign twin"
        );
    }
    Ok(())
}

/// Registration never attaches to the other spelling's original, so a digest
/// can carry an older bare original in `e2` with no storage location alongside
/// its storage-backed `sha256:` original in `e0`. That artifact-less row is
/// neither the build's preferred original, nor a create candidate before `e2`
/// gets a storage-backed twin, nor a partition occupant blocking `insert_twin`,
/// nor a source item in a later campaign out of `e2`, nor the spelling winner
/// over `e2`'s twin once inserted.
#[tokio::test(flavor = "current_thread")]
async fn an_artifactless_other_spelling_original_neither_blocks_nor_wins() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e2) = (environment(0), environment(2));
    catalog(&db.pool, e0, &[1, 15]).await?;
    catalog(&db.pool, e2, &[1, 18]).await?;
    insert_template(&db.pool, TEMPLATE).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");

    let bare_e2 = insert_release(&db.pool, TEMPLATE, bare, Some(metadata(e2))).await?;
    let canonical_e0 = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e0))).await?;
    insert_location(&db.pool, canonical_e0, LOCATION).await?;

    // Even once e2 is allowed or the cluster default, creates stay on the
    // storage-backed original until e2 has a storage-backed release.
    set_state(&db.pool, e2, "allowed").await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(canonical_e0),
            "{hash}: artifact-less e2 row must not outrank storage-backed original"
        );
    }
    set_default(&db.pool, e2).await?;
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(canonical_e0),
            "{hash}: artifact-less e2 row must not win as default"
        );
    }

    // Neither partition lookup nor source paging treats the artifact-less row
    // as an e2 release of the storage-backed build.
    for hash in [bare, HASH] {
        assert_eq!(
            release_of_hash_in_partition(&db.pool, TEMPLATE, hash, bound(e2)).await?,
            None,
            "{hash}"
        );
    }
    assert!(
        source_releases_page(&db.pool, bound(e2), None, 10)
            .await?
            .is_empty(),
        "{bare_e2} has no storage location while {canonical_e0} does"
    );

    // Campaigning e0 -> e2 inserts a storage-backed twin under the preferred
    // spelling rather than returning bare_e2 as Existing.
    let twin_e2 = twin_of(&db.pool, canonical_e0, e2).await?;
    assert_ne!(twin_e2, bare_e2);
    for hash in [bare, HASH] {
        assert_eq!(
            resolved_release(&db.pool, TEMPLATE, hash, e0)
                .await?
                .map(|(id, _)| id),
            Some(twin_e2),
            "{hash}: storage-backed twin wins even when queried bare"
        );
        assert_eq!(
            release_of_hash_in_partition(&db.pool, TEMPLATE, hash, bound(e2)).await?,
            Some(twin_e2),
            "{hash}"
        );
    }
    assert_eq!(
        source_releases_page(&db.pool, bound(e2), None, 10).await?,
        vec![twin_e2]
    );

    // A location-less row under the exact same spelling occupies that
    // partition's unique-index slot without serving artifacts, so twinning
    // into it is refused with that row named, rather than claiming a usable
    // Existing release or blaming the old index.
    let e3 = environment(3);
    let bare_e3 = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e3))).await?;
    assert_eq!(
        insert_twin(
            &db.pool,
            canonical_e0,
            bound(e3),
            &metadata(e3),
            &prepared()
        )
        .await?,
        InsertTwin::SlotTaken(bare_e3)
    );
    Ok(())
}

/// A rollback that fails is reported in place of the error that asked for it,
/// so a caller can't read a failed cleanup as a confirmed one.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_rollback_replaces_the_error_that_asked_for_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut tx = db.pool.begin().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await?;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1, 5000)")
        .bind(pid)
        .fetch_one(&db.pool)
        .await?;
    assert!(terminated);
    let ended = end_transaction::<(), sqlx::Error>(
        tx,
        Err(sqlx::Error::InvalidArgument("rejected".to_owned())),
    )
    .await;
    match ended {
        Err(sqlx::Error::InvalidArgument(message)) => {
            panic!("the failed rollback was hidden behind {message:?}")
        }
        Err(_) => Ok(()),
        Ok(()) => panic!("an error ended as success"),
    }
}

/// The S3 client refuses a location whose endpoint isn't HTTPS before it
/// loads credentials, unless it was built to allow plaintext endpoints.
#[tokio::test(flavor = "current_thread")]
async fn the_s3_client_refuses_plaintext_endpoints_unless_allowed() -> TestResult {
    let plaintext = target(
        "s3://templates-bucket/space/templates/seed/sha256:abababababababababababababababababababababababababababababababab?region=us-east-1&endpoint=http://object-store:8333&path_style=true",
    )?;
    for client in [S3ReleaseObjects::new(), S3ReleaseObjects::default()] {
        assert!(matches!(
            client.read_metadata(&plaintext).await,
            Err(ObjectAccessError::PlaintextEndpoint { bucket }) if bucket == "templates-bucket"
        ));
        assert!(matches!(
            client.read_manifest(&plaintext).await,
            Err(ObjectAccessError::PlaintextEndpoint { .. })
        ));
        assert!(matches!(
            client
                .write_twin_metadata(&plaintext, environment(1), Bytes::from_static(b"{}"))
                .await,
            Err(ObjectAccessError::PlaintextEndpoint { .. })
        ));
    }
    let client = S3ReleaseObjects::new().allowing_endpoint_overrides();
    for location in [
        LOCATION.to_owned(),
        format!("{LOCATION}&endpoint=https://object-store.example:8443"),
        format!("{LOCATION}&endpoint=HTTPS://object-store.example:8443"),
    ] {
        let target = target(&location)?;
        client.admit(&target)?;
    }
    S3ReleaseObjects::allowing_plaintext_endpoints().admit(&plaintext)?;
    Ok(())
}

/// With no trusted buckets configured, a client refuses a location that
/// overrides the endpoint, unless it was built to allow overrides.
#[tokio::test(flavor = "current_thread")]
async fn the_s3_client_refuses_endpoint_overrides_unless_allowed() -> TestResult {
    let overridden = target(&format!(
        "{LOCATION}&endpoint=https://object-store.example:8443"
    ))?;
    let client = S3ReleaseObjects::new();
    client.admit(&target(LOCATION)?)?;
    assert!(matches!(
        client.read_metadata(&overridden).await,
        Err(ObjectAccessError::UntrustedOrigin {
            reason: StorageOriginRejection::EndpointOverride,
            ..
        })
    ));
    assert!(matches!(
        client.read_manifest(&overridden).await,
        Err(ObjectAccessError::UntrustedOrigin { .. })
    ));
    assert!(matches!(
        client
            .write_twin_metadata(&overridden, environment(1), Bytes::from_static(b"{}"))
            .await,
        Err(ObjectAccessError::UntrustedOrigin { .. })
    ));
    S3ReleaseObjects::new()
        .allowing_endpoint_overrides()
        .admit(&overridden)?;
    Ok(())
}

/// A client built with trusted buckets refuses a location in another bucket,
/// or one that overrides the endpoint.
#[tokio::test(flavor = "current_thread")]
async fn the_s3_client_refuses_origins_its_trusted_buckets_refuse() -> TestResult {
    let client = S3ReleaseObjects::allowing_plaintext_endpoints()
        .trusting(TrustedStorageBuckets::from_list("templates-bucket"));
    client.admit(&target(LOCATION)?)?;
    for (location, rejection) in [
        (
            LOCATION.replace("templates-bucket", "other-bucket"),
            StorageOriginRejection::UntrustedBucket,
        ),
        (
            format!("{LOCATION}&endpoint=https://object-store.example:8443"),
            StorageOriginRejection::EndpointOverride,
        ),
    ] {
        let target = target(&location)?;
        assert!(matches!(
            client.read_metadata(&target).await,
            Err(ObjectAccessError::UntrustedOrigin { reason, .. }) if reason == rejection
        ));
        assert!(matches!(
            client.read_manifest(&target).await,
            Err(ObjectAccessError::UntrustedOrigin { .. })
        ));
        assert!(matches!(
            client
                .write_twin_metadata(&target, environment(1), Bytes::from_static(b"{}"))
                .await,
            Err(ObjectAccessError::UntrustedOrigin { .. })
        ));
    }
    Ok(())
}

/// A location whose endpoint or bucket carries credentials, tokens or a
/// malformed URL is refused at parse, so secrets never reach a target, its
/// errors or its `Debug`.
#[test]
fn a_location_carrying_credentials_is_refused() {
    for location in [
        format!("{LOCATION}&endpoint=https://user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint= https://user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=http_s://user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https://user:pa/ss@object-store.example"),
        format!("{LOCATION}&endpoint=https://user:8080/hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https:///user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https:/user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https:user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https:\\\\user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=//user:hunter2@object-store.example"),
        format!("{LOCATION}&endpoint=https://user:hunter2@ho st"),
        format!("{LOCATION}&endpoint=user:hunter2@object-store.example/path"),
        format!("{LOCATION}&endpoint=user:hunter2@object-store.example?next=http://other"),
        format!("{LOCATION}&endpoint=https://object-store.example?token=hunter2"),
        format!("{LOCATION}&endpoint=https://object-store.example#hunter2"),
        format!("{LOCATION}&endpoint=ftp://object-store.example"),
        "s3://user:hunter2@templates-bucket/seed?region=us-east-1".to_owned(),
        "s3://templates-bucket:hunter2/seed?region=us-east-1".to_owned(),
        "s3://templates-bucket#hunter2?region=us-east-1".to_owned(),
        "s3://templates-bucket/seed?region=us-east-1:hunter2".to_owned(),
        "s3://templates-bucket/seed?region=us-east-1#hunter2".to_owned(),
        format!(
            "{LOCATION}&endpoint=http://user:hunter2@object-store.example&endpoint=https://object-store.example"
        ),
        format!("{LOCATION}&region=us-east-1:hunter2&region=us-east-1"),
        format!("{LOCATION}&path_style=false&path_style=true"),
        format!("{LOCATION}&&endpoint=https://object-store.example"),
    ] {
        let error = target(&location).expect_err(&location);
        assert!(
            matches!(error, ObjectAccessError::Location { .. }),
            "{location}"
        );
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }
    target(&format!(
        "{LOCATION}&endpoint=https://object-store.example:8443/prefix"
    ))
    .expect("a clean https endpoint with port and path prefix parses");
}

/// A twin's metadata goes only into its release's own build directory, under
/// either spelling of the hash. A location in another template's or another
/// build's directory, or in a directory every build of the template shares,
/// still reads, but takes no twin metadata.
#[tokio::test(flavor = "current_thread")]
async fn twin_metadata_goes_only_into_the_releases_own_build_directory() -> TestResult {
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    for location in [
        LOCATION.to_owned(),
        format!("s3://templates-bucket/templates/{TEMPLATE}/{bare}?region=us-east-1"),
    ] {
        target(&location)?.twin_metadata_key(environment(1))?;
        TwinMetadataTarget::parse(&location, TEMPLATE, bare)?.twin_metadata_key(environment(1))?;
    }

    let other_hash = format!("sha256:{}", "cd".repeat(32));
    let blake3_hash = format!("blake3:{bare}");
    let store = RecordingStore::default();
    let memory = test_support::MemoryReleaseObjects::default();
    let s3 = S3ReleaseObjects::new();
    for (location, name, hash) in [
        (
            format!("s3://templates-bucket/space/templates/other/{HASH}?region=us-east-1"),
            TEMPLATE,
            HASH,
        ),
        (
            format!(
                "s3://templates-bucket/space/templates/{TEMPLATE}/{other_hash}?region=us-east-1"
            ),
            TEMPLATE,
            HASH,
        ),
        (
            format!("s3://templates-bucket/space/notemplates/{TEMPLATE}/{HASH}?region=us-east-1"),
            TEMPLATE,
            HASH,
        ),
        (
            format!(
                "s3://templates-bucket/templates/other/{other_hash}/../../../templates/{TEMPLATE}/{HASH}?region=us-east-1"
            ),
            TEMPLATE,
            HASH,
        ),
        (
            "s3://templates-bucket/space/templates/seed/..?region=us-east-1".to_owned(),
            TEMPLATE,
            "sha256:..",
        ),
        (
            "s3://templates-bucket/space/templates/seed/.?region=us-east-1".to_owned(),
            TEMPLATE,
            ".",
        ),
        (
            format!("s3://templates-bucket/space/templates/../{HASH}?region=us-east-1"),
            "..",
            HASH,
        ),
        (
            format!("s3://templates-bucket/space/templates/./{HASH}?region=us-east-1"),
            ".",
            HASH,
        ),
        (
            format!(
                "s3://templates-bucket/space/templates/{TEMPLATE}/sha256:{blake3_hash}?region=us-east-1"
            ),
            TEMPLATE,
            blake3_hash.as_str(),
        ),
        (
            format!("s3://templates-bucket/{TEMPLATE}/{HASH}/extra?region=us-east-1"),
            TEMPLATE,
            HASH,
        ),
        (
            format!("s3://templates-bucket/{TEMPLATE}?region=us-east-1"),
            TEMPLATE,
            HASH,
        ),
        (
            "s3://templates-bucket?region=us-east-1".to_owned(),
            TEMPLATE,
            HASH,
        ),
    ] {
        let target = TwinMetadataTarget::parse(&location, name, hash)?;
        assert!(!target.shared_metadata_key().is_empty());
        assert!(
            matches!(
                objects::write_twin_metadata(
                    &store,
                    &credentials(),
                    &target,
                    environment(1),
                    Bytes::from_static(b"{}"),
                )
                .await,
                Err(ObjectAccessError::NotReleaseDirectory { ref bucket }) if bucket == "templates-bucket"
            ),
            "{location}"
        );
        assert!(
            matches!(
                s3.write_twin_metadata(&target, environment(1), Bytes::from_static(b"{}"))
                    .await,
                Err(ObjectAccessError::NotReleaseDirectory { ref bucket }) if bucket == "templates-bucket"
            ),
            "{location}"
        );
        assert!(
            matches!(
                memory
                    .write_twin_metadata(&target, environment(1), Bytes::from_static(b"{}"))
                    .await,
                Err(ObjectAccessError::NotReleaseDirectory { .. })
            ),
            "{location}"
        );
    }
    assert!(store.objects.lock().expect("lock").is_empty());
    assert!(memory.writes().is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn has_encrypted_storage_locations_reports_only_encrypted_sidecars() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    assert!(!has_encrypted_storage_locations(&db.pool, Uuid::now_v7()).await?);

    let org_id = Uuid::now_v7();
    let master_key_id = Uuid::now_v7();
    sqlx::query("INSERT INTO organizations (id, name, slug) VALUES ($1, 'Enc Org', 'enc-org')")
        .bind(org_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier)
         VALUES ($1, $2, 'in_memory', 'test-key')",
    )
    .bind(master_key_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "INSERT INTO templates (id, name, org_id, visibility)
         VALUES ($1, 'enc-seed', $2, 'org_specific')",
    )
    .bind(Uuid::now_v7())
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    let release: Uuid = sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash, org_id)
         VALUES ('enc-seed', 'fc', $1, $2)
         RETURNING id",
    )
    .bind(HASH)
    .bind(org_id)
    .fetch_one(&db.pool)
    .await?;
    let location_id: Uuid = sqlx::query_scalar(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         VALUES ($1, '', $2)
         RETURNING id",
    )
    .bind(release)
    .bind(org_id)
    .fetch_one(&db.pool)
    .await?;
    let plain_release = seeded(&db.pool, None).await?;
    assert!(!has_encrypted_storage_locations(&db.pool, release).await?);
    assert!(!has_encrypted_storage_locations(&db.pool, plain_release).await?);

    sqlx::query(
        "INSERT INTO template_release_storage_location_encrypted_fields
             (template_release_storage_location_id, org_id, master_key_id, wrapped_dek, storage_connection_string)
         VALUES ($1, $2, $3, '\\x00', '\\x00')",
    )
    .bind(location_id)
    .bind(org_id)
    .bind(master_key_id)
    .execute(&db.pool)
    .await?;
    assert!(has_encrypted_storage_locations(&db.pool, release).await?);
    assert!(!has_encrypted_storage_locations(&db.pool, plain_release).await?);
    Ok(())
}

/// An older location-less row under the same hash spelling doesn't steal
/// `is_original` from the earliest storage-backed release: fallback still finds
/// the storage-backed original before `e0` has a policy state, and once `e0`
/// is `allowed` it stays in the original tier behind a `deprecated` twin.
#[tokio::test(flavor = "current_thread")]
async fn an_older_artifactless_same_spelling_row_keeps_the_original_tier() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2, e3) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
    );
    catalog(&db.pool, e0, &[1, 15]).await?;
    catalog(&db.pool, e1, &[1, 17]).await?;
    catalog(&db.pool, e2, &[1, 18]).await?;
    catalog(&db.pool, e3, &[1, 19]).await?;
    insert_template(&db.pool, TEMPLATE).await?;

    insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e2))).await?;
    let original = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(e0))).await?;
    insert_location(&db.pool, original, LOCATION).await?;

    set_state(&db.pool, e2, "allowed").await?;
    set_default(&db.pool, e2).await?;
    assert_eq!(
        resolved_release(&db.pool, TEMPLATE, HASH, e0)
            .await?
            .map(|(id, _)| id),
        Some(original),
        "storage-backed original keeps fallback rank before e0 has a state"
    );

    set_state(&db.pool, e0, "allowed").await?;
    let twin_e1 = twin_of(&db.pool, original, e1).await?;
    set_state(&db.pool, e1, "deprecated").await?;
    set_state(&db.pool, e3, "allowed").await?;
    set_default(&db.pool, e3).await?;
    assert_eq!(
        resolved_release(&db.pool, TEMPLATE, HASH, e0)
            .await?
            .map(|(id, _)| id),
        Some(twin_e1),
        "allowed e0 original stays at original rank behind deprecated e1 twin"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn partition_selector_and_memory_release_objects_match_contracts() -> TestResult {
    let (e0, e1) = (environment(0), environment(1));
    let (e0_str, e1_str) = (e0.to_string(), e1.to_string());
    assert!(bound(e0).contains(Some(e0_str.as_str())));
    assert!(!bound(e0).contains(Some(e1_str.as_str())));
    assert!(!bound(e0).contains(None));
    let with_unbound = PartitionSelector {
        environment: e0,
        includes_unbound: true,
    };
    assert!(with_unbound.contains(Some(e0_str.as_str())));
    assert!(!with_unbound.contains(Some(e1_str.as_str())));
    assert!(with_unbound.contains(None));

    let prefixed = target(LOCATION)?;
    assert_eq!(
        prefixed.manifest_key(),
        format!("space/templates/{TEMPLATE}/{HASH}/manifest.json")
    );
    let root = TwinMetadataTarget::parse("s3://templates-bucket?region=us-east-1", TEMPLATE, HASH)?;
    assert_eq!(root.shared_metadata_key(), "metadata.json");
    assert_eq!(root.manifest_key(), "manifest.json");

    let memory = test_support::MemoryReleaseObjects::default();
    memory.put(
        prefixed.bucket(),
        &prefixed.shared_metadata_key(),
        Bytes::from_static(b"meta"),
    );
    memory.put(
        prefixed.bucket(),
        &prefixed.manifest_key(),
        Bytes::from_static(b"manifest"),
    );
    assert_eq!(memory.read_metadata(&prefixed).await?, b"meta".to_vec());
    assert_eq!(memory.read_manifest(&prefixed).await?, b"manifest".to_vec());
    memory
        .write_twin_metadata(&prefixed, e1, Bytes::from_static(b"twin"))
        .await?;
    let twin_key = prefixed.twin_metadata_key(e1)?;
    assert_eq!(memory.writes(), vec![twin_key.clone()]);
    assert_eq!(
        memory.get(prefixed.bucket(), &twin_key),
        Some(Bytes::from_static(b"twin"))
    );
    memory.refuse_writes_to(prefixed.bucket());
    assert!(matches!(
        memory
            .write_twin_metadata(&prefixed, e1, Bytes::from_static(b"twin-2"))
            .await,
        Err(ObjectAccessError::Store(_))
    ));
    Ok(())
}

#[tokio::test]
async fn a_release_is_published_once_a_storage_location_is_registered() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let template = "storage-vend-release";
    let hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse()?;
    let unregistered: ContentHash = format!("sha256:{}", "cd".repeat(32)).parse()?;
    insert_template(&db.pool, template).await?;
    let release = insert_release(&db.pool, template, &hash.to_string(), None).await?;

    // A release row without artifacts has not been published yet.
    assert!(!release_published(&db.pool, template, &hash).await?);

    insert_location(
        &db.pool,
        release,
        "s3://artifacts/templates/storage-vend-release/x?region=us-east-1",
    )
    .await?;
    assert!(release_published(&db.pool, template, &hash).await?);
    assert!(!release_published(&db.pool, template, &unregistered).await?);

    // A release registered before the `sha256:` prefix was required is stored
    // bare, and is just as published.
    let bare_hex = "ef".repeat(32);
    let bare_release = insert_release(&db.pool, template, &bare_hex, None).await?;
    insert_location(
        &db.pool,
        bare_release,
        "s3://artifacts/templates/storage-vend-release/y?region=us-east-1",
    )
    .await?;
    let legacy: ContentHash = format!("sha256:{bare_hex}").parse()?;
    assert!(release_published(&db.pool, template, &legacy).await?);
    Ok(())
}

/// Widening an org-specific template whose content is wrapped under its
/// owner's key to `generic` would make it launchable-but-undecryptable for
/// every other org — the RFC forbids it and defers re-keying. The guard
/// must report the per-template encryption sidecar, or one on a storage
/// location of any of its releases, so the handler refuses the widen; a
/// template without either is safe to widen, plaintext locations included.
#[tokio::test(flavor = "current_thread")]
async fn widen_guard_detects_owner_encryption() -> TestResult {
    // Seeded by the baseline migration's fixture data.
    const ORG_A: Uuid = Uuid::from_u128(0x1);
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for name in [
        "plain-owned",
        "encrypted-owned",
        "plain-location-owned",
        "encrypted-location-owned",
    ] {
        sqlx::query(
            "INSERT INTO templates (id, name, org_id, visibility)
             VALUES ($1, $2, $3, 'org_specific')",
        )
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(ORG_A)
        .execute(pool)
        .await?;
    }

    let key_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier)
         VALUES ($1, $2, 'test', 'k')",
    )
    .bind(key_id)
    .bind(ORG_A)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_encrypted_fields
             (template_name, org_id, master_key_id, wrapped_dek, network_policy_json)
         VALUES ('encrypted-owned', $1, $2, '\\x00'::BYTEA, '\\x00'::BYTEA)",
    )
    .bind(ORG_A)
    .bind(key_id)
    .execute(pool)
    .await?;
    for name in ["plain-location-owned", "encrypted-location-owned"] {
        let release: Uuid = sqlx::query_scalar(
            "INSERT INTO template_releases (template_name, type, hash, org_id)
             VALUES ($1, 'fc', $2, $3)
             RETURNING id",
        )
        .bind(name)
        .bind(HASH)
        .bind(ORG_A)
        .fetch_one(pool)
        .await?;
        let location_id: Uuid = sqlx::query_scalar(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string, org_id)
             VALUES ($1, '', $2)
             RETURNING id",
        )
        .bind(release)
        .bind(ORG_A)
        .fetch_one(pool)
        .await?;
        if name == "encrypted-location-owned" {
            sqlx::query(
                "INSERT INTO template_release_storage_location_encrypted_fields
                     (template_release_storage_location_id, org_id, master_key_id, wrapped_dek,
                      storage_connection_string)
                 VALUES ($1, $2, $3, '\\x00', '\\x00')",
            )
            .bind(location_id)
            .bind(ORG_A)
            .bind(key_id)
            .execute(pool)
            .await?;
        }
    }

    assert!(
        !template_has_owner_encryption(pool, "plain-owned").await?,
        "a template with no encryption sidecar is safe to widen"
    );
    assert!(
        !template_has_owner_encryption(pool, "plain-location-owned").await?,
        "a plaintext storage location is safe to widen"
    );
    assert!(
        template_has_owner_encryption(pool, "encrypted-location-owned").await?,
        "an encrypted storage location must block the widen"
    );
    assert!(
        template_has_owner_encryption(pool, "encrypted-owned").await?,
        "a per-template encryption sidecar must block the widen"
    );
    assert!(
        !template_has_owner_encryption(pool, "absent").await?,
        "an unknown template has no sidecar"
    );
    Ok(())
}

/// Registration's fills write only an empty field and say whether it was empty.
#[tokio::test(flavor = "current_thread")]
async fn registration_fills_only_a_missing_binding_or_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert_template(&db.pool, TEMPLATE).await?;
    let release = insert_release(&db.pool, TEMPLATE, HASH, None).await?;
    let first = metadata(environment(1));
    assert!(registration::fill_execution_metadata(&db.pool, release, &first).await?);
    assert!(
        !registration::fill_execution_metadata(&db.pool, release, &metadata(environment(2)))
            .await?
    );
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT execution_metadata FROM template_releases WHERE id = $1")
            .bind(release)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(stored, Some(first));

    sqlx::query("UPDATE templates SET org_id = $2, visibility = 'org_specific' WHERE name = $1")
        .bind(TEMPLATE)
        .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
        .execute(&db.pool)
        .await?;
    sqlx::query("UPDATE template_releases SET org_id = $2 WHERE id = $1")
        .bind(release)
        .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
        .execute(&db.pool)
        .await?;
    insert_location(&db.pool, release, LOCATION).await?;
    let location: Uuid = sqlx::query_scalar(
        "SELECT id FROM template_release_storage_locations WHERE template_release_id = $1",
    )
    .bind(release)
    .fetch_one(&db.pool)
    .await?;
    assert!(registration::adopt_storage_location_owner(&db.pool, location).await?);
    assert!(!registration::adopt_storage_location_owner(&db.pool, location).await?);
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT org_id FROM template_release_storage_locations WHERE id = $1")
            .bind(location)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(owner, Some(pplx_space_db_test_fixtures::SEED_ORG_ID));
    Ok(())
}

/// Whether another transaction can take `lock` on release `release` now.
async fn release_lock_free(pool: &PgPool, release: Uuid, lock: &str) -> TestResult<bool> {
    let mut other = pool.begin().await?;
    let result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM template_releases WHERE id = $1 FOR {lock} NOWAIT"
    )))
    .bind(release)
    .execute(&mut *other)
    .await;
    other.rollback().await?;
    match result {
        Ok(_) => Ok(true),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn release_locks_last_until_the_callers_transaction_ends() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let bound = metadata(environment(1));
    let original = seeded(&db.pool, Some(bound.clone())).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let other_spelling = insert_release(&db.pool, TEMPLATE, bare, None).await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(
        registration::lock_execution_metadata(&mut tx, original).await?,
        Some(bound)
    );
    assert!(!release_lock_free(&db.pool, original, "KEY SHARE").await?);
    assert!(release_lock_free(&db.pool, other_spelling, "UPDATE").await?);
    tx.rollback().await?;
    let mut tx = db.pool.begin().await?;
    assert!(matches!(
        registration::lock_execution_metadata(&mut tx, Uuid::now_v7()).await,
        Err(sqlx::Error::RowNotFound)
    ));
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    let release = lookup::lock_release_of_present_template(&mut tx, original)
        .await?
        .expect("a present template's release");
    assert_eq!(
        (release.id, release.content_hash.as_str()),
        (original, HASH)
    );
    assert!(!release_lock_free(&db.pool, original, "KEY SHARE").await?);
    let mut other = db.pool.begin().await?;
    sqlx::query("SELECT 1 FROM templates WHERE name = $1 FOR UPDATE NOWAIT")
        .bind(TEMPLATE)
        .execute(&mut *other)
        .await?;
    other.rollback().await?;
    tx.rollback().await?;
    sqlx::query("UPDATE templates SET desired_state = 'deleting' WHERE name = $1")
        .bind(TEMPLATE)
        .execute(&db.pool)
        .await?;
    let mut tx = db.pool.begin().await?;
    assert!(
        lookup::lock_release_of_present_template(&mut tx, original)
            .await?
            .is_none()
    );
    assert!(release_lock_free(&db.pool, original, "UPDATE").await?);
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    removal::lock_releases_of_digest(&mut tx, other_spelling).await?;
    assert!(!release_lock_free(&db.pool, original, "KEY SHARE").await?);
    assert!(!release_lock_free(&db.pool, other_spelling, "KEY SHARE").await?);
    tx.rollback().await?;
    assert!(release_lock_free(&db.pool, original, "UPDATE").await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn original_lookups_and_the_twin_check_follow_the_shared_rule() -> TestResult {
    let Some(db) = twin_ready_db().await? else {
        return Ok(());
    };
    let original = seeded(&db.pool, None).await?;
    let bare = HASH.strip_prefix("sha256:").expect("prefixed");
    let other_spelling = insert_release(&db.pool, TEMPLATE, bare, None).await?;
    for hash in [HASH, bare] {
        assert_eq!(
            lookup::original_release_id(&db.pool, TEMPLATE, hash).await?,
            Some(original),
            "the original that owns a location wins under either spelling"
        );
    }
    assert_eq!(
        lookup::original_release_id(&db.pool, "other", HASH).await?,
        None
    );
    let registered = registration::original_release(&db.pool, TEMPLATE, HASH, bare)
        .await?
        .expect("the digest's original");
    assert_eq!((registered.id, registered.hash.as_str()), (original, HASH));
    assert!(
        registration::original_release(&db.pool, "other", HASH, bare)
            .await?
            .is_none()
    );

    assert!(!lookup::release_serves_a_tag(&db.pool, original).await?);
    pplx_space_template_store::test_support::tag_release(
        &db.pool,
        TEMPLATE,
        other_spelling,
        "stable",
    )
    .await?;
    assert!(
        lookup::release_serves_a_tag(&db.pool, original).await?,
        "a tag on the other spelling resolves to the digest original"
    );
    pplx_space_template_store::test_support::tag_release(&db.pool, TEMPLATE, original, "pinned")
        .await?;
    assert!(
        !lookup::release_serves_a_tag(&db.pool, other_spelling).await?,
        "a non-original serves neither its own non-latest tag nor a tag on another release of the digest"
    );
    pplx_space_template_store::test_support::tag_release(
        &db.pool,
        TEMPLATE,
        other_spelling,
        "latest",
    )
    .await?;
    assert!(lookup::release_serves_a_tag(&db.pool, other_spelling).await?);

    assert!(!lookup::release_has_twins(&db.pool, original).await?);
    let twin = insert_release(&db.pool, TEMPLATE, HASH, Some(metadata(environment(2)))).await?;
    assert!(lookup::release_has_twins(&db.pool, original).await?);
    assert!(
        !lookup::release_has_twins(&db.pool, twin).await?,
        "a twin is no original"
    );
    assert!(
        !lookup::release_has_twins(&db.pool, other_spelling).await?,
        "the other spelling's original has no twin of its own"
    );
    Ok(())
}
