//! `template_releases.first_mutable_tag_at`, which the tag verbs set in the
//! statement that writes the tag. Times are compared as text, in the
//! session's zone.

use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_template_model::TemplateTag;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::release_tags;

const TEMPLATE: &str = "seed";

async fn insert_release(pool: &PgPool, hash: &str) -> TestResult<Uuid> {
    sqlx::query("INSERT INTO templates (id, name) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING")
        .bind(Uuid::now_v7())
        .bind(TEMPLATE)
        .execute(pool)
        .await?;
    Ok(sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash)
         VALUES ($1, 'fc', $2)
         RETURNING id",
    )
    .bind(TEMPLATE)
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

fn digest(byte: &str) -> String {
    format!("sha256:{}", byte.repeat(32))
}

fn tag(value: &str) -> TestResult<TemplateTag> {
    Ok(value.parse()?)
}

/// When `tag` was last written.
async fn tagged_at(pool: &PgPool, tag: &str) -> TestResult<String> {
    Ok(sqlx::query_scalar(
        "SELECT modified_at::TEXT FROM template_release_tags
         WHERE template_name = $1 AND tag = $2",
    )
    .bind(TEMPLATE)
    .bind(tag)
    .fetch_one(pool)
    .await?)
}

async fn first_mutable_tag_at(pool: &PgPool, release: Uuid) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(
            "SELECT first_mutable_tag_at::TEXT FROM template_releases WHERE id = $1",
        )
        .bind(release)
        .fetch_one(pool)
        .await?,
    )
}

/// A release's own digest, in either spelling, doesn't set it, as
/// registration writes the digest tag for every build. The first other tag
/// does, at the time it writes the tag, and a later one leaves that time
/// alone.
#[tokio::test(flavor = "current_thread")]
async fn upsert_marks_only_a_tag_other_than_the_digest() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let release = insert_release(pool, &digest("cd")).await?;
    let bare = "cd".repeat(32);

    release_tags::upsert(pool, release, TEMPLATE, &digest("cd")).await?;
    release_tags::upsert(pool, release, TEMPLATE, &bare).await?;
    // A republication rewrites the digest tag in place.
    release_tags::upsert(pool, release, TEMPLATE, &digest("cd")).await?;
    assert_eq!(
        first_mutable_tag_at(pool, release).await?,
        None,
        "digest tags only"
    );

    release_tags::upsert(pool, release, TEMPLATE, "latest").await?;
    let first = tagged_at(pool, "latest").await?;
    assert_eq!(
        first_mutable_tag_at(pool, release).await?.as_deref(),
        Some(first.as_str())
    );
    release_tags::upsert(pool, release, TEMPLATE, "prod").await?;
    release_tags::upsert(pool, release, TEMPLATE, "latest").await?;
    assert_eq!(
        first_mutable_tag_at(pool, release).await?.as_deref(),
        Some(first.as_str()),
        "the first time stays"
    );

    // A release stored under the bare spelling is tagged with either.
    let stored_bare = insert_release(pool, &"ef".repeat(32)).await?;
    release_tags::upsert(pool, stored_bare, TEMPLATE, &"ef".repeat(32)).await?;
    release_tags::upsert(pool, stored_bare, TEMPLATE, &digest("ef")).await?;
    assert_eq!(
        first_mutable_tag_at(pool, stored_bare).await?,
        None,
        "bare digest tags only"
    );
    Ok(())
}

/// The admin tag move marks the release a tag lands on, whether it moves an
/// existing tag or creates one, and the release it left keeps its time: a
/// former `latest` has still carried a mutable tag.
#[tokio::test(flavor = "current_thread")]
async fn retarget_and_insert_if_absent_mark_the_release_they_tag() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let former = insert_release(pool, &digest("01")).await?;
    let current = insert_release(pool, &digest("02")).await?;
    let created = insert_release(pool, &digest("03")).await?;

    assert!(release_tags::insert_if_absent(pool, former, TEMPLATE, &tag("latest")?).await?);
    let first = tagged_at(pool, "latest").await?;
    assert_eq!(
        first_mutable_tag_at(pool, former).await?.as_deref(),
        Some(first.as_str())
    );

    assert!(release_tags::retarget(pool, current, TEMPLATE, &tag("latest")?).await?);
    let moved = tagged_at(pool, "latest").await?;
    assert_eq!(
        first_mutable_tag_at(pool, current).await?.as_deref(),
        Some(moved.as_str())
    );
    assert_eq!(
        first_mutable_tag_at(pool, former).await?.as_deref(),
        Some(first.as_str()),
        "moving a tag off a release keeps its time"
    );

    // Neither admin verb moves a time once set: moving `latest` back onto
    // `former` and creating a tag on `current` leave both times.
    assert!(release_tags::retarget(pool, former, TEMPLATE, &tag("latest")?).await?);
    assert!(release_tags::insert_if_absent(pool, current, TEMPLATE, &tag("canary")?).await?);
    assert_eq!(
        first_mutable_tag_at(pool, former).await?.as_deref(),
        Some(first.as_str()),
        "retarget keeps the first time"
    );
    assert_eq!(
        first_mutable_tag_at(pool, current).await?.as_deref(),
        Some(moved.as_str()),
        "insert_if_absent keeps the first time"
    );

    // A release stored bare, tagged with that same bare digest.
    let stored_bare = insert_release(pool, &"05".repeat(32)).await?;
    let own = tag(&"05".repeat(32))?;
    assert!(release_tags::insert_if_absent(pool, stored_bare, TEMPLATE, &own).await?);
    assert!(release_tags::retarget(pool, stored_bare, TEMPLATE, &own).await?);
    assert_eq!(
        first_mutable_tag_at(pool, stored_bare).await?,
        None,
        "own bare digest, stored bare"
    );

    // The admin move accepts a bare digest as a tag. On the release it names,
    // that's the release's own digest, so neither verb marks it.
    let bare = "03".repeat(32);
    assert!(release_tags::insert_if_absent(pool, created, TEMPLATE, &tag(&bare)?).await?);
    assert!(release_tags::retarget(pool, created, TEMPLATE, &tag(&bare)?).await?);
    assert_eq!(
        first_mutable_tag_at(pool, created).await?,
        None,
        "own bare digest"
    );

    // Nothing written, nothing marked: an insert that finds the tag taken and
    // a retarget of a tag the template doesn't have.
    assert!(!release_tags::insert_if_absent(pool, created, TEMPLATE, &tag("latest")?).await?);
    assert!(!release_tags::retarget(pool, created, TEMPLATE, &tag("stable")?).await?);
    assert_eq!(first_mutable_tag_at(pool, created).await?, None);

    assert!(release_tags::insert_if_absent(pool, created, TEMPLATE, &tag("stable")?).await?);
    let stable = tagged_at(pool, "stable").await?;
    assert_eq!(
        first_mutable_tag_at(pool, created).await?.as_deref(),
        Some(stable.as_str())
    );
    Ok(())
}

/// Only the release's own exact spellings are its digest: a tag that merely
/// contains `sha256:` more than once, or puts it before another algorithm's
/// hash, is a mutable tag.
#[tokio::test(flavor = "current_thread")]
async fn a_doubled_digest_prefix_is_a_mutable_tag() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let release = insert_release(pool, &digest("ab")).await?;
    let doubled = format!("sha256:{}", digest("ab"));

    release_tags::upsert(pool, release, TEMPLATE, &doubled).await?;
    assert_eq!(
        first_mutable_tag_at(pool, release).await?.as_deref(),
        Some(tagged_at(pool, &doubled).await?.as_str())
    );

    // Another algorithm has one spelling: `sha256:` before a BLAKE3 hash
    // isn't its digest, while the hash itself is.
    let blake3 = format!("blake3:{}", "ac".repeat(32));
    let blake3_release = insert_release(pool, &blake3).await?;
    release_tags::upsert(pool, blake3_release, TEMPLATE, &blake3).await?;
    assert_eq!(first_mutable_tag_at(pool, blake3_release).await?, None);
    let prefixed = format!("sha256:{blake3}");
    release_tags::upsert(pool, blake3_release, TEMPLATE, &prefixed).await?;
    assert_eq!(
        first_mutable_tag_at(pool, blake3_release).await?.as_deref(),
        Some(tagged_at(pool, &prefixed).await?.as_str())
    );
    Ok(())
}

/// Registration stamps a release's `created_at` with `clock_timestamp()` and
/// tags it in the same transaction, after the transaction's `NOW()`. The mark
/// is never before the release existed.
#[tokio::test(flavor = "current_thread")]
async fn a_release_tagged_in_its_registration_is_marked_after_it_was_created() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    sqlx::query("INSERT INTO templates (id, name) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(TEMPLATE)
        .execute(pool)
        .await?;

    let mut tx = pool.begin().await?;
    // The registration's wait for its template lock, after `NOW()`.
    sqlx::query("SELECT pg_sleep(0.05)")
        .execute(&mut *tx)
        .await?;
    let release: Uuid = sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash, created_at)
         VALUES ($1, 'fc', $2, clock_timestamp())
         RETURNING id",
    )
    .bind(TEMPLATE)
    .bind(digest("ba"))
    .fetch_one(&mut *tx)
    .await?;
    release_tags::upsert(&mut *tx, release, TEMPLATE, &digest("ba")).await?;
    release_tags::upsert(&mut *tx, release, TEMPLATE, "latest").await?;
    // The admin verbs too, each on a release created in the transaction.
    let mut created_in_tx = Vec::new();
    for byte in ["bb", "bc"] {
        created_in_tx.push(
            sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO template_releases (template_name, type, hash, created_at)
                 VALUES ($1, 'fc', $2, clock_timestamp())
                 RETURNING id",
            )
            .bind(TEMPLATE)
            .bind(digest(byte))
            .fetch_one(&mut *tx)
            .await?,
        );
    }
    assert!(
        release_tags::insert_if_absent(&mut *tx, created_in_tx[0], TEMPLATE, &tag("stable")?)
            .await?
    );
    assert!(release_tags::retarget(&mut *tx, created_in_tx[1], TEMPLATE, &tag("stable")?).await?);
    tx.commit().await?;
    for admin_tagged in created_in_tx {
        let (marked, created): (Option<String>, String) = sqlx::query_as(
            "SELECT first_mutable_tag_at::TEXT, created_at::TEXT
             FROM template_releases WHERE id = $1",
        )
        .bind(admin_tagged)
        .fetch_one(pool)
        .await?;
        assert_eq!(marked.as_deref(), Some(created.as_str()), "admin verb");
    }

    let (marked, created, began_before): (Option<String>, String, bool) = sqlx::query_as(
        "SELECT first_mutable_tag_at::TEXT, created_at::TEXT,
                (SELECT modified_at FROM template_release_tags
                  WHERE template_name = $2 AND tag = 'latest') < created_at
         FROM template_releases WHERE id = $1",
    )
    .bind(release)
    .bind(TEMPLATE)
    .fetch_one(pool)
    .await?;
    assert!(
        began_before,
        "the transaction began before the release existed"
    );
    assert_eq!(marked.as_deref(), Some(created.as_str()));
    Ok(())
}
