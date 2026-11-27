//! Postgres tests for the original-release rule.
//!
//! The model test checks [`digest_original`] and [`spelling_original`] against
//! a direct Rust spelling of the rule the crate documents, over generated
//! fixtures with both spellings, twins, storage locations and `created_at`
//! ties. The script tests run `scripts/bootstrap-templates.sh` against a
//! migrated database and check that it attaches its tag and storage location
//! to the release the fragment picks.

use super::{digest_original, spelling_original};
use pplx_space_db_test_fixtures::{report_unavailable_dependency, try_pg_test_db};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BARE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OLD_INDEX: &str = "idx_template_releases_template_name_hash_unique";

/// One release of a fixture: which spelling it's stored under, its
/// `created_at` in seconds past a fixed epoch, and whether it owns a storage
/// location.
#[derive(Clone, Copy, Debug)]
struct Seeded {
    canonical: bool,
    second: i64,
    storage: bool,
}

#[derive(Debug)]
struct ModelRow {
    id: Uuid,
    canonical: bool,
    second: i64,
    storage: bool,
}

impl ModelRow {
    fn key(&self) -> (i64, Uuid) {
        (self.second, self.id)
    }
}

/// The rule as the crate documents it, over one fixture's rows.
fn model(rows: &[ModelRow]) -> HashMap<Uuid, (bool, bool)> {
    let spelling_original = |row: &ModelRow| {
        !rows
            .iter()
            .any(|other| other.canonical == row.canonical && other.key() < row.key())
    };
    rows.iter()
        .map(|row| {
            let spelling = spelling_original(row);
            let outranked = rows.iter().any(|other| {
                other.canonical != row.canonical
                    && spelling_original(other)
                    && ((other.storage && !row.storage)
                        || (other.storage == row.storage && other.key() < row.key()))
            });
            (row.id, (spelling, spelling && !outranked))
        })
        .collect()
}

/// A small deterministic generator, so a failure names a reproducible case.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

fn release_id(fixture: usize, index: usize, scramble: u64) -> Uuid {
    // Version 7 shape, with the low bits scrambled so `id` order differs from
    // insertion order and a `created_at` tie is broken by something other
    // than the order the rows went in.
    let low = (u128::from(scramble) << 16) | ((fixture as u128) << 4) | index as u128;
    Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | (low & 0xffff_ffff_ffff))
}

async fn insert_template(pool: &PgPool, name: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name) VALUES (space_uuid_v7(), $1)"
    ))
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_release(
    pool: &PgPool,
    id: Uuid,
    template: &str,
    hash: &str,
    second: i64,
    environment: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at, execution_metadata)
         VALUES ($1, $2, 'fc', $3,
                 TIMESTAMPTZ '2026-01-01 00:00:00+00' + $4 * INTERVAL '1 second',
                 jsonb_build_object('environment', $5::TEXT))"
    ))
    .bind(id)
    .bind(template)
    .bind(hash)
    .bind(second)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_storage(pool: &PgPool, release: Uuid, uri: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(release)
    .bind(uri)
    .execute(pool)
    .await?;
    Ok(())
}

/// Both fragments over every row of `template`: `(id, spelling, digest)`.
async fn evaluate(pool: &PgPool, template: &str) -> TestResult<Vec<(Uuid, bool, bool)>> {
    let statement = format!(
        "SELECT r.id, {spelling} AS spelling, {digest} AS digest
         FROM template_releases r
         WHERE r.template_name = $1",
        spelling = spelling_original("r"),
        digest = digest_original("r"),
    );
    Ok(
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement.as_str())))
            .bind(template)
            .fetch_all(pool)
            .await?,
    )
}

/// Every generated fixture has exactly one digest original, and both
/// fragments agree with the documented rule row for row. The fixtures cover
/// a digest under one spelling or both, each with up to three releases (an
/// original and twins), any subset owning storage locations, and
/// `created_at` ties that only `id` breaks.
#[tokio::test(flavor = "current_thread")]
async fn the_fragments_match_the_documented_rule() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "DROP INDEX IF EXISTS {OLD_INDEX}"
    ))))
    .execute(pool)
    .await?;

    let mut rng = Lcg(0x5eed);
    let mut fixtures = Vec::new();
    for fixture in 0..240 {
        let template = format!("fixture-{fixture}");
        insert_template(pool, &template).await?;
        let mut seeded = Vec::new();
        for canonical in [false, true] {
            // Of each three fixtures, one draws canonical releases only, one bare
            // releases only, and one at least one of each.
            let count = match (fixture % 3, canonical) {
                (0, true) | (1, false) => 0,
                (2, _) => 1 + rng.next(3),
                _ => rng.next(4),
            };
            for _ in 0..count {
                seeded.push(Seeded {
                    canonical,
                    // A narrow range, so ties on `created_at` are common.
                    second: i64::try_from(rng.next(3))?,
                    storage: rng.next(2) == 1,
                });
            }
        }
        let mut rows = Vec::new();
        for (index, seed) in seeded.iter().enumerate() {
            let id = release_id(fixture, index, rng.next(1 << 20));
            let hash = if seed.canonical {
                format!("sha256:{BARE}")
            } else {
                BARE.to_owned()
            };
            insert_release(
                pool,
                id,
                &template,
                &hash,
                seed.second,
                &format!("e{index}"),
            )
            .await?;
            if seed.storage {
                insert_storage(pool, id, &format!("s3://bucket/{template}/{index}")).await?;
            }
            rows.push(ModelRow {
                id,
                canonical: seed.canonical,
                second: seed.second,
                storage: seed.storage,
            });
        }
        fixtures.push((template, rows));
    }

    let mut both_spellings = 0;
    let mut outranked_by_storage = 0;
    for (template, rows) in &fixtures {
        if rows.is_empty() {
            continue;
        }
        let expected = model(rows);
        let actual = evaluate(pool, template).await?;
        assert_eq!(actual.len(), rows.len(), "{template}");
        for (id, spelling, digest) in &actual {
            assert_eq!(
                Some(&(*spelling, *digest)),
                expected.get(id),
                "{template}: release {id} in {rows:?}"
            );
        }
        let originals = actual.iter().filter(|(_, _, digest)| *digest).count();
        assert_eq!(originals, 1, "{template}: {rows:?}");
        if rows.iter().any(|row| row.canonical) && rows.iter().any(|row| !row.canonical) {
            both_spellings += 1;
            let older = rows
                .iter()
                .filter(|row| expected[&row.id].0)
                .min_by_key(|row| row.key())
                .map(|row| row.id);
            let chosen = actual
                .iter()
                .find(|(_, _, digest)| *digest)
                .map(|row| row.0);
            if older != chosen {
                outranked_by_storage += 1;
            }
        }
    }
    // The generator has to reach the cases the rule exists for.
    assert!(
        both_spellings > 50,
        "{both_spellings} fixtures with both spellings"
    );
    assert!(
        outranked_by_storage > 5,
        "{outranked_by_storage} fixtures where storage beat age"
    );
    Ok(())
}

#[test]
#[should_panic(expected = "must be a lowercase SQL identifier")]
fn an_alias_that_collides_with_the_fragment_is_refused() {
    let _ = digest_original("digest_rival");
}

#[test]
#[should_panic(expected = "must be a lowercase SQL identifier")]
fn an_alias_that_isnt_an_identifier_is_refused() {
    let _ = spelling_original("r; DROP TABLE templates");
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../..")
}

/// Runs `scripts/bootstrap-templates.sh` for one template whose manifest
/// names `content_hash`, with `aws` and `psql` replaced by shims on `PATH`:
/// the `aws` shim serves the manifest, and the `psql` shim records its
/// arguments and the SQL on its standard input. The recorded SQL then runs
/// against `pool` with the script's `-v` variables interpolated the way
/// `psql` does: `:'name'` as a quoted literal and `:name` verbatim.
///
/// Returns `false` when `jq` or `bash` is missing and the run opted out of
/// required test dependencies.
async fn run_bootstrap(
    pool: &PgPool,
    work: &Path,
    template: &str,
    content_hash: &str,
) -> TestResult<bool> {
    for tool in ["bash", "jq"] {
        let found = std::process::Command::new("sh")
            .args(["-c", &format!("command -v {tool}")])
            .output()?
            .status
            .success();
        if !found {
            report_unavailable_dependency(tool, &"the bootstrap script needs it")?;
            return Ok(false);
        }
    }
    let shims = work.join("bin");
    std::fs::create_dir_all(&shims)?;
    let capture = work.join("capture");
    std::fs::create_dir_all(&capture)?;
    let write_shim = |name: &str, body: &str| -> std::io::Result<()> {
        let path = shims.join(name);
        std::fs::write(&path, body)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
    };
    write_shim(
        "aws",
        "#!/usr/bin/env bash\n\
         case \"$1 $2\" in\n\
           's3 ls') exit 0 ;;\n\
           's3 cp') printf '{\"content_hash\":\"%s\"}\\n' \"$FAKE_CONTENT_HASH\" > \"$4\" ;;\n\
           *) exit 1 ;;\n\
         esac\n",
    )?;
    write_shim(
        "psql",
        "#!/usr/bin/env bash\n\
         printf '%s\\0' \"$@\" > \"$PSQL_CAPTURE/args\"\n\
         cat > \"$PSQL_CAPTURE/stdin\"\n",
    )?;
    let path = format!(
        "{}:{}",
        shims.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = tokio::process::Command::new("bash")
        .arg(repo_root().join("scripts/bootstrap-templates.sh"))
        .env("PATH", path)
        .env("TEMPLATE_BOOTSTRAP_BUCKET", "bucket")
        .env("TEMPLATE_BOOTSTRAP_NAMES", template)
        .env("POSTGRES_CONNECTION_STRING", "postgres://unused")
        .env("FAKE_CONTENT_HASH", content_hash)
        .env("PSQL_CAPTURE", &capture)
        .output()
        .await?;
    assert!(
        output.status.success(),
        "bootstrap-templates.sh failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let args = std::fs::read(capture.join("args"))?;
    let args: Vec<String> = args
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .map(|arg| String::from_utf8(arg.to_vec()))
        .collect::<Result<_, _>>()?;
    let mut variables = Vec::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "-v" {
            let assignment = args.next().ok_or("-v without a value")?;
            let (name, value) = assignment.split_once('=').ok_or("-v without =")?;
            variables.push((name.to_owned(), value.to_owned()));
        }
    }
    let mut statement = std::fs::read_to_string(capture.join("stdin"))?;
    // Longest names first, so `:hash` never matches inside `:hash_other`.
    variables.sort_by_key(|(name, _)| std::cmp::Reverse(name.len()));
    for (name, value) in &variables {
        statement = statement.replace(
            &format!(":'{name}'"),
            &format!("'{}'", value.replace('\'', "''")),
        );
    }
    for (name, value) in &variables {
        statement = statement.replace(&format!(":{name}"), value);
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await?;
    Ok(true)
}

/// What the script left for `template`: the release `latest` points at, the
/// releases owning each storage location, the digest original, and every
/// release's stored hash.
#[derive(Debug)]
struct Registered {
    latest: Option<Uuid>,
    storage_owners: Vec<(String, Uuid)>,
    digest_original: Vec<Uuid>,
    hashes: Vec<String>,
}

async fn registered(pool: &PgPool, template: &str) -> TestResult<Registered> {
    let latest = sqlx::query_scalar(sql!(
        "SELECT template_release_id FROM template_release_tags
         WHERE template_name = $1 AND tag = 'latest'"
    ))
    .bind(template)
    .fetch_optional(pool)
    .await?;
    let storage_owners = sqlx::query_as(sql!(
        "SELECT l.storage_connection_string, l.template_release_id
         FROM template_release_storage_locations l
         JOIN template_releases r ON r.id = l.template_release_id
         WHERE r.template_name = $1
         ORDER BY l.storage_connection_string"
    ))
    .bind(template)
    .fetch_all(pool)
    .await?;
    let digest_original = evaluate(pool, template)
        .await?
        .into_iter()
        .filter(|(_, _, digest)| *digest)
        .map(|(id, _, _)| id)
        .collect();
    let hashes = sqlx::query_scalar(sql!(
        "SELECT hash FROM template_releases WHERE template_name = $1 ORDER BY created_at, id"
    ))
    .bind(template)
    .fetch_all(pool)
    .await?;
    Ok(Registered {
        latest,
        storage_owners,
        digest_original,
        hashes,
    })
}

/// A digest already stored bare, owning a storage location, gets no second
/// release when the bucket's manifest names it canonically: the script's tag
/// and location go to the bare release, the one every other caller resolves.
/// Before this, the canonical registration inserted a second, artifact-less
/// release and the internal API kept answering with the bare one.
#[tokio::test(flavor = "current_thread")]
async fn the_script_attaches_to_the_digest_original_under_the_other_spelling() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let work = tempfile::tempdir()?;
    let template = "seeded";
    insert_template(pool, template).await?;
    let bare = release_id(0, 0, 0);
    insert_release(pool, bare, template, BARE, 0, "e1").await?;
    insert_storage(pool, bare, "s3://bucket/legacy").await?;

    if !run_bootstrap(pool, work.path(), template, &format!("sha256:{BARE}")).await? {
        return Ok(());
    }
    let after = registered(pool, template).await?;
    assert_eq!(after.hashes, [BARE], "no release under the other spelling");
    assert_eq!(after.digest_original, [bare]);
    assert_eq!(after.latest, Some(bare));
    assert!(
        after.storage_owners.iter().all(|(_, owner)| *owner == bare),
        "{after:?}"
    );
    assert_eq!(after.storage_owners.len(), 2, "{after:?}");
    // The reused release takes the template's owner, as the release upsert
    // backfills one under the manifest's spelling, and so does the location
    // the script attached: publication checks both against the template.
    let (template_org, release_org, location_org): (Option<Uuid>, Option<Uuid>, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT t.org_id, r.org_id, l.org_id
             FROM templates t
             JOIN template_releases r ON r.template_name = t.name
             JOIN template_release_storage_locations l
               ON l.template_release_id = r.id
              AND l.storage_connection_string <> 's3://bucket/legacy'
             WHERE t.name = $1"
        ))
        .bind(template)
        .fetch_one(pool)
        .await?;
    assert!(template_org.is_some(), "the script owns the template");
    assert_eq!(release_org, template_org);
    assert_eq!(location_org, template_org);
    Ok(())
}

/// A digest under both spellings, where the canonical release is older but
/// owns no storage location and the bare one does: the script attaches to
/// the bare one, as the fragment picks, not to the release stored under the
/// manifest's spelling.
#[tokio::test(flavor = "current_thread")]
async fn the_script_prefers_the_storage_owning_spelling() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let work = tempfile::tempdir()?;
    let template = "both";
    insert_template(pool, template).await?;
    let canonical = release_id(0, 0, 0);
    let bare = release_id(0, 1, 0);
    insert_release(
        pool,
        canonical,
        template,
        &format!("sha256:{BARE}"),
        0,
        "e1",
    )
    .await?;
    insert_release(pool, bare, template, BARE, 1, "e1").await?;
    insert_storage(pool, bare, "s3://bucket/legacy").await?;

    if !run_bootstrap(pool, work.path(), template, &format!("sha256:{BARE}")).await? {
        return Ok(());
    }
    let after = registered(pool, template).await?;
    assert_eq!(after.digest_original, [bare]);
    assert_eq!(after.latest, Some(bare));
    assert!(
        after.storage_owners.iter().all(|(_, owner)| *owner == bare),
        "{after:?}"
    );
    Ok(())
}

/// A digest with no release yet is registered under the manifest's spelling,
/// tagged and given its location, and a second run changes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_script_registers_a_new_digest_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let work = tempfile::tempdir()?;
    let template = "fresh";
    let hash = format!("sha256:{BARE}");
    for _ in 0..2 {
        if !run_bootstrap(pool, work.path(), template, &hash).await? {
            return Ok(());
        }
    }
    let after = registered(pool, template).await?;
    assert_eq!(after.hashes, [hash.as_str()]);
    let [original] = after.digest_original[..] else {
        panic!("one digest original: {after:?}");
    };
    assert_eq!(after.latest, Some(original));
    assert_eq!(
        after.storage_owners,
        [(
            format!("s3://bucket/{template}/?region=us-east-1"),
            original
        )]
    );
    Ok(())
}

/// The shared spelling-pair fixture names the release the fragment picks,
/// with twins and without, so other crates' tests can assert against it.
#[tokio::test(flavor = "current_thread")]
async fn the_spelling_pair_fixture_names_the_digest_original() -> TestResult {
    use pplx_space_template_release_store::spelling_pairs::{
        OLD_INDEX as INDEX, seed_spelling_pairs,
    };
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_template(pool, "pairs").await?;
    let pairs = seed_spelling_pairs(pool, "pairs", None).await?;
    sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
        "DROP INDEX IF EXISTS {INDEX}"
    ))))
    .execute(pool)
    .await?;
    insert_template(pool, "pairs-with-twins").await?;
    let with_twins = seed_spelling_pairs(
        pool,
        "pairs-with-twins",
        Some(r#"{"environment": "fixture-twin"}"#),
    )
    .await?;
    for (template, pairs) in [("pairs", &pairs), ("pairs-with-twins", &with_twins)] {
        let originals = evaluate(pool, template)
            .await?
            .into_iter()
            .filter(|(_, _, digest)| *digest)
            .map(|(id, _, _)| id)
            .collect::<std::collections::HashSet<_>>();
        let expected = pairs.iter().map(|pair| pair.original).collect();
        assert_eq!(originals, expected, "{template}");
    }
    // The layouts reach both spellings as the original.
    assert!(pairs.iter().any(|pair| pair.original == pair.canonical));
    assert!(pairs.iter().any(|pair| pair.original == pair.bare));
    assert!(with_twins.iter().all(|pair| pair.twins.len() == 2));
    Ok(())
}
