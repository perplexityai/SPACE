//! A shared fixture for other crates' tests: digests stored under both
//! spellings, so each caller of the original-release rule
//! (`pplx_space_template_release_original::digest_original`) can show it
//! resolves the release the rule picks. It lives here because this crate
//! owns the tables it seeds.
//!
//! Enable the `test-support` feature only from `[dev-dependencies]`.

use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

/// The unique index on `(template_name, hash)` that twins can't be inserted
/// under. [`seed_spelling_pairs`] with twins needs it dropped first.
pub const OLD_INDEX: &str = "idx_template_releases_template_name_hash_unique";

/// One digest of a [`seed_spelling_pairs`] template, stored under both
/// spellings.
#[derive(Clone, Debug)]
pub struct SpellingPair {
    /// The digest as `sha256:<hex>`.
    pub canonical_hash: String,
    /// The digest as bare `<hex>`.
    pub bare_hash: String,
    /// The original of the `sha256:<hex>` spelling.
    pub canonical: Uuid,
    /// The original of the bare spelling.
    pub bare: Uuid,
    /// The twins of both spelling originals, when seeded with twins.
    pub twins: Vec<Uuid>,
    /// The release the rule picks: [`Self::canonical`] or [`Self::bare`].
    pub original: Uuid,
}

impl SpellingPair {
    /// Every release of the digest.
    pub fn releases(&self) -> Vec<Uuid> {
        let mut releases = vec![self.canonical, self.bare];
        releases.extend(&self.twins);
        releases
    }

    /// The hash the original is stored under.
    pub fn original_hash(&self) -> &str {
        if self.original == self.canonical {
            &self.canonical_hash
        } else {
            &self.bare_hash
        }
    }

    /// The original of the spelling the rule doesn't pick.
    pub fn other(&self) -> Uuid {
        if self.original == self.canonical {
            self.bare
        } else {
            self.canonical
        }
    }
}

/// Point `tag` of `template_name` at `release`, as a tag written before the
/// shared rule could: nothing here checks that `release` is the digest
/// original.
pub async fn tag_release(
    pool: &PgPool,
    template_name: &str,
    release: Uuid,
    tag: &str,
) -> sqlx::Result<()> {
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)
         ON CONFLICT (template_name, tag) DO UPDATE
         SET template_release_id = EXCLUDED.template_release_id,
             modified_at = clock_timestamp()"
    ))
    .bind(release)
    .bind(template_name)
    .bind(tag)
    .execute(pool)
    .await
    .map(drop)
}

/// One pair's layout: the digest's hex character, then each spelling's age
/// in hours and whether its original owns a storage location.
struct Layout {
    digit: char,
    canonical: (i32, bool),
    bare: (i32, bool),
    canonical_wins: bool,
}

/// The four layouts: storage beats age either way, and between equals the
/// older wins either way. Neither a spelling preference nor `created_at`,
/// `id` or insertion order alone picks every original.
const LAYOUTS: [Layout; 4] = [
    // The older canonical original has no storage; the bare one has.
    Layout {
        digit: '1',
        canonical: (96, false),
        bare: (72, true),
        canonical_wins: false,
    },
    // The older bare original has no storage; the canonical one has.
    Layout {
        digit: '2',
        canonical: (72, true),
        bare: (96, false),
        canonical_wins: true,
    },
    // Neither has storage: the older bare one wins.
    Layout {
        digit: '3',
        canonical: (72, false),
        bare: (96, false),
        canonical_wins: false,
    },
    // Both have storage: the older canonical one wins.
    Layout {
        digit: '4',
        canonical: (96, true),
        bare: (72, true),
        canonical_wins: true,
    },
];

/// Seed four digests of `template`, each stored under both spellings with
/// the layouts above, and answer them in that order. Releases are generic
/// Firecracker releases with no execution metadata, inserted newest first so
/// `id` order runs against `created_at` order. The caller inserts the
/// `templates` row.
///
/// With `twin_metadata` (an `execution_metadata` JSON object binding another
/// environment), each spelling original also gets a twin, an hour younger
/// and owning a storage location, so a rule that ranks storage before the
/// twin test would pick it. Drop [`OLD_INDEX`] first.
pub async fn seed_spelling_pairs(
    pool: &PgPool,
    template: &str,
    twin_metadata: Option<&str>,
) -> sqlx::Result<Vec<SpellingPair>> {
    let mut pairs = Vec::new();
    for layout in &LAYOUTS {
        let bare_hash = layout.digit.to_string().repeat(64);
        let canonical_hash = format!("sha256:{bare_hash}");
        let mut spellings = [
            (canonical_hash.as_str(), layout.canonical),
            (bare_hash.as_str(), layout.bare),
        ];
        // Newest first, so `id` order runs against age.
        spellings.sort_by_key(|(_, (hours, _))| *hours);
        let mut ids = Vec::new();
        let mut twins = Vec::new();
        for (hash, (hours, storage)) in spellings {
            let id = insert_release(pool, template, hash, hours, None, storage).await?;
            ids.push((hash, id));
            if let Some(metadata) = twin_metadata {
                twins.push(
                    insert_release(pool, template, hash, hours - 1, Some(metadata), true).await?,
                );
            }
        }
        let id_of = |hash: &str| {
            ids.iter()
                .find(|(stored, _)| *stored == hash)
                .map(|(_, id)| *id)
                .expect("both spellings were inserted")
        };
        let canonical = id_of(&canonical_hash);
        let bare = id_of(&bare_hash);
        pairs.push(SpellingPair {
            original: if layout.canonical_wins {
                canonical
            } else {
                bare
            },
            canonical_hash,
            bare_hash,
            canonical,
            bare,
            twins,
        });
    }
    Ok(pairs)
}

async fn insert_release(
    pool: &PgPool,
    template: &str,
    hash: &str,
    hours: i32,
    metadata: Option<&str>,
    storage: bool,
) -> sqlx::Result<Uuid> {
    let id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at, execution_metadata)
         VALUES (space_uuid_v7(), $1, 'fc', $2, NOW() - $3 * INTERVAL '1 hour', $4::JSONB)
         RETURNING id"
    ))
    .bind(template)
    .bind(hash)
    .bind(hours)
    .bind(metadata)
    .fetch_one(pool)
    .await?;
    if storage {
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(id)
        .bind(format!("s3://fixture-bucket/{template}/{id}"))
        .execute(pool)
        .await?;
    }
    Ok(id)
}
