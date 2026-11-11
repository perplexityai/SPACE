use pplx_space_common_core::{
    sql,
    types::{TemplateId, TemplateVisibility},
};
use pplx_space_db_test_fixtures::TestDbResult;
use sqlx::PgPool;
use uuid::Uuid;

pub(super) async fn insert_org_specific_template(
    pool: &PgPool,
    name: &str,
    owner_org: Uuid,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id, visibility, network_policy_generation)
         VALUES ($1, $2, NULL, $3, $4, 1)"
    ))
    .bind(TemplateId::new().as_uuid())
    .bind(name)
    .bind(owner_org)
    .bind(TemplateVisibility::OrgSpecific.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) const RESOLVER_TEMPLATE: &str = "resolver-fixture";
pub(super) const RESOLVER_PRIVATE_TEMPLATE: &str = "resolver-fixture-private";
pub(super) const RESOLVER_UNTAGGED_TEMPLATE: &str = "resolver-fixture-untagged";
pub(super) const RESOLVER_DELETING_TEMPLATE: &str = "resolver-fixture-deleting";
/// Owns `RESOLVER_PRIVATE_TEMPLATE`.
pub(super) const RESOLVER_OWNER_ORG: Uuid = pplx_space_db_test_fixtures::SEED_ORG_ID;
/// A caller that owns nothing; it needs no `organizations` row.
pub(super) const RESOLVER_OTHER_ORG: Uuid = Uuid::from_u128(0x5e50_1e00);

/// 64 copies of `c`: a synthetic digest body.
pub(super) fn hex(c: char) -> String {
    c.to_string().repeat(64)
}

/// Release ids of the shared template release resolver world.
///
/// `RESOLVER_TEMPLATE` (generic), all Firecracker, with `created_at` ages:
/// `bare` (`aa…`, 5d, tag `legacy` modified 30s ago), `prefixed`
/// (`sha256:bb…`, 4d), `blake3` (`blake3:cc…`, 3d), `latest_holder`
/// (`sha256:dd…`, 6d), and `newest_created` (`sha256:ee…`, 1h). `latest` was
/// first written on `newest_created` and then moved to `latest_holder`
/// (modified 1m ago), so `legacy`, not `latest`, is the most recently modified
/// tag. Two digests are stored in both forms, laid out opposite ways:
/// `dual_prefixed` (`sha256:99…`, 8d) is inserted before `dual_bare` (`99…`,
/// 7d), and `early_bare` (`88…`, 9d) before `late_prefixed` (`sha256:88…`,
/// 2d). The prefixed row is the older one with the smaller id in the first
/// pair and the newer one with the larger id in the second, so newest first,
/// oldest first, id order and insertion order each miss it in one pair; only
/// an exact-form preference picks it in both.
/// `RESOLVER_PRIVATE_TEMPLATE` is org-specific to `RESOLVER_OWNER_ORG` with
/// `private` (`sha256:ff…`) tagged `latest`. `RESOLVER_UNTAGGED_TEMPLATE` has
/// no tags and three releases in insertion order: `untagged_older`
/// (`sha256:11…`, 2d), `untagged_newest` (`sha256:22…`, 1d) and
/// `sha256:33…` (3d), so the newest by `created_at` is neither first nor last
/// by insertion, id or hash. `RESOLVER_DELETING_TEMPLATE` is generic with
/// `desired_state = 'deleting'` and `deleting` (`sha256:77…`) tagged `latest`.
///
/// `template_releases_type_check` admits only `fc`, so no Cloud Hypervisor
/// release can be stored beside these; runtime is exercised on the request.
#[cfg_attr(not(feature = "kubernetes"), allow(dead_code))]
pub(super) struct ResolverWorld {
    pub(super) bare: Uuid,
    pub(super) prefixed: Uuid,
    pub(super) blake3: Uuid,
    pub(super) latest_holder: Uuid,
    pub(super) dual_bare: Uuid,
    pub(super) dual_prefixed: Uuid,
    pub(super) late_prefixed: Uuid,
    pub(super) private: Uuid,
    pub(super) untagged_older: Uuid,
    pub(super) untagged_newest: Uuid,
    pub(super) deleting: Uuid,
}

pub(super) async fn seed_resolver_world(pool: &PgPool) -> TestDbResult<ResolverWorld> {
    async fn release(
        pool: &PgPool,
        template: &str,
        hash: String,
        age: &str,
        org: Option<Uuid>,
    ) -> TestDbResult<Uuid> {
        let id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash, org_id, created_at)
             VALUES ($1, $2, 'fc', $3, $4, NOW() - $5::INTERVAL)"
        ))
        .bind(id)
        .bind(template)
        .bind(hash)
        .bind(org)
        .bind(age)
        .execute(pool)
        .await?;
        Ok(id)
    }
    async fn tag(pool: &PgPool, template: &str, release: Uuid, tag: &str) -> TestDbResult<()> {
        sqlx::query(sql!(
            "INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, $3)"
        ))
        .bind(release)
        .bind(template)
        .bind(tag)
        .execute(pool)
        .await?;
        Ok(())
    }
    async fn retag(
        pool: &PgPool,
        template: &str,
        release: Uuid,
        tag: &str,
        age: &str,
    ) -> TestDbResult<()> {
        sqlx::query(sql!(
            "UPDATE template_release_tags
             SET template_release_id = $1, modified_at = NOW() - $4::INTERVAL
             WHERE template_name = $2 AND tag = $3"
        ))
        .bind(release)
        .bind(template)
        .bind(tag)
        .bind(age)
        .execute(pool)
        .await?;
        Ok(())
    }

    for name in [RESOLVER_TEMPLATE, RESOLVER_UNTAGGED_TEMPLATE] {
        sqlx::query(sql!("INSERT INTO templates (id, name) VALUES ($1, $2)"))
            .bind(TemplateId::new().as_uuid())
            .bind(name)
            .execute(pool)
            .await?;
    }
    insert_org_specific_template(pool, RESOLVER_PRIVATE_TEMPLATE, RESOLVER_OWNER_ORG).await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, desired_state) VALUES ($1, $2, 'deleting')"
    ))
    .bind(TemplateId::new().as_uuid())
    .bind(RESOLVER_DELETING_TEMPLATE)
    .execute(pool)
    .await?;

    let t = RESOLVER_TEMPLATE;
    let newest_created = release(pool, t, format!("sha256:{}", hex('e')), "1 hour", None).await?;
    let dual_prefixed = release(pool, t, format!("sha256:{}", hex('9')), "8 days", None).await?;
    release(pool, t, hex('8'), "9 days", None).await?;
    let untagged_older = release(
        pool,
        RESOLVER_UNTAGGED_TEMPLATE,
        format!("sha256:{}", hex('1')),
        "2 days",
        None,
    )
    .await?;
    let world = ResolverWorld {
        bare: release(pool, t, hex('a'), "5 days", None).await?,
        prefixed: release(pool, t, format!("sha256:{}", hex('b')), "4 days", None).await?,
        blake3: release(pool, t, format!("blake3:{}", hex('c')), "3 days", None).await?,
        latest_holder: release(pool, t, format!("sha256:{}", hex('d')), "6 days", None).await?,
        dual_bare: release(pool, t, hex('9'), "7 days", None).await?,
        dual_prefixed,
        private: release(
            pool,
            RESOLVER_PRIVATE_TEMPLATE,
            format!("sha256:{}", hex('f')),
            "1 day",
            Some(RESOLVER_OWNER_ORG),
        )
        .await?,
        untagged_newest: release(
            pool,
            RESOLVER_UNTAGGED_TEMPLATE,
            format!("sha256:{}", hex('2')),
            "1 day",
            None,
        )
        .await?,
        untagged_older,
        deleting: release(
            pool,
            RESOLVER_DELETING_TEMPLATE,
            format!("sha256:{}", hex('7')),
            "1 day",
            None,
        )
        .await?,
        late_prefixed: release(pool, t, format!("sha256:{}", hex('8')), "2 days", None).await?,
    };
    release(
        pool,
        RESOLVER_UNTAGGED_TEMPLATE,
        format!("sha256:{}", hex('3')),
        "3 days",
        None,
    )
    .await?;
    tag(pool, t, world.bare, "legacy").await?;
    retag(pool, t, world.bare, "legacy", "30 seconds").await?;
    tag(pool, t, newest_created, "latest").await?;
    retag(pool, t, world.latest_holder, "latest", "1 minute").await?;
    tag(pool, RESOLVER_PRIVATE_TEMPLATE, world.private, "latest").await?;
    tag(pool, RESOLVER_DELETING_TEMPLATE, world.deleting, "latest").await?;
    Ok(world)
}
