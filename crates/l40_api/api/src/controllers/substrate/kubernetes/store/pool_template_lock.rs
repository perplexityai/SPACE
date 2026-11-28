//! The template row lock an operator's pool-config write takes against a
//! runtime upgrade campaign's twin insert.

use super::*;

impl K8sStore {
    /// Lock `template_name`'s row against a twin insert, before an operator's
    /// pool-config write changes a config or looks up the twins it changes
    /// too (the one creates resolve to, for an upsert, which takes this only
    /// where creates boot twins, the only case it changes a twin; every twin
    /// of an original, for a disable). The insert holds the row `FOR NO KEY UPDATE` while it makes
    /// the twin and copies the original's manual configs to it in one
    /// transaction (`pplx_space_template_release_store::insert_twin`), which
    /// conflicts with this `FOR SHARE`. So either the copy reads this write,
    /// or this write sees the committed twin and changes its config too;
    /// without it each could miss the other and leave the twin on the
    /// original's old settings. Taken before any config row, the order the
    /// insert takes them in.
    ///
    /// Only a `present` template is locked, as only one gets a twin
    /// (`insert_twin` locks only a `present` template's row), so a pool write
    /// on a template being deleted has no twin insert to serialize with. Once
    /// the template is marked `deleting` it takes no lock and doesn't queue
    /// behind the delete's cleanup; it waits only on a mark that hasn't
    /// committed yet, which takes no config row.
    pub(in crate::controllers::substrate::kubernetes) async fn lock_template_against_twin_inserts(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        template_name: &str,
    ) -> Result<(), SubstrateError> {
        pplx_space_template_store::templates::share_lock_if_present(&mut **tx, template_name)
            .await
            .db_context("failed to lock the template against twin inserts")?;
        Ok(())
    }

    /// The template whose release pool config `config_id` belongs to; `None`
    /// when there's no such config. Takes no lock.
    pub(in crate::controllers::substrate::kubernetes) async fn pool_config_template(
        &self,
        conn: &mut PgConnection,
        config_id: Uuid,
    ) -> Result<Option<String>, SubstrateError> {
        pplx_space_cluster_pool::pool_config_template_name(&mut *conn, config_id)
            .await
            .map_err(sqlx::Error::from)
            .db_context("failed to read the pool config's template")
    }
}
