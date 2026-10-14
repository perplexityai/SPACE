use super::*;
use pplx_space_common_core::types::OrgId;

pub(crate) fn register(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        sql!("INSERT INTO sandbox_pool_adoptions (sandbox_id) VALUES (?1)"),
        [id],
    )?;
    Ok(())
}

pub(crate) fn incomplete(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(sql!("SELECT EXISTS (SELECT 1 FROM sandbox_pool_adoptions WHERE sandbox_id = ?1 AND claim_id IS NOT NULL AND completed = 0)"), [id], |row| row.get(0))
}

pub(crate) fn guest_access_blocked(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(sql!("SELECT EXISTS (SELECT 1 FROM sandbox_pool_adoptions WHERE sandbox_id = ?1 AND completed = 0)"), [id], |row| row.get(0))
}

#[derive(Clone, Copy)]
pub(crate) struct PoolClaim {
    pub id: uuid::Uuid,
    pub org_id: OrgId,
    pub volume_encryption: bool,
}

pub(crate) fn begin(
    conn: &Connection,
    id: &str,
    claim: PoolClaim,
    owner: &SandboxOwner,
    env: &BTreeMap<String, String>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(sandbox) = get_sandbox(&tx, id)? else {
        return Ok(false);
    };
    if !matches!(sandbox.state, SandboxState::Started | SandboxState::Paused) {
        return Ok(false);
    }
    let stored: Option<(Option<String>, Option<String>, bool)> = tx
        .query_row(
            sql!("SELECT claim_id, org_id, volume_encryption FROM sandbox_pool_adoptions WHERE sandbox_id = ?1"),
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    match stored {
        Some((None, None, _))
            if sandbox.user == pplx_space_common_core::types::ROOT_SANDBOX_OWNER
                && !sandbox.volume_encryption =>
        {
            tx.execute(sql!("UPDATE sandbox_pool_adoptions SET claim_id = ?2, org_id = ?3, volume_encryption = ?4 WHERE sandbox_id = ?1"), params![id, claim.id.to_string(), claim.org_id.to_string(), claim.volume_encryption])?;
            update_user(&tx, id, owner)?;
            update_process_env(&tx, id, env)?;
        }
        Some((Some(stored_claim), Some(org), encrypted))
            if stored_claim == claim.id.to_string()
                && org == claim.org_id.to_string()
                && encrypted == claim.volume_encryption
                && sandbox.user == *owner
                && sandbox.process_env == *env => {}
        _ => return Ok(false),
    }
    tx.commit()?;
    Ok(true)
}

pub(crate) fn complete(conn: &Connection, id: &str, claim: PoolClaim) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(sql!("UPDATE sandboxes SET volume_encryption = ?3 WHERE id = ?1 AND state = 'paused' AND EXISTS (SELECT 1 FROM sandbox_pool_adoptions WHERE sandbox_id = ?1 AND claim_id = ?2 AND volume_encryption = ?3 AND org_id = ?4)"), params![id, claim.id.to_string(), claim.volume_encryption, claim.org_id.to_string()])?;
    if changed != 1 {
        return Ok(false);
    }
    tx.execute(sql!("UPDATE sandbox_pool_adoptions SET completed = 1 WHERE sandbox_id = ?1 AND claim_id = ?2"), params![id, claim.id.to_string()])?;
    tx.commit()?;
    Ok(true)
}
