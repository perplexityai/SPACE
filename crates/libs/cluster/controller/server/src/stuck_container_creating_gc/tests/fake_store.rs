//! The in-memory `GcStore` the reconciler tests run against.

use super::*;

/// In-memory `GcStore`. Only rows explicitly registered exist; a requested key
/// with no registration comes back `Absent`, which is what a stale marker looks
/// like.
#[derive(Default)]
pub(super) struct FakeStore {
    pub(super) restores: HashMap<SandboxId, Owner>,
    /// Sandbox ids whose row exists but whose `lifecycle` doesn't parse.
    pub(super) unreadable_sandboxes: HashSet<SandboxId>,
    pub(super) slots: HashMap<String, Owner>,
    /// Pod names whose slot row exists but whose `state` doesn't parse.
    pub(super) unreadable_slots: HashSet<String>,
    pub(super) fail_lifecycles: bool,
    pub(super) fail_slots: bool,
    /// When set, both lookups await this long — used under `start_paused` to
    /// exercise the marker-lookup budget without touching the tick budget.
    pub(super) lookup_delay: Option<Duration>,
    /// Sandbox ids passed to each `restore_owners` call, so tests can assert
    /// the lookup is batched (one call per tick) and scoped to marked pods.
    pub(super) lifecycle_queries: Mutex<Vec<Vec<SandboxId>>>,
    pub(super) slot_queries: Mutex<Vec<Vec<String>>>,
    /// Applied once the first query has answered, so a test can simulate an
    /// ownership row changing between the tick's batch lookup and the
    /// pre-delete re-read.
    pub(super) slot_reclaim: Option<(String, Owner)>,
    pub(super) lifecycle_reclaim: Option<(SandboxId, Owner)>,
}

impl FakeStore {
    pub(super) fn with_restore_owner(mut self, sandbox_id: SandboxId, owner: Owner) -> Self {
        self.restores.insert(sandbox_id, owner);
        self
    }

    pub(super) fn with_unreadable_lifecycle(mut self, sandbox_id: SandboxId) -> Self {
        self.unreadable_sandboxes.insert(sandbox_id);
        self
    }

    pub(super) fn with_slot_owner(mut self, pod_name: &str, owner: Owner) -> Self {
        self.slots.insert(pod_name.to_string(), owner);
        self
    }

    pub(super) fn with_unreadable_slot(mut self, pod_name: &str) -> Self {
        self.unreadable_slots.insert(pod_name.to_string());
        self
    }

    pub(super) fn stalling(delay: Duration) -> Self {
        Self {
            lookup_delay: Some(delay),
            ..Default::default()
        }
    }

    pub(super) fn failing_lifecycles() -> Self {
        Self {
            fail_lifecycles: true,
            ..Default::default()
        }
    }

    pub(super) fn failing_slots() -> Self {
        Self {
            fail_slots: true,
            ..Default::default()
        }
    }

    /// From the second `warm_pool_owners` call on, answer `state` for
    /// `pod_name` — the Postgres-only ownership change that races the delete.
    pub(super) fn with_slot_owner_after_first_read(mut self, pod_name: &str, owner: Owner) -> Self {
        self.slot_reclaim = Some((pod_name.to_string(), owner));
        self
    }

    /// The lifecycle equivalent of [`Self::with_slot_owner_after_first_read`].
    pub(super) fn with_restore_owner_after_first_read(
        mut self,
        sandbox_id: SandboxId,
        owner: Owner,
    ) -> Self {
        self.lifecycle_reclaim = Some((sandbox_id, owner));
        self
    }

    /// The slot state as of *this* query. Both query methods record the call
    /// before building their answer, so "a later query than the first" is
    /// exactly `count > 1` — which is what makes the reclaim mean "the row
    /// changed between the tick's batch lookup and the pre-delete re-read".
    pub(super) fn effective_slot(&self, pod_name: &str) -> Option<Owner> {
        if self.slot_query_count() > 1
            && let Some((reclaimed, owner)) = &self.slot_reclaim
            && reclaimed == pod_name
        {
            return Some(*owner);
        }
        self.slots.get(pod_name).copied()
    }

    pub(super) fn effective_restore_owner(&self, sandbox_id: SandboxId) -> Option<Owner> {
        if self.lifecycle_query_count() > 1
            && let Some((reclaimed, owner)) = &self.lifecycle_reclaim
            && *reclaimed == sandbox_id
        {
            return Some(*owner);
        }
        self.restores.get(&sandbox_id).copied()
    }

    pub(super) fn lifecycle_query_count(&self) -> usize {
        self.lifecycle_queries.lock().unwrap().len()
    }

    pub(super) fn slot_query_count(&self) -> usize {
        self.slot_queries.lock().unwrap().len()
    }

    pub(super) fn last_lifecycle_query(&self) -> Vec<SandboxId> {
        self.lifecycle_queries
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn last_slot_query(&self) -> Vec<String> {
        self.slot_queries
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

impl GcStore for FakeStore {
    async fn restore_owners(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, Lookup<Owner>>, StoreError> {
        self.lifecycle_queries
            .lock()
            .unwrap()
            .push(sandbox_ids.to_vec());
        if let Some(delay) = self.lookup_delay {
            tokio::time::sleep(delay).await;
        }
        if self.fail_lifecycles {
            return Err(StoreError::Query(sqlx::Error::PoolTimedOut));
        }
        let answer: HashMap<SandboxId, Lookup<Owner>> = sandbox_ids
            .iter()
            .map(|id| {
                let resolved = match self.effective_restore_owner(*id) {
                    Some(owner) => Lookup::Known(owner),
                    None if self.unreadable_sandboxes.contains(id) => Lookup::Unknown,
                    None => Lookup::Absent,
                };
                (*id, resolved)
            })
            .collect();
        Ok(answer)
    }

    async fn warm_pool_owners(
        &self,
        pod_names: &[String],
    ) -> Result<HashMap<String, Lookup<Owner>>, StoreError> {
        self.slot_queries.lock().unwrap().push(pod_names.to_vec());
        if let Some(delay) = self.lookup_delay {
            tokio::time::sleep(delay).await;
        }
        if self.fail_slots {
            return Err(StoreError::Query(sqlx::Error::PoolTimedOut));
        }
        let answer: HashMap<String, Lookup<Owner>> = pod_names
            .iter()
            .map(|name| {
                let resolved = match self.effective_slot(name) {
                    Some(owner) => Lookup::Known(owner),
                    None if self.unreadable_slots.contains(name) => Lookup::Unknown,
                    None => Lookup::Absent,
                };
                (name.clone(), resolved)
            })
            .collect();
        Ok(answer)
    }
}
