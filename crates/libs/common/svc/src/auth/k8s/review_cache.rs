//! The reviewed-token cache that every `TokenReview` reviewer in
//! [`crate::auth`] uses: node-api's surface verifier and the build reviewer.
//!
//! Each reviewer keeps its own instance and decides what a verdict holds and
//! how long it lives. This module stores verdicts by token digest and
//! audience, bounds how many it keeps, and lets concurrent requests bearing
//! the same token wait for one review instead of each starting their own.

use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sha2::{Digest, Sha256};

use super::lock_or_recover;

/// A token is cached under its SHA-256 digest, never its value, and per
/// audience: a token reviewed for one audience is no answer for another.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::auth) struct TokenCacheKey {
    token_digest: [u8; 32],
    audience: String,
}

impl TokenCacheKey {
    pub(in crate::auth) fn new(token: &str, audience: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        Self {
            token_digest: hasher.finalize().into(),
            audience: audience.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::auth) struct CachedTokenReview<R> {
    pub(super) result: R,
    pub(super) expires_at: Instant,
}

/// What a lookup found under a key.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::auth) enum Lookup<R> {
    Live(R),
    /// The verdict had expired and is now gone.
    Expired,
    Absent,
}

type InFlightReviews = Arc<Mutex<HashMap<TokenCacheKey, Vec<tokio::sync::oneshot::Sender<()>>>>>;

pub(in crate::auth) struct TokenReviewCache<R> {
    pub(super) entries: Mutex<HashMap<TokenCacheKey, CachedTokenReview<R>>>,
    in_flight: InFlightReviews,
    max_entries: usize,
}

impl<R: Clone> TokenReviewCache<R> {
    pub(in crate::auth) fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            max_entries,
        }
    }

    pub(in crate::auth) fn lookup(&self, key: &TokenCacheKey, now: Instant) -> Lookup<R> {
        let mut entries = lock_or_recover(&self.entries);
        match entries.get(key) {
            Some(cached) if cached.expires_at > now => Lookup::Live(cached.result.clone()),
            Some(_) => {
                entries.remove(key);
                Lookup::Expired
            }
            None => Lookup::Absent,
        }
    }

    /// Stores `result` until `expires_at`. Returns whether an unexpired entry
    /// was evicted to make room.
    pub(in crate::auth) fn insert(
        &self,
        key: TokenCacheKey,
        result: R,
        expires_at: Instant,
        now: Instant,
    ) -> bool {
        insert_token_review(
            &mut lock_or_recover(&self.entries),
            key,
            result,
            expires_at,
            now,
            self.max_entries,
        )
    }

    /// Claims the review of `key`, or joins the review another request
    /// already claimed. A waiter is woken when that claim drops, whatever its
    /// outcome, and must look the key up again.
    pub(in crate::auth) fn join_in_flight_or_claim(&self, key: &TokenCacheKey) -> InFlightState {
        let mut in_flight = lock_or_recover(&self.in_flight);
        match in_flight.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                entry.get_mut().push(tx);
                InFlightState::Waiting(rx)
            }
            Entry::Vacant(entry) => {
                entry.insert(Vec::new());
                InFlightState::Claimed(InFlightClaim {
                    key: key.clone(),
                    in_flight: Arc::clone(&self.in_flight),
                })
            }
        }
    }
}

pub(in crate::auth) enum InFlightState {
    Claimed(InFlightClaim),
    Waiting(tokio::sync::oneshot::Receiver<()>),
}

pub(in crate::auth) struct InFlightClaim {
    key: TokenCacheKey,
    in_flight: InFlightReviews,
}

impl Drop for InFlightClaim {
    fn drop(&mut self) {
        let waiters = lock_or_recover(&self.in_flight)
            .remove(&self.key)
            .unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

pub(super) fn prune_token_review_cache<R>(
    cache: &mut HashMap<TokenCacheKey, CachedTokenReview<R>>,
    now: Instant,
    max_entries: usize,
) {
    if max_entries == 0 {
        cache.clear();
        return;
    }
    cache.retain(|_, cached| cached.expires_at > now);
}

pub(super) fn insert_token_review<R>(
    cache: &mut HashMap<TokenCacheKey, CachedTokenReview<R>>,
    key: TokenCacheKey,
    result: R,
    expires_at: Instant,
    now: Instant,
    max_entries: usize,
) -> bool {
    if max_entries == 0 {
        cache.clear();
        return false;
    }

    prune_token_review_cache(cache, now, max_entries);
    // Capacity eviction is O(N), bounded by max_entries. Keep the cap modest;
    // if this cache ever needs to grow materially, replace the HashMap with a
    // real LRU/LFU structure instead of stretching this scan.
    let evicted_capacity = if cache.len() >= max_entries {
        if let Some(evict_key) = cache
            .iter()
            .min_by_key(|(_, cached)| cached.expires_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&evict_key);
            true
        } else {
            false
        }
    } else {
        false
    };
    cache.insert(key, CachedTokenReview { result, expires_at });
    evicted_capacity
}
