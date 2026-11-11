//! Metric families that more than one service records, so that each of them
//! can emit the same names and tag values without depending on another
//! service's crate: sandbox admission, decided by the API server for the
//! cluster and by sbx-mgr for its node; and sandbox state publication,
//! degraded on either side of the publication from sbx-mgr to internal-api.
//!
//! `gateway_readiness` holds the readiness-wait guard both the net gateway and
//! sbx-mgr time their waits with: the `Wait` guard, its `Outcome` vocabulary
//! and the node and build tags it adds. Each service's wait constructors sit
//! beside it so that every wait is built on the same guard.
//!
//! A family one service records otherwise lives in that service's crate.

pub mod admission;
pub mod gateway_readiness;
pub mod publication;
