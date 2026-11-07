//! The node's activation seal.

use crate::db::activation_seal::{ActivationSealRecord, DrainEvacuation};
use crate::db::{Connection, Result};

/// The seal row, `None` on a node that was never sealed; a row that does not
/// decode fails the read.
pub fn activation_seal(conn: &Connection) -> Result<Option<ActivationSealRecord>> {
    crate::db::activation_seal::activation_seal(conn)
}

/// Every drain evacuation the node recorded.
pub fn drain_evacuations(conn: &Connection) -> Result<Vec<DrainEvacuation>> {
    crate::db::activation_seal::drain_evacuations(conn)
}
