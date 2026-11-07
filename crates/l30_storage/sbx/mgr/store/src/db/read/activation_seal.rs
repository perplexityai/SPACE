//! The node's activation seal.

use crate::db::activation_seal::ActivationSealRecord;
use crate::db::{Connection, Result};

/// The seal row, `None` on a node that was never sealed; a row that does not
/// decode fails the read.
pub fn activation_seal(conn: &Connection) -> Result<Option<ActivationSealRecord>> {
    crate::db::activation_seal::activation_seal(conn)
}
