//! Configuration stamps: inert marker rules that record which version of a
//! namespace rule part node-manager installed.
//!
//! A stamp is a rule whose only statement is a counter and whose comment
//! starts with [`PREFIX`]. It has no match and no verdict, so it never changes
//! how a packet is handled. Checks of listed namespace rules skip stamps so
//! they see only the rules that filter traffic. A rule that carries the prefix
//! with any other statement is not a stamp and is checked like any other rule.

use serde_json::Value;

pub(super) const PREFIX: &str = "space-cfg:";

/// Whether a rule object from an `nft -j` listing is a stamp.
pub(super) fn is_stamp_rule(rule: &Value) -> bool {
    let comment = rule.get("comment").and_then(Value::as_str);
    let exprs = rule.get("expr").and_then(Value::as_array);
    comment.is_some_and(|comment| comment.starts_with(PREFIX))
        && matches!(exprs.map(Vec::as_slice), Some([expr]) if is_counter(expr))
}

fn is_counter(expr: &Value) -> bool {
    expr.as_object()
        .is_some_and(|expr| expr.len() == 1 && expr.contains_key("counter"))
}

#[cfg(test)]
mod tests;
