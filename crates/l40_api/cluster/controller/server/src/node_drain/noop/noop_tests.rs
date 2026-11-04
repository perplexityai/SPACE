use super::*;
use pplx_space_common_core::types::NodeId;

#[tokio::test]
async fn returns_ok_without_side_effects() {
    let term = NoopInstanceTerminator::new();
    let res = term.terminate(NodeId::new()).await;
    assert!(res.is_ok(), "noop terminator must always return Ok");
}
