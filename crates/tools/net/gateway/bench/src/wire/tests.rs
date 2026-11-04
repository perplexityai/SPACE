use super::*;

#[test]
fn every_op_round_trips_through_the_header() {
    for op in [Op::Echo, Op::Sink, Op::Source] {
        let arg = 1u64 << 40;
        assert_eq!(decode(&encode(op, arg)), Some((op, arg)));
    }
}

#[test]
fn an_unknown_op_code_does_not_decode() {
    let mut header = encode(Op::Echo, 0);
    if let Some(code) = header.first_mut() {
        *code = 0xff;
    }
    assert_eq!(decode(&header), None);
}
