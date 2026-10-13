use super::*;

const WINDOW: TailWindow = TailWindow {
    scan_bytes: 64 * 1024,
    keep_bytes: 4 * 1024,
    noise: firecracker_state_poll,
};

/// Working memory is the buffers [`TailBuffers::new`] sizes from the window:
/// no input shape makes them grow, and the output stays within `keep_bytes`.
#[test]
fn tail_working_memory_does_not_grow_with_line_structure() {
    let poll = "T [vm:fc_api] The API server received a Get request on \"/\".\n\
                T [vm:fc_api] The request was executed successfully. Status code: 200 OK.\n";
    let scan = WINDOW.scan_bytes as usize;
    let mut invalid_utf8 = Vec::new();
    while invalid_utf8.len() < scan {
        invalid_utf8.extend_from_slice(b"\xff\xe2\x82 ok\x1b\n");
    }
    let inputs: [(&str, Vec<u8>); 5] = [
        ("only newlines", vec![b'\n'; scan]),
        ("one unterminated line", vec![b'x'; scan]),
        ("two-byte lines", b"x\n".repeat(scan / 2)),
        ("state polls", poll.repeat(scan / poll.len()).into_bytes()),
        ("invalid UTF-8 and controls", invalid_utf8),
    ];
    let mut buffers = TailBuffers::new(WINDOW);
    let reserved = buffers.capacities();
    let mut tails = Vec::new();
    for (shape, input) in &inputs {
        let tail = tail_lines(input.as_slice(), false, WINDOW, &mut buffers).unwrap();
        assert_eq!(buffers.capacities(), reserved, "{shape}");
        assert!(tail.len() <= WINDOW.keep_bytes, "{shape}");
        tails.push(tail);
    }
    assert_eq!(tails[0], "\n".repeat(WINDOW.keep_bytes));
    assert_eq!(tails[1], "");
    assert_eq!(tails[2], "x\n".repeat(WINDOW.keep_bytes / 2));
    assert_eq!(tails[3], "");
    assert!(tails[4].ends_with("?? ok?\n"));
}
