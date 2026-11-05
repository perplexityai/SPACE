// Each terminal `input` frame becomes one SendInput call to the sandbox, and
// the node's gRPC services take at most 4 MiB per message: a larger paste
// fails the call and closes the session, though the data plane's websocket
// takes messages up to 64 MiB. Input is sent in chunks well under that limit.
export const TERMINAL_INPUT_CHUNK_BYTES = 64 * 1024;

export type TerminalInputFrame = { type: 'input'; seq: number; data_base64: string };

const utf8 = new TextEncoder();

// Text is UTF-8 encoded once and split by bytes, so a chunk can end inside a
// character: the PTY receives the chunks' bytes back to back, in order. An
// empty input is one empty chunk.
export function terminalInputChunks(
  input: string | Uint8Array,
  chunkBytes = TERMINAL_INPUT_CHUNK_BYTES,
): Uint8Array[] {
  const bytes = typeof input === 'string' ? utf8.encode(input) : input;
  if (bytes.byteLength <= chunkBytes) return [bytes];
  const chunks: Uint8Array[] = [];
  for (let start = 0; start < bytes.byteLength; start += chunkBytes) {
    chunks.push(bytes.subarray(start, start + chunkBytes));
  }
  return chunks;
}

// One `input` frame per chunk, in order, each with the next `seq`.
export function terminalInputFrames(
  input: string | Uint8Array,
  nextSeq: () => number,
  chunkBytes = TERMINAL_INPUT_CHUNK_BYTES,
): TerminalInputFrame[] {
  return terminalInputChunks(input, chunkBytes).map((chunk) => ({
    type: 'input',
    seq: nextSeq(),
    data_base64: encodeBase64(chunk),
  }));
}

function encodeBase64(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}
