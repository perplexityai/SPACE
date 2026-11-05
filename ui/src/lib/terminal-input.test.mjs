import assert from 'node:assert/strict';
import { test } from 'node:test';
import { TERMINAL_INPUT_CHUNK_BYTES, terminalInputChunks, terminalInputFrames } from './terminal-input.ts';

const utf8 = new TextEncoder();

function concat(chunks) {
  const bytes = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function counter(start) {
  let seq = start;
  return () => (seq += 1);
}

test('a large paste is sent as ordered frames whose bytes concatenate to its UTF-8', () => {
  // About 5 MiB of numbered lines, so a reordered chunk changes the bytes, with
  // a 3-byte character astride the first chunk boundary.
  const head = 'x'.repeat(TERMINAL_INPUT_CHUNK_BYTES - 1) + '€';
  const lines = [];
  for (let line = 0; lines.length * 15 < 5 * 1024 * 1024; line += 1) {
    lines.push(`${String(line).padStart(14, '0')}\r`);
  }
  const paste = head + lines.join('');
  const bytes = utf8.encode(paste);
  const frames = terminalInputFrames(paste, counter(7));
  assert.equal(frames.length, Math.ceil(bytes.byteLength / TERMINAL_INPUT_CHUNK_BYTES));
  assert.deepEqual(frames.map((frame) => frame.seq), frames.map((_, index) => 8 + index));
  const chunks = frames.map((frame) => Buffer.from(frame.data_base64, 'base64'));
  for (const chunk of chunks.slice(0, -1)) assert.equal(chunk.byteLength, TERMINAL_INPUT_CHUNK_BYTES);
  assert.deepEqual([...chunks[0].subarray(-1)], [0xe2]);
  assert.deepEqual([...chunks[1].subarray(0, 2)], [0x82, 0xac]);
  assert.deepEqual(new Uint8Array(Buffer.concat(chunks)), bytes);
});

test('a chunk boundary can fall inside a multibyte character', () => {
  // `€` is 3 bytes; with 4-byte chunks the second starts mid-character.
  const chunks = terminalInputChunks('a€€b', 4);
  assert.deepEqual(chunks.map((chunk) => [...chunk]), [
    [0x61, 0xe2, 0x82, 0xac],
    [0xe2, 0x82, 0xac, 0x62],
  ]);
  const split = terminalInputChunks('ab€', 3);
  assert.deepEqual(split.map((chunk) => [...chunk]), [[0x61, 0x62, 0xe2], [0x82, 0xac]]);
  assert.equal(new TextDecoder().decode(concat(split)), 'ab€');
});

test('bytes split the same way and are not re-encoded', () => {
  const bytes = Uint8Array.from({ length: 10 }, (_, index) => 0x80 + index);
  const frames = terminalInputFrames(bytes, counter(0), 4);
  assert.deepEqual(
    frames.map((frame) => [frame.seq, [...Buffer.from(frame.data_base64, 'base64')]]),
    [
      [1, [0x80, 0x81, 0x82, 0x83]],
      [2, [0x84, 0x85, 0x86, 0x87]],
      [3, [0x88, 0x89]],
    ],
  );
});

test('input that fits one chunk, and empty input, is one frame', () => {
  assert.deepEqual(terminalInputFrames('ls\r', counter(0)), [{ type: 'input', seq: 1, data_base64: 'bHMN' }]);
  assert.deepEqual(terminalInputChunks('aaaa', 4).map((chunk) => chunk.byteLength), [4]);
  assert.deepEqual(terminalInputFrames('', counter(0)), [{ type: 'input', seq: 1, data_base64: '' }]);
  assert.deepEqual(terminalInputFrames(new Uint8Array(), counter(0)), [{ type: 'input', seq: 1, data_base64: '' }]);
});
