import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import {
  TERMINAL_SHELL_COMMAND,
  TerminalInputGate,
  binaryStringToBytes,
  debounce,
  isMacPlatform,
  terminalEventText,
  terminalKeyAction,
  terminalLinkUrl,
  terminalNotice,
} from './terminal-pane.ts';

function fakeTimers() {
  let now = 0;
  let next = 1;
  const pending = new Map();
  return {
    setTimeout(callback, ms) {
      const handle = next++;
      pending.set(handle, { at: now + ms, callback });
      return handle;
    },
    clearTimeout(handle) {
      pending.delete(handle);
    },
    advance(ms) {
      now += ms;
      for (const [handle, timer] of [...pending]) {
        if (timer.at <= now) {
          pending.delete(handle);
          timer.callback();
        }
      }
    },
  };
}

test('a burst of resizes fits once, after the last one settles', () => {
  const timers = fakeTimers();
  let fits = 0;
  const resize = debounce(() => { fits += 1; }, 100, timers);

  resize.schedule();
  timers.advance(60);
  resize.schedule();
  timers.advance(60);
  resize.schedule();
  timers.advance(99);
  assert.equal(fits, 0);
  timers.advance(1);
  assert.equal(fits, 1);

  // A later burst fits again.
  resize.schedule();
  timers.advance(100);
  assert.equal(fits, 2);
});

test('a cancelled resize never fits a disposed terminal', () => {
  const timers = fakeTimers();
  let fits = 0;
  const resize = debounce(() => { fits += 1; }, 100, timers);

  resize.schedule();
  resize.cancel();
  timers.advance(1_000);
  assert.equal(fits, 0);
});

test('output is written exactly as the PTY sent it', () => {
  const text = '\x1b[01;34mbin\x1b[0m  \x1b[H\x1b[2Jtop\r\n';
  assert.equal(terminalEventText({ type: 'output', stream: 'stdout', text }), text);
  assert.equal(terminalEventText({ type: 'output', stream: 'stderr', text: 'warn\n' }), 'warn\n');
  // A frame whose bytes end mid-character decodes to nothing yet.
  assert.equal(terminalEventText({ type: 'output', stream: 'stdout', text: '' }), null);
});

test('session notices are dim lines of their own; the rest writes nothing', () => {
  const cases = [
    [{ type: 'reconnecting', message: 'Backend reconnecting: node drained' }, 'Backend reconnecting: node drained'],
    [{ type: 'error', message: 'Terminal WebSocket failed', retryable: true }, 'error: Terminal WebSocket failed'],
    [{ type: 'exited', processId: 'p1', exitCode: 130, status: 'exited' }, 'process exited: exited 130'],
    [{ type: 'status', status: 'closed', message: 'idle timeout' }, 'idle timeout'],
    [{ type: 'status', status: 'closed' }, null],
    [{ type: 'status', status: 'attached' }, null],
    [{ type: 'started', processId: 'p1', pid: 7 }, null],
    [{ type: 'accepted', seq: 3 }, null],
  ];
  for (const [event, notice] of cases) {
    assert.equal(terminalEventText(event), notice === null ? null : terminalNotice(notice), event.type);
  }
  const written = terminalNotice('idle timeout');
  assert.ok(written.startsWith('\r\n\x1b[2m'), 'starts on a fresh line, dim');
  assert.ok(written.endsWith('\x1b[22m\r\n'), 'resets dim and returns to column 0');
});

test('copy and paste follow the platform convention; Ctrl+C and Ctrl+V reach the shell', () => {
  const key = (k, mods = {}) => ({ key: k, ctrlKey: false, metaKey: false, altKey: false, shiftKey: false, ...mods });
  const ctrl = { ctrlKey: true };
  const ctrlShift = { ctrlKey: true, shiftKey: true };
  const cmd = { metaKey: true };
  // [event, selection, macOS, action]
  const cases = [
    [key('c', cmd), true, true, 'copy'],
    [key('c', cmd), false, true, 'ignore'],
    [key('v', cmd), false, true, 'paste'],
    [key('c', ctrl), true, true, 'terminal'],
    [key('v', ctrl), false, true, 'terminal'],
    [key('C', ctrlShift), true, false, 'copy'],
    [key('C', ctrlShift), false, false, 'ignore'],
    [key('V', ctrlShift), false, false, 'paste'],
    [key('c', ctrl), true, false, 'terminal'],
    [key('v', ctrl), true, false, 'terminal'],
    // The other platform's chord is a plain key here.
    [key('c', cmd), true, false, 'terminal'],
    [key('C', ctrlShift), true, true, 'terminal'],
    [key('c', { ...ctrlShift, altKey: true }), true, false, 'terminal'],
    [key('d', ctrl), false, false, 'terminal'],
    [key('c'), true, false, 'terminal'],
    [key('ArrowUp'), true, true, 'terminal'],
  ];
  for (const [event, hasSelection, mac, action] of cases) {
    assert.equal(terminalKeyAction(event, hasSelection, mac), action, JSON.stringify({ event, hasSelection, mac }));
  }
  assert.equal(isMacPlatform('MacIntel'), true);
  assert.equal(isMacPlatform('iPhone'), true);
  assert.equal(isMacPlatform('Linux x86_64'), false);
  assert.equal(isMacPlatform('Win32'), false);
});

test('input typed before the shell starts is sent first, in order', () => {
  const gate = new TerminalInputGate();
  const text = (data) => ({ kind: 'text', data });
  const bytes = { kind: 'bytes', data: Uint8Array.of(0x1b, 0x5b, 0x4d, 0x20, 0x84, 0x21) };

  // No session: dropped.
  assert.equal(gate.write(text('lost')), null);
  gate.open();
  assert.equal(gate.write(text('ec')), null);
  assert.equal(gate.write(bytes), null);
  assert.equal(gate.write(text('ho hi\r')), null);
  assert.deepEqual(gate.start(), [text('ec'), bytes, text('ho hi\r')]);
  // Started: passes straight through, and a second start sends nothing.
  assert.deepEqual(gate.write(text('x')), text('x'));
  assert.deepEqual(gate.start(), []);

  // Disconnect or cancel before the start drops what was held.
  gate.open();
  gate.write(text('rm -rf /tmp/x\r'));
  gate.close();
  assert.deepEqual(gate.start(), []);
  assert.equal(gate.write(text('y')), null);
  // A new session never sees an older one's held input.
  gate.open();
  gate.write(text('old'));
  gate.open();
  gate.write(text('new'));
  assert.deepEqual(gate.start(), [text('new')]);
});

test('held input past the limit is dropped whole, and nothing more is held', () => {
  const text = (data) => ({ kind: 'text', data });
  const gate = new TerminalInputGate(8);
  gate.open();
  assert.equal(gate.write(text('echo ')), null);
  // A paste that would pass the limit drops everything held, once.
  assert.equal(gate.write(text('0123456789')), 'discarded');
  // The Enter after it must not run `echo` alone.
  assert.equal(gate.write(text('\r')), null);
  assert.equal(gate.write(text('x')), null);
  assert.deepEqual(gate.start(), []);
  // Once started, input flows again.
  assert.deepEqual(gate.write(text('ls\r')), text('ls\r'));
  // The next session holds again.
  gate.open();
  assert.equal(gate.write(text('pwd\r')), null);
  assert.deepEqual(gate.start(), [text('pwd\r')]);
});

test('the limit counts UTF-8 bytes', () => {
  const text = (data) => ({ kind: 'text', data });
  // 'é' is one UTF-16 unit and two UTF-8 bytes; '€' one unit and three bytes.
  const exact = new TerminalInputGate(8);
  exact.open();
  assert.equal(exact.write(text('ééé')), null);
  assert.equal(exact.write(text('é')), null);
  assert.deepEqual(exact.start(), [text('ééé'), text('é')]);

  const over = new TerminalInputGate(8);
  over.open();
  assert.equal(over.write(text('€€')), null);
  // 6 + 3 bytes is past 8, though it is only 3 UTF-16 units.
  assert.equal(over.write(text('€')), 'discarded');
  assert.deepEqual(over.start(), []);

  const bytes = new TerminalInputGate(4);
  bytes.open();
  assert.equal(bytes.write({ kind: 'bytes', data: Uint8Array.of(1, 2, 3, 4) }), null);
  assert.equal(bytes.write({ kind: 'bytes', data: Uint8Array.of(5) }), 'discarded');
});

// Runs the terminal command with stdin as the shell's input, in an
// environment holding only `env`.
function runShellCommand(env, input) {
  return spawnSync(TERMINAL_SHELL_COMMAND[0], TERMINAL_SHELL_COMMAND.slice(1), {
    env, input, encoding: 'utf8', timeout: 3000,
  });
}

test('the shell command finds bash on PATH first, and keeps PATH', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'terminal-shell-'));
  try {
    const bash = path.join(dir, 'bash');
    fs.writeFileSync(bash, '#!/bin/sh\necho "path-bash $*"\n', { mode: 0o755 });
    const result = runShellCommand({ PATH: `${dir}:/usr/bin:/bin`, HOME: dir }, '');
    assert.equal(result.stdout.trim(), 'path-bash -i');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a PATH without /bin still opens an interactive shell that keeps that PATH', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'terminal-shell-'));
  try {
    // Nothing is on this PATH; bash comes from its usual path, or /bin/sh.
    const result = runShellCommand({ PATH: dir, HOME: dir }, 'case $- in *i*) echo "interactive PATH=$PATH";; esac\nexit 0\n');
    assert.equal(result.status, 0, result.stderr);
    const shown = result.stdout.trim().match(/^interactive PATH=(.*)$/);
    assert.ok(shown, result.stdout);
    // A system bashrc may add to PATH; the given one must still be there.
    assert.ok(shown[1].split(':').includes(dir), shown[1]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('binary input keeps each byte', () => {
  // An X10 mouse report at column 100, row 2: 0x84 must stay one byte, not
  // UTF-8's c2 84.
  const report = '\x1b[M \x84"';
  assert.deepEqual([...binaryStringToBytes(report)], [0x1b, 0x5b, 0x4d, 0x20, 0x84, 0x22]);
});

test('links in sandbox output open only as http(s)', () => {
  const cases = [
    ['https://example.com/a?b=1', 'https://example.com/a?b=1'],
    ['http://localhost:3000/x', 'http://localhost:3000/x'],
    ['HTTPS://EXAMPLE.COM', 'https://example.com/'],
    ['javascript:alert(1)', null],
    ['data:text/html,<script>1</script>', null],
    ['file:///etc/passwd', null],
    ['vbscript:x', null],
    ['not a url', null],
  ];
  for (const [text, url] of cases) assert.equal(terminalLinkUrl(text), url, text);
});
