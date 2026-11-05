import assert from 'node:assert/strict';
import { test } from 'node:test';
import { debounce, terminalEventText, terminalKeyAction, terminalNotice } from './terminal-pane.ts';

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

test('copy and paste keys go to the browser; everything else to the PTY', () => {
  const key = (k, mods = {}) => ({ key: k, ctrlKey: false, metaKey: false, altKey: false, ...mods });
  const cases = [
    [key('c', { ctrlKey: true }), true, 'copy'],
    [key('c', { metaKey: true }), true, 'copy'],
    [key('C', { ctrlKey: true, shiftKey: true }), true, 'copy'],
    // Without a selection Ctrl+C is the interrupt.
    [key('c', { ctrlKey: true }), false, 'terminal'],
    [key('v', { ctrlKey: true }), false, 'paste'],
    [key('v', { metaKey: true }), true, 'paste'],
    [key('d', { ctrlKey: true }), false, 'terminal'],
    [key('c'), true, 'terminal'],
    [key('c', { ctrlKey: true, altKey: true }), true, 'terminal'],
    [key('ArrowUp'), false, 'terminal'],
  ];
  for (const [event, hasSelection, action] of cases) {
    assert.equal(terminalKeyAction(event, hasSelection), action, JSON.stringify({ event, hasSelection }));
  }
});
