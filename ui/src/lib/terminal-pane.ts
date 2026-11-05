import type { TerminalSessionEvent } from './data-plane-terminal';

// Card resizes arrive in bursts (a window drag, the sider collapsing); the PTY
// is resized once the burst settles.
export const TERMINAL_RESIZE_DEBOUNCE_MS = 120;

type Timers = {
  setTimeout: (callback: () => void, ms: number) => unknown;
  clearTimeout: (handle: any) => void;
};

export type Debouncer = {
  schedule: () => void;
  cancel: () => void;
};

// Runs `callback` once, `delayMs` after the last `schedule` call.
export function debounce(callback: () => void, delayMs: number, timers: Timers = globalThis): Debouncer {
  let handle: unknown = null;
  const cancel = () => {
    if (handle !== null) {
      timers.clearTimeout(handle);
      handle = null;
    }
  };
  return {
    schedule() {
      cancel();
      handle = timers.setTimeout(() => {
        handle = null;
        callback();
      }, delayMs);
    },
    cancel,
  };
}

// Notices are the console's own words, not the shell's: dim, on a line of
// their own. A PTY wants CRLF, so the notice ends at column 0 for whatever the
// shell writes next.
export function terminalNotice(message: string): string {
  return `\r\n\x1b[2m[${message}]\x1b[22m\r\n`;
}

// What the terminal writes for one session event, or null for none. Output is
// written exactly as the PTY sent it, after the session's per-stream decoder.
export function terminalEventText(event: TerminalSessionEvent): string | null {
  switch (event.type) {
    case 'output':
      return event.text || null;
    case 'status':
      return event.message ? terminalNotice(event.message) : null;
    case 'started':
    case 'accepted':
      return null;
    case 'exited':
      return terminalNotice(`process exited: ${event.status} ${event.exitCode}`);
    case 'reconnecting':
      return terminalNotice(event.message);
    case 'error':
      return terminalNotice(`error: ${event.message}`);
  }
}

type KeyLike = {
  key: string;
  ctrlKey: boolean;
  metaKey: boolean;
  altKey: boolean;
};

// `copy` and `paste` leave the key to the browser, whose copy and paste events
// the terminal handles: copy puts the selection on the clipboard, paste writes
// the clipboard to the PTY. Every other key goes to the PTY, so Ctrl+C with no
// selection is still an interrupt.
export type TerminalKeyAction = 'copy' | 'paste' | 'terminal';

export function terminalKeyAction(event: KeyLike, hasSelection: boolean): TerminalKeyAction {
  if (event.altKey || (!event.ctrlKey && !event.metaKey)) return 'terminal';
  const key = event.key.toLowerCase();
  if (key === 'c' && hasSelection) return 'copy';
  if (key === 'v') return 'paste';
  return 'terminal';
}
