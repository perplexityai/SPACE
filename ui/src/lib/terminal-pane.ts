import type { TerminalSessionEvent } from './data-plane-terminal';

// Card resizes arrive in bursts (a window drag, the sider collapsing); the PTY
// is resized once the burst settles.
export const TERMINAL_RESIZE_DEBOUNCE_MS = 120;

// The data plane's default command is `/bin/sh`, which on many images is a
// shell without line editing: no history and no Tab completion. bash is
// interactive with both. It runs as a non-login shell, so a PATH from the
// sandbox's create env survives (a login shell's /etc/profile can reset it).
// The passwd shell is not used: for a service user it can be nologin or false.
export const TERMINAL_SHELL_COMMAND = [
  '/bin/sh',
  '-c',
  'if command -v bash >/dev/null 2>&1; then exec bash -i; fi; exec sh -i',
];

// xterm.js implements xterm-256color; full-screen programs need TERM to draw.
export const TERMINAL_ENV = { TERM: 'xterm-256color' };

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

export type TerminalInput = { kind: 'text'; data: string } | { kind: 'bytes'; data: Uint8Array };

// Holds what is typed between Connect and the shell's start, which the data
// plane would reject, and hands it over in order once the shell runs. Input
// with no session open, or after it ends, is dropped. Past `limit` bytes of
// held input (a large paste before the start) the rest is dropped.
export class TerminalInputGate {
  private held: TerminalInput[] = [];
  private heldBytes = 0;
  private state: 'closed' | 'opening' | 'started' = 'closed';
  private readonly limit: number;

  constructor(limit = 64 * 1024) {
    this.limit = limit;
  }

  // A new session is opening; anything held for an earlier one is dropped.
  open(): void {
    this.held = [];
    this.heldBytes = 0;
    this.state = 'opening';
  }

  // Returns the input to send now; while the session opens, holds it.
  write(input: TerminalInput): TerminalInput | null {
    if (this.state === 'started') return input;
    if (this.state === 'opening') {
      const size = input.kind === 'text' ? input.data.length : input.data.byteLength;
      if (this.heldBytes + size <= this.limit) {
        this.held.push(input);
        this.heldBytes += size;
      }
    }
    return null;
  }

  // The shell started: returns the held input, in order, to send first.
  start(): TerminalInput[] {
    if (this.state !== 'opening') return [];
    const held = this.held;
    this.held = [];
    this.heldBytes = 0;
    this.state = 'started';
    return held;
  }

  get started(): boolean {
    return this.state === 'started';
  }

  close(): void {
    this.held = [];
    this.heldBytes = 0;
    this.state = 'closed';
  }
}

// xterm hands some reports (X10 mouse coordinates past column 95) as a binary
// string, one byte per UTF-16 unit; UTF-8 encoding them would change them.
export function binaryStringToBytes(data: string): Uint8Array {
  const bytes = new Uint8Array(data.length);
  for (let index = 0; index < data.length; index += 1) {
    bytes[index] = data.charCodeAt(index) & 0xff;
  }
  return bytes;
}

// Sandbox output is untrusted: a link opens only as an http(s) URL.
export function terminalLinkUrl(text: string): string | null {
  let url: URL;
  try {
    url = new URL(text);
  } catch {
    return null;
  }
  return url.protocol === 'http:' || url.protocol === 'https:' ? url.href : null;
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
  shiftKey: boolean;
};

export function isMacPlatform(platform: string): boolean {
  return /Mac|iPhone|iPad|iPod/i.test(platform);
}

// The usual terminal convention. On macOS Cmd+C copies a selection and Cmd+V
// pastes; elsewhere Ctrl+Shift+C and Ctrl+Shift+V do. Ctrl+C and Ctrl+V always
// go to the shell: the interrupt, and readline's quoted insert or vim's visual
// block. `copy` copies the selection; `paste` leaves the key to the browser,
// whose paste event the terminal writes to the PTY; `ignore` is a copy key
// with nothing selected; `terminal` is every other key.
export type TerminalKeyAction = 'copy' | 'paste' | 'ignore' | 'terminal';

export function terminalKeyAction(event: KeyLike, hasSelection: boolean, mac: boolean): TerminalKeyAction {
  const key = event.key.toLowerCase();
  if (key !== 'c' && key !== 'v') return 'terminal';
  const chord = mac
    ? event.metaKey && !event.ctrlKey && !event.altKey
    : event.ctrlKey && event.shiftKey && !event.metaKey && !event.altKey;
  if (!chord) return 'terminal';
  if (key === 'v') return 'paste';
  return hasSelection ? 'copy' : 'ignore';
}
