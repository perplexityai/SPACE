// Per-step browser evidence: console errors, page errors, failed and slow
// requests, a full-page screenshot, and the step's own checks.
import fs from 'node:fs';
import path from 'node:path';

import type { Page, Request, Response } from 'playwright';

// `slow` and `devwarn` (React and antd development-build warnings, absent
// from a production build) are reported but never fail a comparison.
export type EventKind = 'console' | 'pageerror' | 'http' | 'netfail' | 'slow' | 'devwarn' | 'check' | 'exception';
export const NON_FAILING: readonly EventKind[] = ['slow', 'devwarn'];

// A step that fabricates a response with `page.route` (to exercise an error
// path) sets this header on it, so the recorder does not report it.
export const SYNTHETIC_HEADER = 'x-console-loop-synthetic';

export interface StepEvent {
  kind: EventKind;
  message: string;
  url?: string;
  status?: number;
  body?: string;
  durationMs?: number;
  // `<step>|<type>`: the type part identifies the error across runs, with
  // IDs, numbers and seeded names replaced by placeholders.
  signature: string;
}

export interface StepResult {
  index: number;
  name: string;
  url: string;
  screenshot: string | null;
  durationMs: number;
  ok: boolean;
  events: StepEvent[];
  notes: string[];
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
// An API key's secret is 64 hex characters; nothing of that shape is ever
// written to a run directory.
const SECRET = /\b[0-9a-f]{64}\b/gi;
export const redact = (value: string): string => value.replace(SECRET, '<redacted>');

export function normalizer(prefix: string): (value: string) => string {
  const seeded = new RegExp(`${prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}-[a-z0-9-]+`, 'g');
  return (value) => value
    .replace(/sbx_[0-9a-f-]{32,36}/gi, 'sbx_<id>')
    .replace(UUID, '<uuid>')
    .replace(/[0-9a-f]{32,}/gi, '<hex>')
    .replace(/\b\d{2,}\b/g, '<n>')
    .replace(seeded, '<seeded>');
}

// Development-build warnings that a production build does not print: React
// 19 logs its development warnings with console.error, as format strings
// whose `%s` placeholders reach the console unformatted, and antd logs its
// deprecation warnings as "Warning: [antd: …]". Any other console error,
// including React errors without placeholders, is a `console` event.
export function isDevWarning(text: string): boolean {
  return /^Warning: /.test(text) || /%[sdo]/.test(text);
}

export class Recorder {
  private events: StepEvent[] = [];
  private notes: string[] = [];
  private readonly started = new Map<Request, number>();
  // The step during which each request was issued.
  private readonly issuedBy = new Map<Request, string>();
  private stepName = '';
  private pending: Promise<unknown>[] = [];
  private readonly page: Page;
  private readonly slowMs: number;
  private readonly normalize: (value: string) => string;
  // Where this run's screenshots and summary go.
  readonly outDir: string;

  constructor(page: Page, { slowMs, prefix, outDir }: { slowMs: number; prefix: string; outDir: string }) {
    this.page = page;
    this.outDir = outDir;
    this.slowMs = slowMs;
    this.normalize = normalizer(prefix);
    page.on('console', (msg) => {
      if (msg.type() !== 'error') return;
      const text = msg.text();
      // Chrome echoes every failed request as a console error; the http
      // event already carries it, with its body.
      if (/^Failed to load resource: the server responded with a status of/.test(text)) return;
      const kind: EventKind = isDevWarning(text) ? 'devwarn' : 'console';
      this.add({ kind, message: text.slice(0, 600), signature: `${kind}|${this.normalize(text).slice(0, 160)}` });
    });
    page.on('pageerror', (err) => {
      // A minified error class's name changes between builds; its message does not.
      this.add({ kind: 'pageerror', message: `${err.name}: ${err.message}`.slice(0, 600), signature: `pageerror|${this.normalize(err.message).slice(0, 160)}` });
    });
    page.on('request', (req) => {
      this.started.set(req, Date.now());
      this.issuedBy.set(req, this.stepName);
    });
    page.on('requestfailed', (req) => {
      const failure = req.failure()?.errorText ?? 'failed';
      // Requests cut off by a navigation are expected.
      if (failure === 'net::ERR_ABORTED') return;
      if (this.late(req, `${req.method()} ${req.url()} failed: ${failure}`)) return;
      this.add({ kind: 'netfail', message: `${req.method()} ${req.url()}: ${failure}`, url: req.url(), signature: `netfail|${req.method()} ${this.pathOf(req.url())}|${failure}` });
    });
    page.on('response', (res) => this.pending.push(this.onResponse(res)));
  }

  private pathOf(url: string): string {
    try {
      const u = new URL(url);
      return this.normalize(u.pathname + (u.search ? `?${[...u.searchParams.keys()].sort().join('&')}` : ''));
    } catch {
      return this.normalize(url);
    }
  }

  // A response or failure of a request an earlier step issued belongs to
  // that step, which has already been written: note it here instead of
  // counting it against this step.
  private late(req: Request, what: string): boolean {
    const issuer = this.issuedBy.get(req);
    if (issuer === undefined || issuer === this.stepName) return false;
    this.notes.push(redact(`late, from step ${issuer || '(before the first step)'}: ${what}`));
    return true;
  }

  private async onResponse(res: Response): Promise<void> {
    if (res.headers()[SYNTHETIC_HEADER]) return;
    const req = res.request();
    if (res.status() >= 400 && this.late(req, `${req.method()} ${res.url()} -> ${res.status()}`)) return;
    const startedAt = this.started.get(req);
    const status = res.status();
    if (status >= 400) {
      let body = '';
      try { body = (await res.text()).slice(0, 500); } catch { body = '<unreadable>'; }
      this.add({ kind: 'http', message: `${req.method()} ${res.url()} -> ${status}`, url: res.url(), status, body, signature: `http|${req.method()} ${this.pathOf(res.url())}|${status}` });
    }
    if (startedAt === undefined) return;
    try { await res.finished(); } catch { /* a stream */ }
    const durationMs = Date.now() - startedAt;
    const type = req.resourceType();
    // Long-lived streams and React Server Component fetches are not requests
    // an operator waits on.
    if (durationMs >= this.slowMs && ['fetch', 'xhr', 'document'].includes(type) && !/\/events(\?|$)|\/attach|metrics-stream|[?&]_rsc=/.test(res.url())) {
      if (this.late(req, `${req.method()} ${res.url()} took ${durationMs} ms`)) return;
      this.add({ kind: 'slow', message: `${req.method()} ${res.url()} took ${durationMs} ms`, url: res.url(), status, durationMs, signature: `slow|${req.method()} ${this.pathOf(res.url())}` });
    }
  }

  add(event: StepEvent): void {
    this.events.push({
      ...event,
      message: redact(event.message),
      body: event.body === undefined ? undefined : redact(event.body),
      url: event.url === undefined ? undefined : redact(event.url),
      signature: `${this.stepName}|${redact(event.signature)}`,
    });
  }

  // A failed check is an event; a passed one is a note in the summary.
  check(ok: boolean, message: string): void {
    // Counts in a message vary with the data; the check's identity does not.
    if (ok) this.notes.push(redact(`ok: ${message}`));
    else this.add({ kind: 'check', message, signature: `check|${this.normalize(message).replace(/\d+/g, '<n>').replace(/\([^)]*\)/g, '(…)').slice(0, 160)}` });
  }

  note(message: string): void {
    this.notes.push(redact(message));
  }

  begin(name: string): void {
    this.stepName = name;
    this.events = [];
    this.notes = [];
    this.pending = [];
  }

  async end(index: number, startedAt: number, failure: unknown): Promise<StepResult> {
    if (failure) {
      const text = failure instanceof Error ? failure.message.split('\n')[0] : String(failure);
      this.add({ kind: 'exception', message: text.slice(0, 600), signature: `exception|${this.normalize(text).slice(0, 160)}` });
    }
    // Let in-flight responses settle so their bodies land in this step.
    await this.page.waitForTimeout(400);
    await Promise.race([Promise.allSettled(this.pending), this.page.waitForTimeout(3000)]);
    const file = `${String(index).padStart(3, '0')}-${this.stepName.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}.png`;
    let screenshot: string | null = path.join(this.outDir, file);
    try {
      await this.page.screenshot({ path: screenshot, fullPage: true, timeout: 15_000 });
    } catch {
      screenshot = null;
    }
    const events = this.events;
    return {
      index,
      name: this.stepName,
      url: this.page.url(),
      screenshot: screenshot ? path.basename(screenshot) : null,
      durationMs: Date.now() - startedAt,
      ok: !events.some((event) => !NON_FAILING.includes(event.kind)),
      events,
      notes: this.notes,
    };
  }
}

export function writeJson(file: string, value: unknown): void {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(value, null, 2));
  fs.renameSync(`${file}.tmp`, file);
}
