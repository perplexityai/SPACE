// A small client for the public (/v1) and admin (/admin/api/v1) APIs, signed
// in as the console account. The seeder and the driver's checks use it; the
// driver itself goes through the browser.
import { randomUUID } from 'node:crypto';

import type { Config } from './config.ts';

export class ApiError extends Error {
  readonly status: number;
  readonly body: string;
  constructor(status: number, method: string, path: string, body: string) {
    super(`${method} ${path} -> ${status}: ${body.slice(0, 300)}`);
    this.status = status;
    this.body = body;
  }
}

type Options = { idempotent?: boolean; timeoutMs?: number };

export class Api {
  private readonly base: string;
  private token: string | null = null;

  constructor(config: Config) {
    this.base = config.apiUrl;
  }

  async signIn(email: string, password: string): Promise<string> {
    const session = await this.request<{ token: string }>('POST', '/auth/v1/sessions', { email, password });
    this.token = session.token;
    return session.token;
  }

  admin<T>(method: string, path: string, body?: unknown, options?: Options): Promise<T> {
    return this.request<T>(method, `/admin/api/v1${path}`, body, options);
  }

  public<T>(method: string, path: string, body?: unknown, options?: Options): Promise<T> {
    return this.request<T>(method, `/v1${path}`, body, options);
  }

  async request<T>(method: string, path: string, body?: unknown, options: Options = {}): Promise<T> {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (options.idempotent) headers['idempotency-key'] = randomUUID();
    const response = await fetch(`${this.base}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(options.timeoutMs ?? 60_000),
    });
    const text = await response.text();
    if (!response.ok) throw new ApiError(response.status, method, path, text);
    return (text ? JSON.parse(text) : null) as T;
  }

  // Wait for a public or admin operation to finish.
  async waitForOperation(id: string, { admin = false, timeoutMs = 600_000 } = {}): Promise<Operation> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const op = admin
        ? await this.admin<Operation>('GET', `/operations/${id}`)
        : await this.public<Operation>('GET', `/operations/${id}`);
      if (['succeeded', 'failed', 'canceled', 'expired'].includes(op.state)) return op;
      if (Date.now() > deadline) throw new Error(`operation ${id} still ${op.state}`);
      await sleep(2000);
    }
  }
}

export interface Operation {
  id: string;
  state: string;
  target_id?: string | null;
  progress_message?: string | null;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
