// Page helpers shared by the steps and the capture flows.
import type { Browser, BrowserContext, Locator, Page } from 'playwright';

import type { Config } from './config.ts';
import type { Recorder } from './recorder.ts';
import type { SeedState } from './seed.ts';

export interface Ctx {
  page: Page;
  context: BrowserContext;
  rec: Recorder;
  config: Config;
  seed: SeedState | null;
  // A per-run tag for objects a mutating step creates and deletes again.
  runTag: string;
  // Shared between steps of one run (for example a sandbox a step created).
  shared: Record<string, string>;
}

export async function newContext(
  browser: Browser,
  { viewport = { width: 1440, height: 900 }, videoDir }: { viewport?: { width: number; height: number }; videoDir?: string } = {},
): Promise<BrowserContext> {
  const context = await browser.newContext({ viewport, ...(videoDir ? { recordVideo: { dir: videoDir, size: viewport } } : {}) });
  // The `next dev` issue badge sits over the sidebar's Log out button.
  await context.addInitScript({
    content: "document.addEventListener('DOMContentLoaded', () => { const s = document.createElement('style'); s.textContent = 'nextjs-portal { display: none !important; }'; document.head.appendChild(s); });",
  });
  // Record antd message toasts, which vanish before a screenshot.
  await context.addInitScript(() => {
    const w = window as unknown as { __toasts: { type: string; text: string }[] };
    w.__toasts = [];
    const observe = () => new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        for (const node of Array.from(mutation.addedNodes)) {
          const el = node as HTMLElement;
          if (!el.querySelectorAll) continue;
          const notices = el.matches?.('.ant-message-notice') ? [el] : Array.from(el.querySelectorAll('.ant-message-notice'));
          for (const notice of notices) {
            const type = notice.querySelector('.ant-message-error') ? 'error'
              : notice.querySelector('.ant-message-warning') ? 'warning'
              : notice.querySelector('.ant-message-success') ? 'success' : 'info';
            w.__toasts.push({ type, text: (notice as HTMLElement).innerText.trim() });
          }
        }
      }
    }).observe(document.documentElement, { childList: true, subtree: true });
    if (document.documentElement) observe(); else document.addEventListener('DOMContentLoaded', observe);
  });
  return context;
}

export async function takeToasts(page: Page): Promise<{ type: string; text: string }[]> {
  return page.evaluate(() => {
    const w = window as unknown as { __toasts?: { type: string; text: string }[] };
    // A notice can be seen twice as antd mounts it; report each text once.
    const seen = new Set<string>();
    const out = (w.__toasts ?? []).filter((t) => !seen.has(`${t.type}|${t.text}`) && seen.add(`${t.type}|${t.text}`) !== undefined);
    w.__toasts = [];
    return out;
  });
}

export async function settle(page: Page, ms = 1500): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  // Most pages poll, so the network may never go idle; bound the wait.
  await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
  await page.waitForTimeout(ms);
}

export async function visit(c: Ctx, route: string, ms = 1500): Promise<void> {
  await c.page.goto(`${c.config.baseUrl}${route}`, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await settle(c.page, ms);
}

// The page body without the sidebar, so menu labels never satisfy a check.
export function content(page: Page): Locator {
  return page.locator('main.ant-layout-content, .ant-layout-content').first();
}

export async function text(page: Page): Promise<string> {
  const main = content(page);
  const target = (await main.count()) ? main : page.locator('body');
  return (await target.innerText()).replace(/\s+/g, ' ');
}

export async function pickSelect(page: Page, select: Locator, option: string): Promise<void> {
  await select.click();
  const item = page.locator('.ant-select-dropdown:visible .ant-select-item-option', { hasText: option }).first();
  await item.waitFor({ timeout: 5000 });
  await item.click();
}

export async function confirmPopover(page: Page, okText?: string): Promise<void> {
  const pop = page.locator('.ant-popover:visible, .ant-popconfirm:visible').last();
  await pop.waitFor({ timeout: 5000 });
  const button = okText
    ? pop.getByRole('button', { name: new RegExp(`(^|\\s)${okText}$`) })
    : pop.locator('.ant-btn-primary, .ant-btn-dangerous').last();
  await button.click();
}

// An admin API call from the page, with the console's own session.
export async function adminFetch(page: Page, method: string, apiPath: string, body?: unknown): Promise<{ status: number; json: any }> {
  return page.evaluate(async ({ method, apiPath, body }) => {
    const token = localStorage.getItem('token');
    const response = await fetch(apiPath, {
      method,
      headers: {
        'content-type': 'application/json',
        ...(token ? { authorization: `Bearer ${token}` } : {}),
        ...(method !== 'GET' ? { 'idempotency-key': crypto.randomUUID() } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const raw = await response.text();
    let json: unknown = null;
    try { json = JSON.parse(raw); } catch { /* not JSON */ }
    return { status: response.status, json };
  }, { method, apiPath, body });
}

export async function signIn(c: Ctx, email: string, password: string): Promise<void> {
  await c.page.evaluate(() => localStorage.removeItem('token')).catch(() => {});
  await visit(c, '/login', 1000);
  await c.page.getByLabel('Email').fill(email);
  await c.page.getByLabel('Password').fill(password);
  await c.page.getByRole('button', { name: /sign in|log in|login/i }).click();
  await c.page.waitForURL(/\/dashboard/, { timeout: 20_000 }).catch(() => {});
  await settle(c.page, 2000);
}

export async function waitForStatus(c: Ctx, id: string, want: string[], timeoutMs = 180_000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  let last = '';
  while (Date.now() < deadline) {
    const response = await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`);
    last = response.json?.status ?? `http ${response.status}`;
    if (want.includes(last)) return last;
    await c.page.waitForTimeout(2000);
  }
  return last;
}

// The last 8 hex digits, the short label the console shows for a sandbox ID.
export function shortLabel(id: string): string {
  return id.replace(/^sbx_/, '').replaceAll('-', '').slice(-8);
}
