import assert from 'node:assert/strict';
import { test } from 'node:test';

// Each import with its own query string is a fresh module instance, which
// stands in for a separate browser tab. They share one fake localStorage, as
// real tabs of one origin do.
function installSharedStorage() {
  const data = new Map();
  globalThis.window = {
    localStorage: {
      getItem: (k) => (data.has(k) ? data.get(k) : null),
      setItem: (k, v) => void data.set(k, String(v)),
      removeItem: (k) => void data.delete(k),
    },
    setInterval: globalThis.setInterval.bind(globalThis),
    clearInterval: globalThis.clearInterval.bind(globalThis),
  };
}

const openTab = (name) => import(`./view-refresh.ts?tab=${name}`);

test('a refresh in one tab does not hold another tab back', async () => {
  installSharedStorage();
  const [a, b] = [await openTab('a1'), await openTab('b1')];
  const t0 = 1_000_000;

  assert.equal(a.shouldRefreshView('list', 10_000, t0), true);
  assert.equal(b.shouldRefreshView('list', 10_000, t0), true);

  a.markViewRefreshed('list', t0);
  assert.equal(a.shouldRefreshView('list', 10_000, t0 + 5_000), false);
  assert.equal(b.shouldRefreshView('list', 10_000, t0 + 5_000), true);

  b.markViewRefreshed('list', t0 + 5_000);
  // Each tab comes due 10 s after its own refresh, not the other tab's.
  assert.equal(a.shouldRefreshView('list', 10_000, t0 + 10_000), true);
  assert.equal(b.shouldRefreshView('list', 10_000, t0 + 10_000), false);
  assert.equal(b.shouldRefreshView('list', 10_000, t0 + 15_000), true);
});

test('views are stamped independently by key', async () => {
  installSharedStorage();
  const a = await openTab('a2');
  a.markViewRefreshed('list', 1_000_000);
  assert.equal(a.shouldRefreshView('list:org-x:limit-5', 10_000, 1_000_001), true);
  assert.equal(a.shouldRefreshView('list', 10_000, 1_000_001), false);
});

test('a locked refresh stamps only its own tab, and skips while still fresh', async () => {
  installSharedStorage();
  const [a, b] = [await openTab('a3'), await openTab('b3')];
  let fetchesA = 0;
  let fetchesB = 0;

  assert.equal(await a.runLockedViewRefresh('list', async () => void fetchesA++), true);
  assert.equal(await a.runLockedViewRefresh('list', async () => void fetchesA++), false);
  assert.equal(fetchesA, 1);

  // The other tab has not refreshed yet, so it fetches despite tab A's refresh.
  assert.equal(await b.runLockedViewRefresh('list', async () => void fetchesB++), true);
  assert.equal(fetchesB, 1);
});

test('a forced refresh runs even when the view is fresh', async () => {
  installSharedStorage();
  const a = await openTab('a4');
  let fetches = 0;
  await a.runLockedViewRefresh('list', async () => void fetches++);
  assert.equal(await a.runLockedViewRefresh('list', async () => void fetches++, { force: true }), true);
  assert.equal(fetches, 2);
});

test('two tabs do not fetch at the same instant', async () => {
  installSharedStorage();
  const [a, b] = [await openTab('a5'), await openTab('b5')];
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  let fetchesB = 0;

  const first = a.runLockedViewRefresh('list', () => gate);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(await b.runLockedViewRefresh('list', async () => void fetchesB++), false);
  assert.equal(fetchesB, 0);

  release();
  assert.equal(await first, true);
  // Once the first fetch ends, the other tab's next check refreshes.
  assert.equal(await b.runLockedViewRefresh('list', async () => void fetchesB++), true);
});
