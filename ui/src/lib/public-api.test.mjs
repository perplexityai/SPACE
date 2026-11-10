import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { test } from 'node:test';

const apiUrl = 'data:text/javascript,export const api = { async get() {} }; export const adminApi = {}; export class ApiError extends Error {}';
const libraryUrl = new URL('./', import.meta.url).href;
const hook = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL?.startsWith(libraryUrl)) {
      if (specifier === './api') return { url: apiUrl, shortCircuit: true };
      if (specifier.startsWith('.') && !/\.[cm]?[jt]s$/.test(specifier)) {
        return nextResolve(`${specifier}.ts`, context);
      }
    }
    return nextResolve(specifier, context);
  },
});
const { api } = await import(apiUrl);
const {
  getPublicPage,
  PUBLIC_LIST_MAX_PAGES,
  PUBLIC_LIST_MAX_ITEMS,
  PUBLIC_LIST_PAGE_SIZE,
  PUBLIC_LIST_TIMEOUT_MS,
} = await import('./public-api.ts');
hook.deregister();

for (const [initialPath, firstPath, continuationPath] of [
  ['/templates?limit=2&cursor=start', '/templates?limit=2&cursor=start', '/templates?limit=2&cursor=next%2F%2B%3D'],
  ['/templates?limit=100000&cursor=start', '/templates?limit=1000&cursor=start', '/templates?limit=1000&cursor=next%2F%2B%3D'],
  ['/sandboxes/sbx_example/snapshots', '/sandboxes/sbx_example/snapshots?limit=1000', '/sandboxes/sbx_example/snapshots?limit=1000&cursor=next%2F%2B%3D'],
]) {
  test(`lists every page of ${initialPath}`, async (t) => {
    const requests = [];
    t.mock.method(api, 'get', async (path) => {
      requests.push(path);
      return requests.length === 1
        ? { data: ['first'], next_cursor: 'next/+=' }
        : { data: ['last'], next_cursor: null };
    });
    assert.deepEqual(await getPublicPage(initialPath), ['first', 'last']);
    assert.deepEqual(requests, [firstPath, continuationPath]);
  });
}

test('does not return a partial list when a later page fails', async (t) => {
  let requests = 0;
  t.mock.method(api, 'get', async () => {
    if (++requests === 1) return { data: ['first'], next_cursor: 'next' };
    throw new Error('service unavailable');
  });
  await assert.rejects(getPublicPage('/templates'), /service unavailable/);
});

test('an empty terminal page completes the list without another request', async (t) => {
  const get = t.mock.method(api, 'get', async () => ({ data: [], next_cursor: null }));
  assert.deepEqual(await getPublicPage('/templates'), []);
  assert.equal(get.mock.callCount(), 1);
});

for (const [name, path, cursors] of [
  ['repeated cursor', '/templates', ['next', 'next']],
  ['cursor cycle', '/templates', ['one', 'two', 'one']],
  ['initial cursor repeated', '/templates?cursor=start', ['start']],
]) {
  test(`${name}: rejects before requesting that page again`, async (t) => {
    let requests = 0;
    t.mock.method(api, 'get', async () => {
      if (requests >= cursors.length) throw new Error('followed a cursor twice');
      return { data: [++requests], next_cursor: cursors[requests - 1] };
    });
    await assert.rejects(getPublicPage(path), /repeated pagination cursor/);
    assert.equal(requests, cursors.length);
  });
}

const fullPage = Array(PUBLIC_LIST_PAGE_SIZE).fill('row');
const fullPages = PUBLIC_LIST_MAX_ITEMS / PUBLIC_LIST_PAGE_SIZE;
for (const [name, page, requests, error, count] of [
  ['page too large', () => ({ data: [...fullPage, 'extra'], next_cursor: null }), 1, /page that is too large/],
  ['total too large', (i) => ({ data: i <= fullPages ? fullPage : ['extra'], next_cursor: `page-${i}` }), fullPages + 1, /too large to load/],
  ['endless distinct cursors', (i) => ({ data: [], next_cursor: `page-${i}` }), PUBLIC_LIST_MAX_PAGES, /too many requests/],
  ['last allowed page', (i) => ({ data: [i], next_cursor: i < PUBLIC_LIST_MAX_PAGES ? `page-${i}` : null }), PUBLIC_LIST_MAX_PAGES, null, PUBLIC_LIST_MAX_PAGES],
  ['last allowed item', (i) => ({ data: fullPage, next_cursor: i < fullPages ? `page-${i}` : null }), fullPages, null, PUBLIC_LIST_MAX_ITEMS],
]) {
  test(`public list budget: ${name}`, async (t) => {
    let reads = 0;
    t.mock.method(api, 'get', async () => {
      if (++reads > requests) throw new Error('request budget exceeded');
      return page(reads);
    });
    const loading = getPublicPage('/templates');
    if (error) await assert.rejects(loading, error);
    else assert.equal((await loading).length, count);
    assert.equal(reads, requests);
  });
}

test('one deadline covers every page and cancels a stalled continuation', { timeout: 2_000 }, async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const signals = [];
  t.mock.method(api, 'get', async (_path, { signal } = {}) => {
    signals.push(signal);
    if (signals.length === 1) {
      t.mock.timers.tick(PUBLIC_LIST_TIMEOUT_MS / 2);
      return { data: ['first'], next_cursor: 'next' };
    }
    return new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
  });
  const loading = getPublicPage('/templates');
  const outcome = loading.then((value) => ({ value }), (error) => ({ error }));
  await Promise.resolve();
  assert.equal(signals.length, 2);
  assert.ok(signals[0] instanceof AbortSignal);
  assert.equal(signals[0], signals[1]);
  t.mock.timers.tick(PUBLIC_LIST_TIMEOUT_MS / 2 - 1);
  assert.equal(signals[1].aborted, false);
  t.mock.timers.tick(1);
  assert.equal((await outcome).error?.name, 'TimeoutError');
  assert.equal(signals[1].aborted, true);
  assert.equal(signals.length, 2);
});
