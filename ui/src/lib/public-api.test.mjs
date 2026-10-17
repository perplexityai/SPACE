import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { test } from 'node:test';

const apiUrl = 'data:text/javascript,export const api = { async get() {} }; export const adminApi = {};';
const hook = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === new URL('./public-api.ts', import.meta.url).href) {
      if (specifier === './api') return { url: apiUrl, shortCircuit: true };
      if (specifier === './sandbox-id') return nextResolve('./sandbox-id.ts', context);
    }
    return nextResolve(specifier, context);
  },
});
const { api } = await import(apiUrl);
const { getPublicPage } = await import('./public-api.ts');
hook.deregister();

for (const [initialPath, continuationPath] of [
  ['/templates?limit=2&cursor=start', '/templates?limit=2&cursor=next%2F%2B%3D'],
  ['/sandboxes/sbx_example/snapshots', '/sandboxes/sbx_example/snapshots?cursor=next%2F%2B%3D'],
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
    assert.deepEqual(requests, [initialPath, continuationPath]);
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
