import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  CONSOLE_REQUEST_HEADER,
  PASSWORD_MODE,
  accessProblemForStatus,
  consoleRequestHeaders,
  parseConsoleAuthConfig,
  safeLogoutUrl,
} from './console-auth.ts';

test('password discovery parses to password mode', () => {
  assert.deepEqual(parseConsoleAuthConfig({ mode: 'password' }), PASSWORD_MODE);
});

test('external discovery keeps the provider name and a safe logout URL', () => {
  assert.deepEqual(
    parseConsoleAuthConfig({
      mode: 'external',
      provider_name: '  Company SSO ',
      logout_url: '/sso/logout',
    }),
    { mode: 'external', providerName: 'Company SSO', logoutUrl: '/sso/logout' },
  );
});

test('external discovery defaults the provider name and drops a missing logout URL', () => {
  assert.deepEqual(parseConsoleAuthConfig({ mode: 'external' }), {
    mode: 'external',
    providerName: 'Single sign-on',
    logoutUrl: null,
  });
});

test('unknown or malformed discovery is not guessed', () => {
  for (const body of [null, undefined, 'password', 42, {}, { mode: 'oidc' }, { mode: ['external'] }]) {
    assert.equal(parseConsoleAuthConfig(body), null, JSON.stringify(body));
  }
});

test('logout URLs are limited to same-origin paths and https', () => {
  for (const url of ['/logout', '/sso/logout?x=1', 'https://sso.example.com/logout']) {
    assert.equal(safeLogoutUrl(url), url);
  }
  for (const url of [
    '',
    '   ',
    '//evil.example.com/logout',
    '/\\evil.example.com',
    'http://sso.example.com/logout',
    'javascript:alert(1)',
    'data:text/html,hi',
    'https://user:pass@sso.example.com/logout',
    '/log out',
    '/logout\n',
    'logout',
    42,
    null,
  ]) {
    assert.equal(safeLogoutUrl(url), null, JSON.stringify(url));
  }
});

test('an unsafe logout URL in discovery is dropped, not followed', () => {
  assert.equal(
    parseConsoleAuthConfig({ mode: 'external', logout_url: 'javascript:alert(1)' }).logoutUrl,
    null,
  );
});

test('external mode never sends a bearer token and always marks console requests', () => {
  const headers = consoleRequestHeaders(
    { mode: 'external', providerName: 'SSO', logoutUrl: null },
    'stale-password-session',
  );
  assert.deepEqual(headers, { [CONSOLE_REQUEST_HEADER]: '1', 'X-Requested-With': 'XMLHttpRequest' });
  assert.equal(Object.keys(headers).some((name) => name.toLowerCase() === 'authorization'), false);
});

test('password mode sends the stored session token only', () => {
  assert.deepEqual(consoleRequestHeaders(PASSWORD_MODE, 'tok'), { Authorization: 'Bearer tok' });
  assert.deepEqual(consoleRequestHeaders(PASSWORD_MODE, null), {});
});

test('status codes map to the access problem shown on the login page', () => {
  assert.equal(accessProblemForStatus(401), 'unauthenticated');
  assert.equal(accessProblemForStatus(403), 'forbidden');
  assert.equal(accessProblemForStatus(503), 'unavailable');
  assert.equal(accessProblemForStatus(502), 'unavailable');
  assert.equal(accessProblemForStatus(500), 'error');
});
