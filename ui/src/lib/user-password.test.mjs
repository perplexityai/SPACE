import assert from 'node:assert/strict';
import { test } from 'node:test';
import { passwordChangeOutcome } from './user-password.ts';

const external = { mode: 'external', providerName: 'Company SSO', logoutUrl: null };

test('password mode: your own change ends this console session', () => {
  for (const mode of [{ mode: 'password' }, null, undefined]) {
    const self = passwordChangeOutcome(mode, true);
    assert.equal(self.endsConsoleSession, true);
    assert.match(self.notice, /including this one/);
    assert.equal(self.createHint, null);
    const other = passwordChangeOutcome(mode, false);
    assert.equal(other.endsConsoleSession, false);
    assert.match(other.notice, /Your own session is not affected/);
  }
});

test('external mode: a password can be rotated without signing the console out', () => {
  for (const isSelf of [true, false]) {
    const outcome = passwordChangeOutcome(external, isSelf);
    assert.equal(outcome.endsConsoleSession, false, `isSelf=${isSelf}`);
    assert.match(outcome.notice, /password sessions/);
    assert.match(outcome.notice, /Company SSO is unaffected/);
    assert.match(outcome.createHint, /Company SSO/);
    assert.match(outcome.createHint, /strong, random/);
  }
});
