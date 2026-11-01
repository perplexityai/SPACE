import assert from 'node:assert/strict';
import { test } from 'node:test';
import { networkPolicyTagText } from './network-policy.ts';

test('the tag names the policy and its version', () => {
  assert.equal(networkPolicyTagText('closed', 1, 1).label, 'closed · v1');
  assert.equal(networkPolicyTagText(null, 3, 3).label, 'v3');
});

test('the tooltip says what the policy does', () => {
  assert.match(networkPolicyTagText('closed', 1, 1).meaning, /deny all/);
  assert.match(networkPolicyTagText('open', 1, 1).meaning, /allow all/);
  assert.match(networkPolicyTagText('custom', 2, 2).meaning, /custom/);
});

test('the tooltip says whether the node applied the latest version', () => {
  assert.match(networkPolicyTagText('closed', 1, null).sync, /not yet reported/);
  assert.match(networkPolicyTagText('closed', 3, 2).sync, /applies version 2; version 3 is pending/);
  assert.match(networkPolicyTagText('closed', 2, 3).sync, /newer than the desired/);
  assert.equal(networkPolicyTagText('closed', 2, 2).sync, 'Version 2, applied by the node.');
});
