import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseSandboxRef, publicSandboxIdFromAny } from './sandbox-id.ts';

const uuid = '01a19089-216f-751e-9f56-1906865f5c23';
const publicId = `sbx_${uuid}`;

for (const [name, input] of [
  ['public ID', publicId],
  ['bare UUID', uuid],
  ['undashed UUID', uuid.replaceAll('-', '')],
  ['upper case', publicId.toUpperCase().replace('SBX_', 'sbx_')],
  ['upper-case prefix', `SBX_${uuid}`],
  ['log tag', `sandbox:${uuid}`],
  ['pod name', `space-sbx-${uuid.replaceAll('-', '')}-g1`],
  ['pod name of a later generation', `space-sbx-${uuid.replaceAll('-', '')}-g12`],
  ['pod name without generation', `space-sbx-${uuid.replaceAll('-', '')}`],
  ['surrounding whitespace and quotes', `  "${publicId}" `],
]) {
  test(`parseSandboxRef accepts a ${name}`, () => {
    assert.deepEqual(parseSandboxRef(input), { uuid, publicId });
  });
}

for (const [name, input] of [
  ['empty', ''],
  ['name', 'uiaudit-running'],
  ['short label', '865f5c23'],
  ['truncated UUID', uuid.slice(0, 30)],
  ['prefixed garbage', 'sbx_not-a-uuid'],
  ['other pod', `space-net-${uuid.replaceAll('-', '')}-g1`],
]) {
  test(`parseSandboxRef rejects a ${name}`, () => {
    assert.equal(parseSandboxRef(input), null);
  });
}

test('publicSandboxIdFromAny canonicalizes every accepted form', () => {
  assert.equal(publicSandboxIdFromAny(uuid), publicId);
  assert.equal(publicSandboxIdFromAny(`space-sbx-${uuid.replaceAll('-', '')}-g3`), publicId);
  assert.equal(publicSandboxIdFromAny(publicId), publicId);
});
