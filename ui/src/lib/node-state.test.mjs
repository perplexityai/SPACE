import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cordonedText, runtimeInstallationLines } from './node-state.ts';

test('cordon reads as yes or no where reported, and as nothing where not (Direct, older releases)', () => {
  assert.equal(cordonedText(true), 'yes');
  assert.equal(cordonedText(false), 'no');
  assert.equal(cordonedText(undefined), null);
});

test('runtime installations list each runtime, version and guest; an empty report differs from none sent', () => {
  assert.deepEqual(
    runtimeInstallationLines([
      { runtime: 'fc', runtime_version: '1.15.1', guests: ['linux/x86_64', 'linux/aarch64'], observed_at: '2026-11-03T00:00:00Z' },
      { runtime: 'other', runtime_version: '2', guests: [], observed_at: '2026-11-03T00:00:00Z' },
    ]),
    ['fc 1.15.1 · linux/x86_64, linux/aarch64', 'other 2'],
  );
  assert.deepEqual(runtimeInstallationLines([]), []);
  assert.equal(runtimeInstallationLines(undefined), null);
});
