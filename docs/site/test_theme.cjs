const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = readFileSync(join(__dirname, 'theme/system-theme.js'), 'utf8');

function browser(dark, saved, blocked = false) {
  const listeners = [];
  const media = { matches: dark, addEventListener: (_, callback) => listeners.push(callback) };
  const storage = new Map([['mdbook-theme', saved]]);
  const root = { dataset: {} };
  const context = {
    document: { documentElement: root },
    window: { matchMedia: () => media },
    get localStorage() {
      if (blocked) throw new Error('Storage is unavailable');
      return { getItem: key => storage.get(key) ?? null };
    },
  };
  return {
    root,
    load: () => vm.runInNewContext(script, context),
    changeSystem(value) {
      media.matches = value;
      for (const listener of listeners) listener({ matches: value });
      media.onchange?.({ matches: value });
    },
  };
}

test('system preference wins over legacy Guide theme choices', () => {
  for (const dark of [false, true]) for (const saved of [null, 'light', 'rust', 'coal', 'navy', 'ayu']) {
    const page = browser(dark, saved);
    page.load();
    assert.equal(page.root.dataset.docsTheme, dark ? 'dark' : 'light');
  }
});

test('an open page keeps its mode until the next load', () => {
  for (const dark of [false, true]) {
    const page = browser(dark, null);
    page.load();
    page.changeSystem(!dark);
    assert.equal(page.root.dataset.docsTheme, dark ? 'dark' : 'light');
    page.load();
    assert.equal(page.root.dataset.docsTheme, dark ? 'light' : 'dark');
  }
});

test('blocked storage does not prevent system theme selection', () => {
  for (const dark of [false, true]) {
    const page = browser(dark, 'light', true);
    page.load();
    assert.equal(page.root.dataset.docsTheme, dark ? 'dark' : 'light');
  }
});
