import assert from 'node:assert/strict';
import { test } from 'node:test';
import { theme } from 'antd';
import { createCuriosityTheme } from './curiosity-theme.ts';

test('organization accents derive matching interaction states without changing surfaces or statuses', () => {
  for (const isDark of [false, true]) {
    const primaryColor = '#b54708';
    const base = createCuriosityTheme({ isDark });
    const customized = createCuriosityTheme({ isDark, primaryColor });
    const actual = theme.getDesignToken(customized.config);
    const expectedAccent = theme.getDesignToken({
      algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
      token: { colorPrimary: primaryColor, colorLink: primaryColor },
    });

    for (const key of ['colorPrimary', 'colorPrimaryHover', 'colorPrimaryActive', 'colorPrimaryBg',
      'colorPrimaryBorder', 'colorLink', 'colorLinkHover', 'colorLinkActive']) {
      assert.equal(actual[key], expectedAccent[key], `${key}, dark=${isDark}`);
    }
    assert.notEqual(actual.colorPrimary, theme.getDesignToken(base.config).colorPrimary);
    assert.deepEqual(customized.surfaces, base.surfaces);
    for (const key of ['colorSuccess', 'colorWarning', 'colorError', 'colorInfo', 'cyan7', 'green7']) {
      assert.equal(actual[key], theme.getDesignToken(base.config)[key]);
    }
    assert.equal(customized.config.components.Menu.itemSelectedColor, actual.colorPrimary);
    assert.equal(customized.config.components.Select.optionSelectedBg, actual.colorPrimaryBg);
  }
});

test('unset organization accents use the preset and explicit palette values survive token resolution', () => {
  for (const isDark of [false, true]) {
    const base = createCuriosityTheme({ isDark });
    for (const primaryColor of [null, '', '   ']) {
      assert.deepEqual(createCuriosityTheme({ isDark, primaryColor }).config.token, base.config.token);
    }
    const resolved = theme.getDesignToken(base.config);
    for (const key of ['colorPrimary', 'colorText', 'colorBgContainer', 'colorSuccess', 'blue1', 'purple7']) {
      assert.equal(resolved[key], base.config.token[key], `${key}, dark=${isDark}`);
    }
    // Both spellings resolve to the same CSS variable in Ant Design 6.
    for (const color of ['blue', 'geekblue', 'cyan', 'green', 'gold', 'orange', 'red', 'purple']) {
      for (const shade of [1, 2, 3, 6, 7]) {
        assert.equal(resolved[`${color}-${shade}`], resolved[`${color}${shade}`]);
      }
    }
    assert.equal(resolved.colorBgLayout, base.surfaces.page);
    assert.match(base.config.components.Table.headerBg, /^#[\da-f]{6}$/i);
    assert.match(base.config.components.Table.rowHoverBg, /^#[\da-f]{6}$/i);
  }
});
