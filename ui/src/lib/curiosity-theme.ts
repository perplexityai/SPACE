import { theme } from 'antd';
import type { ThemeConfig } from 'antd';

type Tokens = NonNullable<ThemeConfig['token']>;

// Curiosity subtly adjusts accent, status, and neutral hues.
const palettes = {
  light: {
    page: '#f8fafb', surface: '#fdfefe', sidebar: '#e8edf0', text: '#172b39',
    accent: '#007aa6', accentHover: '#006083', accentActive: '#004764', accentText: '#006a92',
    status: {
      Success: ['#007748', '#007748'], Warning: ['#a23423', '#a23423'],
      Error: ['#ae1654', '#ae1654'], Info: ['#006f99', '#006f99'],
    },
    tags: {
      blue: ['#0061b0', '#0061b0'], geekblue: ['#0060b6', '#0060b6'],
      purple: ['#6741c8', '#6741c8'], gold: ['#7c5c00', '#7c5c00'],
      green: ['#006f43', '#006f43'], orange: ['#a13625', '#a13625'],
      red: ['#ae1754', '#ae1754'], cyan: ['#0e6589', '#0e6589'],
    },
    fill: '#1d2530', mutedOpacity: 0.75, borderOpacity: 0.15, fillOpacity: 0.035,
  },
  dark: {
    page: '#151719', surface: '#1c1e21', sidebar: '#101214', text: '#dde0e3',
    accent: '#00a2e4', accentHover: '#007db3', accentActive: '#0070a1', accentText: '#00a4e5',
    status: {
      Success: ['#00b96d', '#00a461'], Warning: ['#e3735c', '#da6b54'],
      Error: ['#ed658d', '#e45d85'], Info: ['#01a4e6', '#0096d4'],
    },
    tags: {
      blue: ['#6a9df3', '#588bde'], geekblue: ['#699cff', '#5b8ff2'],
      purple: ['#a888ff', '#9773ef'], gold: ['#c39615', '#b28800'],
      green: ['#02b46b', '#00a461'], orange: ['#eb7d66', '#d86c56'],
      red: ['#ef638c', '#e65a85'], cyan: ['#529dc8', '#4a94bf'],
    },
    fill: '#dde0e3', mutedOpacity: 0.65, borderOpacity: 0.14, fillOpacity: 0.03,
  },
} as const;

function channels(hex: string) {
  return [1, 3, 5].map((offset) => parseInt(hex.slice(offset, offset + 2), 16));
}

function translucent(hex: string, opacity: number) {
  return `rgba(${channels(hex).join(', ')}, ${opacity})`;
}

function composite(background: string, foreground: string, opacity: number) {
  const base = channels(background);
  return `#${channels(foreground).map((value, index) =>
    Math.round(value * opacity + base[index] * (1 - opacity)).toString(16).padStart(2, '0'),
  ).join('')}`;
}

export interface CuriosityAppearance {
  config: ThemeConfig;
  buttonClassName: string;
  primaryColor: string;
  surfaces: { page: string; sidebar: string; sidebarBorder: string };
}

/** Resolve appearance without depending on an authenticated organization. */
export function createCuriosityTheme({
  isDark,
  primaryColor,
}: {
  isDark: boolean;
  primaryColor?: string | null;
}): CuriosityAppearance {
  const palette = palettes[isDark ? 'dark' : 'light'];
  const algorithm = isDark ? theme.darkAlgorithm : theme.defaultAlgorithm;
  const border = translucent(palette.text, palette.borderOpacity);
  const divider = translucent(palette.text, 0.07);
  const fill = translucent(palette.fill, palette.fillOpacity);
  const strongerFill = translucent(palette.fill, palette.fillOpacity * 2);
  const accentBackground = translucent(palette.accent, 0.09);
  const token: Tokens = {
    fontFamily: '"Inter", ui-sans-serif, system-ui, sans-serif',
    fontSize: 14,
    fontSizeSM: 12,
    fontSizeHeading3: 20,
    lineHeight: 20 / 14,
    fontWeightStrong: 600,
    controlHeight: 32,
    controlHeightSM: 24,
    controlHeightLG: 40,
    borderRadius: 8,
    borderRadiusSM: 6,
    borderRadiusLG: 12,
    colorBgBase: palette.page,
    colorBgLayout: palette.page,
    colorBgContainer: palette.surface,
    colorBgElevated: palette.surface,
    colorText: palette.text,
    colorTextSecondary: translucent(palette.text, palette.mutedOpacity),
    colorTextTertiary: translucent(palette.text, 0.5),
    colorTextDisabled: translucent(palette.text, 0.2),
    colorTextPlaceholder: translucent(palette.text, palette.mutedOpacity),
    colorTextLightSolid: palette.page,
    colorBorder: border,
    colorBorderSecondary: divider,
    colorSplit: divider,
    colorFill: strongerFill,
    colorFillSecondary: strongerFill,
    colorFillTertiary: fill,
    colorFillQuaternary: fill,
    colorBgContainerDisabled: fill,
    colorBgTextHover: fill,
    colorBgTextActive: strongerFill,
    colorBgMask: isDark ? translucent(palette.page, 0.7) : 'rgba(211, 216, 222, 0.7)',
    colorPrimary: palette.accent,
    colorPrimaryHover: palette.accentHover,
    colorPrimaryActive: palette.accentActive,
    colorPrimaryBg: accentBackground,
    colorPrimaryBgHover: translucent(palette.accent, 0.055),
    colorPrimaryBorder: palette.accent,
    colorPrimaryBorderHover: palette.accentHover,
    colorPrimaryText: palette.accentText,
    colorPrimaryTextHover: palette.accentText,
    colorPrimaryTextActive: palette.accentText,
    colorLink: palette.accentText,
    colorLinkHover: palette.accentText,
    colorLinkActive: palette.accentText,
    controlOutline: translucent(palette.accent, 0.2),
    boxShadow: '0 4px 20px rgba(0, 0, 0, 0.1)',
    boxShadowSecondary: '0 4px 20px rgba(0, 0, 0, 0.1)',
  };

  // Accent customization keeps its complete derived state family together.
  const override = primaryColor?.trim();
  if (override) {
    const derived = theme.getDesignToken({
      algorithm,
      token: { colorPrimary: override, colorLink: override },
    });
    const accentKeys = [
      'colorPrimary', 'colorPrimaryHover', 'colorPrimaryActive', 'colorPrimaryBg',
      'colorPrimaryBgHover', 'colorPrimaryBorder', 'colorPrimaryBorderHover',
      'colorPrimaryText', 'colorPrimaryTextHover', 'colorPrimaryTextActive',
      'colorLink', 'colorLinkHover', 'colorLinkActive', 'controlOutline', 'colorTextLightSolid',
    ] as const;
    for (const key of accentKeys) token[key] = derived[key];
  }

  for (const [name, [color, tint]] of Object.entries(palette.status)) {
    const status = name as keyof typeof palette.status;
    token[`color${status}`] = color;
    token[`color${status}Text`] = color;
    token[`color${status}Bg`] = translucent(tint, 0.09);
    token[`color${status}Border`] = tint;
  }

  // Numbered Tag palettes are independent of semantic success/error tokens.
  for (const name of Object.keys(palette.tags) as (keyof typeof palette.tags)[]) {
    const [color, tint] = palette.tags[name];
    token[name] = color;
    token[`${name}1`] = translucent(tint, name === 'red' || name === 'cyan' ? 0.09 : 0.16);
    token[`${name}2`] = token[`${name}1`];
    token[`${name}3`] = translucent(tint, palette.borderOpacity);
    token[`${name}6`] = color;
    token[`${name}7`] = color;
    // Legacy and current keys serialize to the same CSS variable in Ant Design 6.
    for (const shade of [1, 2, 3, 6, 7] as const) {
      token[`${name}-${shade}`] = token[`${name}${shade}`];
    }
  }

  const primary = token.colorPrimary!;
  const selectedBackground = token.colorPrimaryBg!;
  return {
    primaryColor: primary,
    buttonClassName: isDark && !override ? 'curiosity-dark-primary' : '',
    surfaces: { page: palette.page, sidebar: palette.sidebar, sidebarBorder: border },
    config: {
      // Derive dimensions and defaults, then preserve the explicit palette in both modes.
      algorithm: (seed) => ({ ...algorithm(seed), ...token }),
      token,
      components: {
        Table: {
          // Fixed cells must stay opaque as other columns scroll behind them.
          headerBg: composite(palette.surface, palette.fill, palette.fillOpacity),
          rowHoverBg: composite(palette.surface, palette.fill, palette.fillOpacity),
          headerColor: token.colorTextSecondary,
          borderColor: divider,
          headerSplitColor: divider,
        },
        Menu: {
          itemBg: 'transparent', itemSelectedBg: selectedBackground,
          itemSelectedColor: token.colorPrimaryText, itemHoverBg: fill, itemBorderRadius: 6, itemHeight: 40,
        },
        Button: {
          primaryShadow: 'none', defaultShadow: 'none', defaultBg: palette.surface,
          defaultColor: palette.text, defaultBorderColor: border,
        },
        Input: { activeBorderColor: primary, hoverBorderColor: primary },
        Select: { optionSelectedBg: selectedBackground, optionActiveBg: fill },
        Card: { headerBg: 'transparent' },
        Tag: { defaultBg: fill, defaultColor: token.colorTextSecondary },
        Segmented: { trackBg: fill, itemSelectedBg: palette.surface },
        Modal: { contentBg: palette.surface, headerBg: palette.surface },
        Message: { contentBg: palette.surface },
      },
    },
  };
}
