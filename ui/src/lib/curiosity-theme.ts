import { theme } from 'antd';
import type { ThemeConfig } from 'antd';

type Tokens = NonNullable<ThemeConfig['token']>;

const palettes = {
  light: {
    page: '#fbfbfb', surface: '#ffffff', sidebar: '#e9eded', text: '#0b2833',
    accent: '#016a71', accentHover: '#0c4f54', accentActive: '#0f3639',
    success: '#016b1d', warning: '#97431a', error: '#a23544',
    blue: '#006495', purple: '#7a3abb', indigo: '#3053b3', gold: '#825503',
    fill: '#1c1c1c', mutedOpacity: 0.75, borderOpacity: 0.15, fillOpacity: 0.035,
  },
  dark: {
    page: '#161616', surface: '#1d1d1d', sidebar: '#111111', text: '#dedede',
    accent: '#4e99a3', accentHover: '#22808c', accentActive: '#1a636c',
    success: '#539e55', warning: '#d57141', error: '#dc6973',
    blue: '#5691c7', purple: '#a96ee0', indigo: '#7989e7', gold: '#bc811e',
    fill: '#dedede', mutedOpacity: 0.65, borderOpacity: 0.14, fillOpacity: 0.03,
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
    colorTextPlaceholder: translucent(palette.text, 0.5),
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
    colorBgMask: isDark ? translucent(palette.page, 0.7) : 'rgba(212, 212, 212, 0.7)',
    colorPrimary: palette.accent,
    colorPrimaryHover: palette.accentHover,
    colorPrimaryActive: palette.accentActive,
    colorPrimaryBg: accentBackground,
    colorPrimaryBgHover: translucent(palette.accent, 0.055),
    colorPrimaryBorder: palette.accent,
    colorPrimaryBorderHover: palette.accentHover,
    colorPrimaryText: palette.accent,
    colorPrimaryTextHover: palette.accentHover,
    colorPrimaryTextActive: palette.accentActive,
    colorLink: palette.accent,
    colorLinkHover: palette.accentHover,
    colorLinkActive: palette.accentActive,
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

  const statusColors = {
    Success: palette.success, Warning: palette.warning, Error: palette.error, Info: palette.accent,
  } as const;
  for (const [name, color] of Object.entries(statusColors)) {
    const status = name as keyof typeof statusColors;
    token[`color${status}`] = color;
    token[`color${status}Text`] = color;
    token[`color${status}Bg`] = translucent(color, 0.09);
    token[`color${status}Border`] = color;
  }

  // Numbered Tag palettes are independent of semantic success/error tokens.
  const tagColors = {
    blue: palette.blue, geekblue: palette.indigo, purple: palette.purple, gold: palette.gold,
    green: palette.success, orange: palette.warning, red: palette.error, cyan: palette.accent,
  } as const;
  for (const name of Object.keys(tagColors) as (keyof typeof tagColors)[]) {
    const color = tagColors[name];
    token[name] = color;
    token[`${name}1`] = translucent(color, name === 'red' || name === 'cyan' ? 0.09 : 0.16);
    token[`${name}2`] = token[`${name}1`];
    token[`${name}3`] = border;
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
          itemSelectedColor: primary, itemHoverBg: fill, itemBorderRadius: 6, itemHeight: 40,
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
