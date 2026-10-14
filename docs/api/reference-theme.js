// Redoc's color calculations require resolved values rather than CSS variables.
function referenceTheme(options) {
  const styles = getComputedStyle(document.documentElement);
  const color = name => styles.getPropertyValue(`--docs-color-${name}`).trim();
  const theme = options.theme;
  return {
    ...options,
    theme: {
      ...theme,
      sidebar: {
        ...theme.sidebar,
        backgroundColor: color('background'),
        textColor: color('muted'),
        activeTextColor: color('accent'),
        groupItems: {
          ...theme.sidebar?.groupItems,
          textTransform: 'none',
        },
        level1Items: {
          ...theme.sidebar?.level1Items,
          activeBackgroundColor: color('soft'),
          activeTextColor: color('accent'),
          textTransform: 'none',
        },
      },
      rightPanel: {
        ...theme.rightPanel,
        backgroundColor: color('raised'),
        textColor: color('code-text'),
        servers: {
          ...theme.rightPanel?.servers,
          overlay: { backgroundColor: color('code-background'), textColor: color('code-text') },
          url: { backgroundColor: color('raised') },
        },
      },
      codeBlock: { backgroundColor: color('code-background') },
      colors: {
        ...theme.colors,
        primary: { main: color('accent') },
        text: { primary: color('text'), secondary: color('muted') },
        border: { light: color('border'), dark: color('border') },
        gray: { 50: color('nested'), 100: color('background') },
        success: { main: color('success') },
        error: { main: color('error') },
        warning: { main: color('warning') },
        http: {
          ...theme.colors?.http,
          get: color('accent'),
          post: color('success'),
          put: color('warning'),
          patch: color('warning'),
          delete: color('error'),
          options: color('accent'),
          head: color('accent'),
          basic: color('accent'),
          link: color('accent'),
        },
        responses: {
          ...theme.colors?.responses,
          success: { color: color('success'), tabTextColor: color('response-success-text') },
          error: { color: color('error'), tabTextColor: color('response-error-text') },
          redirect: { color: color('warning'), tabTextColor: color('warning') },
          info: { color: color('info'), tabTextColor: color('info') },
        },
      },
      typography: {
        ...theme.typography,
        code: { ...theme.typography.code, color: color('text'), backgroundColor: color('inline-code') },
        links: { ...theme.typography.links, color: color('accent'), visited: color('accent'), hover: color('accent') },
      },
      schema: {
        ...theme.schema,
        linesColor: color('border'),
        typeNameColor: color('accent'),
        typeTitleColor: color('text'),
        requireLabelColor: color('error'),
        nestedBackground: color('nested'),
      },
      fab: { backgroundColor: color('raised'), color: color('accent') },
    },
  };
}
