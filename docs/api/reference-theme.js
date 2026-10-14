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
      },
      rightPanel: {
        ...theme.rightPanel,
        backgroundColor: color('surface'),
        textColor: color('code-text'),
      },
      codeBlock: { backgroundColor: color('code-background') },
      colors: {
        ...theme.colors,
        primary: { main: color('accent') },
        text: { primary: color('text'), secondary: color('muted') },
        border: { light: color('border'), dark: color('muted') },
        gray: { 50: color('nested'), 100: color('background') },
        success: { main: color('success') },
        error: { main: color('error') },
        warning: { main: color('warning') },
        responses: {
          ...theme.colors.responses,
          info: { color: color('info') },
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
        nestedBackground: color('nested'),
      },
      fab: { backgroundColor: color('accent'), color: color('surface') },
    },
  };
}
