// Reinitialize the shell and renderer together when the system palette changes.
(() => {
  const root = document.documentElement;
  const preference = window.matchMedia('(prefers-color-scheme: dark)');
  root.dataset.docsTheme = preference.matches ? 'dark' : 'light';
  preference.addEventListener('change', () => window.location.reload());
})();
