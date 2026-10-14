// Choose once per page load so the shell and renderer keep the same palette.
(() => {
  const root = document.documentElement;
  root.dataset.docsTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
})();
