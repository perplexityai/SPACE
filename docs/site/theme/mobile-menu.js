// Keep mobile visibility and navigation with each renderer's native controller.
function initializeDocsMenu() {
  const button = document.querySelector('.docs-menu-toggle');
  const checkbox = document.getElementById('sidebar-toggle-anchor');
  const menu = document.querySelector(checkbox ? '#sidebar' : '#reference .menu-content');
  const nativeToggle = checkbox ? document.getElementById('sidebar-toggle') : menu?.nextElementSibling;
  if (!button || !menu || !nativeToggle) return;
  // Redoc 2.5.4 places its toggle between the sidebar and the reading column.
  // Preserve the original control if an upgraded renderer changes this shape.
  if (!checkbox && (nativeToggle.nextElementSibling !== document.querySelector('#reference .api-content')
      || !nativeToggle.querySelector('svg'))) return;

  const mobile = matchMedia('(max-width: 800px)');
  const isOpen = () => checkbox ? checkbox.checked : getComputedStyle(menu).display !== 'none';
  let focusOnOpen = false;
  let wasOpen = false;
  const update = () => {
    const open = mobile.matches && isOpen();
    button.setAttribute('aria-expanded', String(open));
    if (open && focusOnOpen) {
      focusOnOpen = false;
      const first = menu.querySelector('a.active') || menu.querySelector('[role="menuitem"][tabindex="0"], a[href]');
      first?.focus();
    }
    if (!open && wasOpen && menu.contains(document.activeElement)) button.focus();
    wasOpen = open;
  };
  button.addEventListener('click', () => {
    focusOnOpen = !isOpen();
    nativeToggle.click();
    update();
  });
  const closeOnEscape = event => {
    if (event.key !== 'Escape' || !mobile.matches || !isOpen()) return;
    event.preventDefault();
    event.stopPropagation();
    focusOnOpen = false;
    nativeToggle.click();
    update();
    button.focus();
  };
  button.addEventListener('keydown', closeOnEscape);
  menu.addEventListener('keydown', closeOnEscape, true);
  new MutationObserver(update).observe(menu, { attributes: true, attributeFilter: ['class', 'style', 'aria-hidden'] });
  if (checkbox) checkbox.addEventListener('change', update);
  mobile.addEventListener('change', update);

  menu.id ||= 'reference-menu';
  button.setAttribute('aria-controls', menu.id);
  if (!checkbox) nativeToggle.setAttribute('data-docs-native-menu', '');
  document.documentElement.dataset.docsMenu = 'ready';
  button.hidden = false;
  update();
}

if (document.getElementById('sidebar-toggle-anchor')) initializeDocsMenu();
