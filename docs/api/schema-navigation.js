// Progressive enhancement of native Redoc schema output, never a schema renderer.
function enhanceSchemaNavigation(catalog, scrollOffset = 0) {
  const initialHash = location.hash;
  for (const { name, purpose, related, primitive } of catalog) {
    const section = document.querySelector(`[data-section-id="schema/${name}"]`);
    const heading = section?.querySelector('h2');
    if (!heading || section.querySelector('.schema-context')) continue;
    const context = document.createElement('div');
    context.className = 'schema-context';
    const identifier = document.createElement('code');
    identifier.textContent = name;
    const description = document.createElement('p');
    description.textContent = purpose;
    const links = document.createElement('p');
    links.append('Related: ');
    related.forEach(({ label, href }, index) => {
      if (index) links.append(' · ');
      const link = document.createElement('a');
      link.href = href;
      link.textContent = label;
      links.append(link);
    });
    context.append(identifier, description, links);
    heading.after(context);
    if (!primitive) continue;
    // Redoc 2.5.4: the schema definition's row contains the field view and
    // sample panel. Guard the structure; unexpected output stays fully visible.
    const row = section.children[1]?.firstElementChild;
    const panel = row?.children[1];
    if (row?.children.length !== 2 || !panel
      || row.children[0].querySelector('.redoc-json')
      || panel.querySelectorAll('.redoc-json').length !== 1) continue;
    const details = document.createElement('details');
    details.className = 'schema-example';
    const summary = document.createElement('summary');
    summary.textContent = 'Example value';
    details.append(summary, ...panel.childNodes);
    panel.append(details);
  }
  // Collapsing earlier samples changes layout. Re-anchor an initial legacy
  // schema/index link after enhancement without rewriting its hash or history.
  if (initialHash.startsWith('#schema/') || initialHash === '#tag/Schemas') {
    const reanchor = () => {
      if (location.hash !== initialHash) return;
      const anchor = document.getElementById(decodeURIComponent(initialHash.slice(1)));
      if (!anchor) return;
      // Land just past the section boundary, matching native active-item tracking.
      window.scrollTo(0, window.scrollY + anchor.getBoundingClientRect().top + 1 - scrollOffset);
    };
    // Reload scroll restoration and font layout must finish before re-anchoring.
    const afterLoad = () => document.fonts.ready.then(() => requestAnimationFrame(reanchor));
    if (document.readyState === 'complete') afterLoad();
    else window.addEventListener('load', afterLoad, { once: true });
  }
}
