import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import { applySchemaCatalog } from './schema-catalog.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
const output = resolve(root, 'target/docs/api');
const raw = readFileSync(process.argv[2], 'utf8');
const spec = JSON.parse(raw);
// Node 22's reviver context preserves the original numeric spelling.
const exact = JSON.parse(raw, (key, value, context) =>
  typeof value === 'number' && Number.isInteger(value) && !Number.isSafeInteger(value) ? context.source : value);
const methods = new Set(['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']);
const operations = Object.entries(spec.paths).flatMap(([path, item]) =>
  Object.entries(item).filter(([method]) => methods.has(method))
    .map(([method, operation]) => ({ path, method, ...operation })));
assert.equal(spec.openapi, '3.1.0');
assert.ok(operations.length);
assert.equal(new Set(operations.map(op => op.operationId)).size, operations.length);
for (const op of operations) assert.equal(op['x-space-m1-status'], 'm1-supported');

// Refuse unresolved or external references; browsing never fetches schemas.
function checkRefs(value) {
  if (!value || typeof value !== 'object') return;
  if (value.$ref) {
    assert.ok(value.$ref.startsWith('#/'), `External reference: ${value.$ref}`);
    const target = value.$ref.slice(2).split('/').reduce(
      (node, part) => node?.[part.replaceAll('~1', '/').replaceAll('~0', '~')], spec);
    assert.notEqual(target, undefined, `Missing reference: ${value.$ref}`);
  }
  Object.values(value).forEach(checkRefs);
}
checkRefs(spec);

const git = (...args) => execFileSync('git', args, { cwd: root, encoding: 'utf8' }).trim();
const commit = git('rev-parse', 'HEAD');
const metadata = {
  commit,
  dirty: Boolean(git('status', '--porcelain', '--untracked-files=normal')),
  openapiSha256: createHash('sha256').update(raw).digest('hex'),
  renderer: `redoc ${JSON.parse(readFileSync(resolve(here, 'node_modules/redoc/package.json'))).version}`,
  operations: operations.map(({ method, path, operationId }) => ({ method, path, operationId })),
};
const md = new MarkdownIt({ html: false });
const renderLink = md.renderer.rules.link_open || ((tokens, idx, options, env, self) =>
  self.renderToken(tokens, idx, options));
md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  const token = tokens[idx];
  const href = token.attrGet('href');
  // Keep source navigation useful when the output is served on its own.
  if (href && !/^(?:[a-z]+:|#)/i.test(href)) {
    const [path, fragment] = href.split('#');
    const repoPath = relative(root, resolve(here, path));
    assert.ok(repoPath !== '..' && !repoPath.startsWith('../') && !isAbsolute(repoPath));
    git('ls-files', '--error-unmatch', repoPath);
    token.attrSet('href', `https://github.com/perplexityai/SPACE/blob/${commit}/${repoPath}${fragment ? `#${fragment}` : ''}`);
  }
  return renderLink(tokens, idx, options, env, self);
};
const overview = md.render(readFileSync(resolve(here, 'index.md'), 'utf8').replace(/^# SPACE API\n/, ''));
rmSync(output, { recursive: true, force: true });
mkdirSync(output, { recursive: true });
writeFileSync(resolve(output, 'openapi.json'), raw);
const bundle = readFileSync(resolve(here, 'node_modules/redoc/bundles/redoc.standalone.js'), 'utf8');
const remoteIcon = 'https://cdn.redoc.ly/redoc/logo-mini.svg';
assert.equal(bundle.split(remoteIcon).length, 2, 'Review the offline attribution patch after a Redoc upgrade');
// Retain Redocly's linked text attribution, but suppress its decorative CDN
// image. No external assets should be requested, even when browsing offline.
writeFileSync(resolve(output, 'redoc.standalone.js'), bundle.replace(remoteIcon,
  'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7'));
for (const file of readdirSync(resolve(here, 'node_modules/redoc/bundles'))) {
  if (file.endsWith('.LICENSE.txt')) cpSync(resolve(here, 'node_modules/redoc/bundles', file), resolve(output, file));
}
cpSync(resolve(here, 'node_modules/redoc/LICENSE'), resolve(output, 'REDOC-LICENSE'));
cpSync(resolve(here, 'reference.css'), resolve(output, 'reference.css'));
cpSync(resolve(here, 'schema-navigation.js'), resolve(output, 'schema-navigation.js'));
const themeFiles = JSON.parse(execFileSync('python3', [
  resolve(root, 'docs/site/theme_assets.py'), resolve(output, 'theme'),
], { encoding: 'utf8' }));
const options = JSON.parse(readFileSync(resolve(here, 'redoc-options.json')));
// Presentation only: avoid JS rounding int64 bounds. Preserve the exact bound
// in the field description; the downloaded document remains byte-identical.
function annotateBounds(value, original) {
  if (!value || typeof value !== 'object') return;
  if (value['x-space-m1-status'] === 'incubating') {
    value.description = `**Experimental: outside the supported contract.**\n\n${value.description || ''}`;
  }
  for (const key of ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum']) {
    if (Number.isInteger(value[key]) && !Number.isSafeInteger(value[key])) {
      value.description = `${value.description || ''}\n\nExact \`${key}\`: \`${original[key]}\`.`;
      delete value[key];
    }
  }
  // Redoc displays const constraints, but its sampler needs an explicit
  // example and its union chooser otherwise labels every frame "object".
  if (value.const !== undefined && value.example === undefined) value.example = value.const;
  for (const variant of value.oneOf || []) {
    const tag = variant.properties?.type?.const;
    if (typeof tag === 'string' && !variant.title) variant.title = tag;
  }
  for (const [key, child] of Object.entries(value)) annotateBounds(child, original?.[key]);
  // Redoc takes parameter text from the Parameter Object, not its schema.
  if (value.in && value.schema) {
    for (const key of ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum']) {
      if (typeof original?.schema?.[key] === 'string') {
        value.description = `${value.description || ''}\n\nExact \`${key}\`: \`${original.schema[key]}\`.`;
      }
    }
  }
}
annotateBounds(spec, exact);
// Navigation and samples are presentation-only. The downloadable OpenAPI
// above remains the canonical exporter bytes, including its original tags.
const groups = {
  Sandboxes: ['listSandboxes', 'createSandbox', 'getSandbox', 'deleteSandbox', 'pauseSandbox', 'suspendSandbox', 'wakeSandbox'],
  'Commands and processes': ['createCommand', 'listProcesses', 'getProcess', 'killProcess', 'signalProcess'],
  'Operation polling': ['getOperation'],
  'Event streams': ['streamSessionEvents'],
};
assert.deepEqual(Object.values(groups).flat().sort(), operations.map(op => op.operationId).sort());
spec.tags = Object.keys(groups).map(name => ({ name }));
for (const path of Object.values(spec.paths)) {
  for (const [method, operation] of Object.entries(path)) {
    if (!methods.has(method)) continue;
    operation.tags = [Object.keys(groups).find(name => groups[name].includes(operation.operationId))];
    if (operation.operationId !== 'streamSessionEvents') continue;
    const response = operation.responses['200'];
    const excerpts = md.parse(response.description, {}).filter(token => token.type === 'fence' && token.info === 'text');
    assert.equal(excerpts.length, 1, 'Review the canonical SSE excerpt before rendering it');
    assert.match(response.description, /Excerpt, not a complete stream/);
    response.content['text/event-stream'].examples = {
      excerpt: { summary: 'Excerpt, not a complete stream', value: excerpts[0].content },
    };
  }
}
const schemaCatalog = applySchemaCatalog(spec);
writeFileSync(resolve(output, 'reference.js'),
  `const spec = ${JSON.stringify(spec)};
// Keep authored HTML in the document so combined builds can localize its
// pinned source links without changing the API contract or renderer bundle.
spec.info.description = '# Overview\\n\\n' + document.getElementById('overview').innerHTML + '\\n\\n' + spec.info.description;
Redoc.init(spec, ${JSON.stringify(options)}, document.getElementById('reference'), error => {
  if (error) { document.body.dataset.renderError = String(error); throw error; }
  enhanceSchemaNavigation(${JSON.stringify(schemaCatalog)}, document.querySelector('.docs-utility').getBoundingClientRect().bottom);
  // The outer utility links use browser anchors rather than Redoc's scroller.
  document.querySelectorAll('.docs-utility a[href^="#"]').forEach(link => {
    link.addEventListener('click', event => {
      if (event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      requestAnimationFrame(() => {
        const target = document.getElementById(decodeURIComponent(link.hash.slice(1)));
        if (!target) return;
        const offset = document.querySelector('.docs-utility').getBoundingClientRect().bottom;
        window.scrollTo(0, window.scrollY + target.getBoundingClientRect().top + 1 - offset);
      });
    });
  });
  document.body.dataset.renderReady = 'true';
});\n`);
writeFileSync(resolve(output, 'index.html'), `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'none'; worker-src blob:; base-uri 'none'; form-action 'none'">
<title>SPACE API</title><link rel="stylesheet" href="theme/curiosity.css"><link rel="stylesheet" href="reference.css">
</head><body>
<header class="docs-header">
<a class="docs-brand" href="https://github.com/perplexityai/SPACE/blob/${commit}/docs/introduction.md">SPACE</a>
<nav class="docs-sections" aria-label="Documentation sections">
<a href="https://github.com/perplexityai/SPACE/blob/${commit}/docs/introduction.md">Guide</a>
<a href="index.html" aria-current="page">API Reference</a></nav></header>
<nav class="docs-utility" aria-label="API reading controls">
<div class="docs-utility-links"><a href="#section/Overview">Overview</a><a href="#tag/Sandboxes">Endpoints</a></div>
<span class="docs-utility-title">API Reference</span>
<a href="openapi.json" download>Download OpenAPI JSON</a></nav>
<template id="overview">${overview}</template>
<section id="reference" aria-label="Endpoint reference"></section>
<footer><a href="build.json">Build details</a></footer>
<script src="redoc.standalone.js"></script><script src="schema-navigation.js"></script><script src="reference.js"></script>
</body></html>\n`);
metadata.files = {
  ...Object.fromEntries(readdirSync(output).filter(file => file !== 'theme').map(file => [
    file, createHash('sha256').update(readFileSync(resolve(output, file))).digest('hex'),
  ])),
  ...Object.fromEntries(Object.entries(themeFiles).map(([file, hash]) => [`theme/${file}`, hash])),
};
writeFileSync(resolve(output, 'build.json'), `${JSON.stringify(metadata, null, 2)}\n`);
console.log(`Built ${operations.length} operations from ${commit}${metadata.dirty ? ' (dirty)' : ''} in target/docs/api`);
