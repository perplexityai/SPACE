import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import { chromium } from 'playwright';
import { applySchemaCatalog, modelGroups } from './schema-catalog.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
const output = resolve(root, 'target/docs/api');
const raw = readFileSync(resolve(output, 'openapi.json'), 'utf8');
const spec = JSON.parse(raw);
const metadata = JSON.parse(readFileSync(resolve(output, 'build.json')));
const git = (...args) => execFileSync('git', args, { cwd: root, encoding: 'utf8' }).trim();
assert.equal(metadata.commit, git('rev-parse', 'HEAD'));
assert.equal(metadata.openapiSha256, createHash('sha256').update(raw).digest('hex'));
const methods = new Set(['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']);
const expected = Object.entries(spec.paths).flatMap(([path, item]) =>
  Object.entries(item).filter(([method]) => methods.has(method))
    .map(([method, op]) => ({ method, path, operationId: op.operationId })));
assert.deepEqual(metadata.operations, expected);
const presentation = structuredClone(spec);
presentation.tags = [];
const catalog = applySchemaCatalog(presentation);
assert.deepEqual(presentation.paths, spec.paths, 'Schema navigation does not change endpoints');
for (const [name, schema] of Object.entries(presentation.components.schemas)) {
  const original = spec.components.schemas[name];
  const restored = { ...schema };
  for (const key of ['title', 'x-tags']) {
    if (Object.hasOwn(original, key)) restored[key] = original[key];
    else delete restored[key];
  }
  assert.deepEqual(restored, original, `${name}: only navigation metadata changes`);
}

// Audit Markdown links via tokens, including autolinks and reference links.
// GitHub-style anchors include duplicate-heading suffixes.
const md = new MarkdownIt();
function anchors(source) {
  const counts = new Map();
  const result = new Set();
  const tokens = md.parse(source, {});
  tokens.forEach((token, index) => {
    if (token.type !== 'heading_open') return;
    const text = tokens[index + 1].children
      .filter(child => ['text', 'code_inline'].includes(child.type))
      .map(child => child.content).join('');
    const slug = text.toLowerCase().replace(/[^\p{L}\p{N}_\- ]/gu, '').replaceAll(' ', '-');
    const count = counts.get(slug) || 0;
    counts.set(slug, count + 1);
    result.add(`${slug}${count ? `-${count}` : ''}`);
  });
  return result;
}
let links = 0;
let fences = 0;
for (const file of ['README.md', 'docs/api/index.md']) {
  const source = readFileSync(resolve(root, file), 'utf8');
  for (const token of md.parse(source, {})) {
    if (token.type === 'fence' && token.info === 'bash') {
      assert.equal(spawnSync('bash', ['-n'], { input: token.content }).status, 0, `${file}: Bash syntax`);
      fences++;
    }
    for (const child of token.children || []) {
      const href = child.attrGet('href') || child.attrGet('src');
      if (!href) continue;
      links++;
      if (/^[a-z]+:/i.test(href)) continue;
      const [path, fragment] = decodeURIComponent(href).split('#');
      const target = path ? resolve(root, dirname(file), path) : resolve(root, file);
      assert.ok(target.startsWith(`${root}/`));
      git('ls-files', '--error-unmatch', target.slice(root.length + 1));
      if (fragment) assert.ok(anchors(readFileSync(target, 'utf8')).has(fragment), `${file}: ${href}`);
    }
  }
}
console.log(`Source checks: ${links} Markdown links, ${fences} Bash fences; source commit and spec hash match`);

const mime = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.css': 'text/css' };
const server = createServer((request, response) => {
  const path = resolve(output, `.${decodeURIComponent(new URL(request.url, 'http://localhost').pathname)}`);
  if (!path.startsWith(`${output}/`) && path !== output) { response.writeHead(403).end(); return; }
  if (request.method !== 'GET') { response.writeHead(405).end(); return; }
  try {
    const file = path === output ? resolve(output, 'index.html') : path;
    response.setHeader('Content-Type', mime[extname(file)] || 'text/plain');
    response.end(readFileSync(file));
  } catch { response.writeHead(404).end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch();
try {
  const context = await browser.newContext();
  const page = await context.newPage();
  const errors = [];
  const unsafeRequests = [];
  page.on('pageerror', error => errors.push(String(error)));
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
  await context.route('**/*', route => {
    const request = route.request();
    if (new URL(request.url()).origin !== origin || request.method() !== 'GET') {
      unsafeRequests.push(`${request.method()} ${request.url()}`);
      return route.abort();
    }
    return route.continue();
  });
  await page.goto(origin);
  await page.waitForSelector('body[data-render-ready="true"]');
  assert.equal(await page.title(), 'SPACE API');
  assert.equal(await page.locator('input, [data-role="search:results"]').count(), 0);
  assert.deepEqual(await page.locator('.menu-content [data-item-id^="tag/"]')
    .evaluateAll(elements => elements.map(el => el.dataset.itemId).filter(id => !id.includes('/operation/') && !id.includes('/schema/'))),
  ['tag/Sandboxes', 'tag/Commands-and-processes', 'tag/Operation-polling', 'tag/Event-streams',
    'tag/Schemas', ...modelGroups.map(group => 'tag/' + group.name.replaceAll(' ', '-'))]);
  const visibleText = await page.locator('body').innerText();
  assert.match(visibleText, /five attempts/);
  for (const status of ['413', '415', '422']) assert.ok(visibleText.includes(status));
  assert.doesNotMatch(visibleText, /\b(?:E2B|M1|native API|space-m1-status|incubating)\b/i);
  assert.doesNotMatch(visibleText, /sandbox row|CreateCommandRequest::env|EnvMap|cluster.activation.limiter/);
  const ids = await page.locator('[data-section-id^="operation/"]').evaluateAll(
    elements => elements.map(element => element.dataset.sectionId.replace('operation/', '')).sort());
  assert.deepEqual(ids, expected.map(op => op.operationId).sort());
  for (const op of expected) {
    const id = `operation/${op.operationId}`;
    assert.ok(await page.locator(`[data-section-id="${id}"]`).count(), id);
    // Navigate through the endpoint menu and check its shareable deep link.
    const menuItem = page.locator(`.menu-content [data-item-id$="/${id}"]`).first();
    const groupedId = await menuItem.getAttribute('data-item-id');
    // Open the native tag group before selecting its endpoint.
    if (!await menuItem.isVisible()) {
      await page.locator(`.menu-content [data-item-id="${groupedId.split('/operation/')[0]}"]`).click();
    }
    await menuItem.click();
    await page.waitForURL(url => decodeURIComponent(url.hash) === `#${groupedId}`);
    assert.ok((await page.locator(`[data-section-id="${id}"]`).innerText()).includes(op.path));
    // Redoc supplies the legacy operation/<id> anchor even with grouped tags.
    await page.goto(`${origin}/#${id}`);
    await page.waitForFunction(id => {
      const top = document.querySelector(`[data-section-id="${id}"] h2`).getBoundingClientRect().top;
      return top >= -1 && top < 200;
    }, id);
    const operation = spec.paths[op.path][op.method];
    for (const [status, response] of Object.entries(operation.responses)) {
      const example = response.content?.['application/json']?.examples?.illustrative?.value;
      if (!status.startsWith('2') || !example) continue;
      const rendered = page.locator(`[data-section-id="${id}"]`);
      await rendered.getByRole('tab', { name: status, exact: true }).click();
      const sample = rendered.locator('[role="tabpanel"]:visible').last();
      const expand = sample.getByRole('button', { name: 'Expand all', exact: true });
      if (await expand.count()) await expand.click();
      assert.deepEqual(JSON.parse(await sample.locator('.redoc-json').innerText()), example);
    }
  }
  const schemas = Object.keys(spec.components.schemas);
  const schemaIds = await page.locator('[data-section-id^="schema/"]').evaluateAll(
    elements => elements.map(element => element.dataset.sectionId.slice('schema/'.length)).sort());
  assert.deepEqual(schemaIds, [...schemas].sort());
  for (const group of modelGroups) {
    const id = 'tag/' + group.name.replaceAll(' ', '-');
    assert.deepEqual(await page.locator(`.menu-content [data-item-id="${id}"] [data-item-id^="schema/"]`)
      .evaluateAll(elements => elements.map(el => el.dataset.itemId.slice(7))),
    group.models.map(([name]) => name));
  }
  for (const { name, title, purpose, related, primitive } of catalog) {
    const section = page.locator(`[data-section-id="schema/${name}"]`);
    assert.equal(await section.locator('h2').first().innerText(), title);
    assert.equal(await section.locator('.schema-context code').innerText(), name);
    assert.ok((await section.locator('.schema-context').innerText()).includes(purpose));
    assert.deepEqual(await section.locator('.schema-context a').evaluateAll(
      links => links.map(link => link.getAttribute('href'))), related.map(link => link.href));
    const details = section.locator('details.schema-example');
    assert.equal(await details.count(), primitive ? 1 : 0);
    if (primitive) {
      assert.equal(await details.getAttribute('open'), null);
      const text = await details.locator('.redoc-json').textContent();
      await details.locator('summary').focus();
      await page.keyboard.press('Enter');
      assert.equal(await details.locator('.redoc-json').isVisible(), true);
      assert.equal(await details.locator('.redoc-json').textContent(), text);
      await page.keyboard.press('Enter');
      assert.equal(await details.getAttribute('open'), null);
    }
    const menuItem = page.locator(`.menu-content [data-item-id="schema/${name}"]`);
    const group = modelGroups.find(group => group.models.some(([key]) => key === name));
    const groupItem = page.locator(`.menu-content [data-item-id="tag/${group.name.replaceAll(' ', '-')}"]`);
    // Redoc's scroll spy activates the menu entry for whichever section is on
    // screen and collapses the groups of the entry it replaces, so this
    // model's group can close between any visibility check and the click.
    // Re-open it whenever it is collapsed and retry a short click until one
    // lands.
    for (let attempt = 1; ; attempt++) {
      if (await groupItem.getAttribute('aria-expanded') !== 'true') await groupItem.click();
      try {
        await menuItem.click({ timeout: 2000 });
        break;
      } catch (error) {
        if (error.name !== 'TimeoutError' || attempt === 5) throw error;
      }
    }
    await page.waitForURL(url => decodeURIComponent(url.hash) === `#schema/${name}`);
    await page.goto(`${origin}/?direct=${name}#schema/${name}`);
    await page.waitForSelector('body[data-render-ready="true"]');
    await page.waitForFunction(name => {
      const top = document.querySelector(`[data-section-id="schema/${name}"] h2`).getBoundingClientRect().top;
      return top >= 0 && top < 150;
    }, name);
    assert.equal(await section.locator('.schema-context code').innerText(), name);
  }
  for (const name of ['CreateCommandRequest', 'TerminalSize']) {
    await page.goto(`${origin}/?fresh=${name}#schema/${name}`);
    await page.waitForSelector('body[data-render-ready="true"]');
    await page.waitForFunction(name => {
      const top = document.querySelector(`[data-section-id="schema/${name}"] h2`).getBoundingClientRect().top;
      return top >= 0 && top < 150;
    }, name);
  }
  await page.goto(`${origin}/#tag/Schemas`);
  await page.waitForFunction(() => {
    const top = document.querySelector('[data-section-id="tag/Schemas"] h2').getBoundingClientRect().top;
    return top >= 0 && top < 150;
  });
  assert.equal(await page.locator('.schema-context').count(), schemas.length);
  assert.equal(await page.locator('details.schema-example').count(), catalog.filter(model => model.primitive).length);
  const create = await page.locator('[data-section-id="operation/createSandbox"]').innerText();
  assert.match(create, /seed:latest/);
  const experimentalCreateFields = Object.values(spec.components.schemas.CreateSandboxRequest.properties)
    .filter(field => field['x-space-m1-status'] === 'incubating').length;
  assert.equal((create.match(/Experimental: outside the supported contract/g) || []).length, experimentalCreateFields);
  const command = await page.locator('[data-section-id="operation/createCommand"]').innerText();
  assert.match(command, /Hello from SPACE/);
  assert.match(command, /Attaching to the stream starts execution/);
  const events = await page.locator('[data-section-id="operation/streamSessionEvents"]').innerText();
  assert.match(events, /missed output is not replayed/);
  assert.match(events, /9223372036854775807/);
  assert.doesNotMatch(events, /9223372036854776000/);
  const eventOperation = page.locator('[data-section-id="operation/streamSessionEvents"]');
  await eventOperation.getByRole('tab', { name: '200', exact: true }).click();
  const eventSample = eventOperation.locator('[role="tabpanel"]:visible');
  assert.match(await eventSample.innerText(), /text\/event-stream/);
  const eventResponse = Object.values(spec.paths).map(path => path.get)
    .find(op => op?.operationId === 'streamSessionEvents').responses['200'];
  const excerpt = md.parse(eventResponse.description, {}).find(token => token.type === 'fence' && token.info === 'text').content;
  assert.equal((await eventSample.locator('pre').innerText()).trim(), excerpt.trim());
  assert.match(await eventSample.innerText(), /stdout[\s\S]*process_exited/);
  assert.match(await eventOperation.innerText(), /Excerpt, not a complete stream/);
  assert.equal(await eventOperation.getByRole('tab', { name: '202', exact: true }).count(), 0);
  const createOperation = page.locator('[data-section-id="operation/createSandbox"]');
  await createOperation.getByRole('tab', { name: '202', exact: true }).click();
  const accepted = JSON.parse(await createOperation.locator('[role="tabpanel"]:visible .redoc-json').last().innerText());
  assert.match(accepted.operation_id, /^op_[0-9a-f-]{36}$/);
  assert.match(accepted.sandbox_id, /^sbx_[0-9a-f-]{36}$/);
  for (const operationId of ['getOperation', 'streamSessionEvents']) {
    const operation = page.locator(`[data-section-id="operation/${operationId}"]`);
    for (const [status, code] of [['400', 'invalid_request'], ['401', 'unauthorized'], ['500', 'internal']]) {
      await operation.getByRole('tab', { name: status, exact: true }).click();
      const sample = JSON.parse(await operation.locator('[role="tabpanel"]:visible .redoc-json').innerText());
      const error = operationId === 'streamSessionEvents' ? sample.error : sample;
      assert.equal(error.code, code);
      assert.equal(error.retryable, status === '500' && operationId === 'getOperation');
      assert.notEqual(error.message, 'string');
    }
  }
  const frames = page.locator('[data-section-id="schema/DataPlaneServerFrame"]');
  assert.equal(await frames.count(), 1);
  assert.match(await frames.innerText(), /attached/);
  // Inspect the stdout union variant and its required base64 payload.
  await frames.getByRole('button', { name: 'stdout', exact: true }).click();
  const variants = frames.locator('select');
  if (await variants.count()) {
    const options = await variants.first().locator('option').allTextContents();
    const stdout = options.findIndex(text => /stdout/.test(text));
    assert.ok(stdout >= 0, 'SSE stdout variant is navigable');
    await variants.first().selectOption({ index: stdout });
    assert.match(await frames.innerText(), /data_base64/);
  } else {
    assert.match(await frames.innerText(), /data_base64/);
  }
  assert.equal(await page.locator('form, textarea, input[type="password"]').count(), 0);
  assert.equal(await page.locator('input, [data-role="search:results"]').count(), 0);
  assert.equal(await page.getByRole('button', { name: /^(try it.*|send request|execute|authorize)$/i }).count(), 0);
  await page.goto(`${origin}/#operation/createSandbox`);
  await page.waitForFunction(() => {
    const top = document.querySelector('[data-section-id="operation/createSandbox"] h2').getBoundingClientRect().top;
    return top >= 0 && top < 200;
  });
  assert.equal(await page.locator('a[download]').getAttribute('href'), 'openapi.json');
  const download = await context.request.get(`${origin}/openapi.json`);
  assert.equal(await download.text(), raw);
  assert.deepEqual(unsafeRequests, [], 'No external assets or API requests');
  assert.deepEqual(errors, [], 'No render or CSP errors');
  await page.screenshot({ path: resolve(output, 'endpoint-check.png'), fullPage: false });
  await frames.scrollIntoViewIfNeeded();
  await page.screenshot({ path: resolve(output, 'schema-check.png'), fullPage: false });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: resolve(output, 'overview-check.png'), fullPage: false });
  console.log(`Render checks: ${expected.length} grouped operations and ${schemas.length} grouped/friendly schemas via native menus/direct links, canonical names/context/relations, primitive keyboard disclosures, fresh schema anchors, canonical SSE 200 excerpt/create 202/error samples, SSE union, exact int64, experimental fields, plain API naming, download; no API search, credential forms, execution controls, external requests, or browser errors`);
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
