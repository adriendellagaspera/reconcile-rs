const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const root = path.resolve('target/swarm-pages');
  const server = http.createServer((request, response) => {
    if (request.url === '/reconcile-rs/probe') return response.end('<!doctype html><title>Runtime probe</title>');
    const relative = decodeURIComponent(request.url).replace(/^\/reconcile-rs\//, '') || 'index.html';
    const file = path.resolve(root, relative);
    if (!file.startsWith(root + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) {
      response.statusCode = 404;
      return response.end();
    }
    response.setHeader('Content-Type', file.endsWith('.wasm') ? 'application/wasm' : file.endsWith('.js') ? 'text/javascript' : 'text/html');
    fs.createReadStream(file).pipe(response);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}/reconcile-rs/`;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
    const page = await browser.newPage();
    const errors = [];
    const apiRequests = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => { if (/\/state$|\/order\//.test(request.url())) apiRequests.push(request.url()); });
    await page.goto(base + 'probe');
    const proof = await page.evaluate(async () => {
      const bindings = await import('./pkg/reconcile_swarm_web.js');
      await bindings.default();
      return bindings.verify_runtime();
    });
    console.log(proof);
    await page.goto(base);
    await page.waitForFunction(() => document.getElementById('node').options.length === 13);
    await page.waitForFunction(() => document.getElementById('reports').textContent.length > 0, null, { timeout: 30000 });
    const before = await page.evaluate(async () => {
      const { request } = await import('./api.js');
      await request('pause', 'POST');
      await request('peer/0/offline', 'POST');
      return request('order/0/hold', 'POST');
    });
    const values = snapshot => snapshot.map(([, value]) => value);
    assert.equal(before.peer_states[0], 'offline');
    assert.equal(values(before.nodes[0]).some(value => Boolean(value.Order)), false);
    assert.equal(values(before.nodes[before.center]).some(value => Boolean(value.Order)), true);
    const other = await browser.newPage();
    await other.goto(base);
    await other.waitForFunction(() => document.getElementById('node').options.length === 13);
    const independent = await other.evaluate(async () => (await import('./api.js')).request('state'));
    assert.equal(independent.playing, true);
    assert.equal(values(independent.nodes[independent.center]).some(value => Boolean(value.Order)), false);
    await other.close();
    await page.evaluate(async () => (await import('./api.js')).request('heal', 'POST'));
    await page.waitForFunction(async () => {
      const state = await (await import('./api.js')).request('state');
      const drone = state.nodes[0].map(([, value]) => value);
      const cc = state.nodes[state.center].map(([, value]) => value);
      return drone.some(value => Boolean(value.Order)) && cc.some(value => value.Acknowledgement?.applied);
    }, null, { timeout: 60000 });
    await page.evaluate(() => { const node = document.getElementById('node'); node.value = '0'; node.dispatchEvent(new Event('change')); });
    await page.locator('#order-action').selectOption('patrol');
    await page.waitForFunction(() => document.getElementById('toast').textContent.includes('Order issued'));
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    assert.deepEqual(errors, []);
    assert.deepEqual(apiRequests, []);
    assert.equal(await page.locator('#error').textContent(), '');
    console.log('WASM fleet: default limits, offline order, reconnect acknowledgement, independent visitors, mobile, no HTTP API: passed');
  } finally {
    if (browser) await browser.close();
    server.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
