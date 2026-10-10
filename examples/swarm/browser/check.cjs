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
    assert.equal(before.reference_map.version, 'synthetic-coast-v1');
    assert.equal(before.reference_map.detail.length, 32 * 20);
    assert.ok(before.command_position.x < before.coastal_position.x);
    assert.ok(before.nodes.every(snapshot => snapshot.every(([key]) => !key.startsWith('map/'))));
    const coastalLinks = before.links.filter(link => link.enabled && link.b === before.center);
    assert.ok(coastalLinks.length > 0 && coastalLinks.length < before.center);
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
    const reconnected = await page.evaluate(async () => (await import('./api.js')).request('state'));
    assert.equal(reconnected.peer_states[0], 'active');
    // Verify delivery with a bounded fleet; the default fleet has no 60-second latency guarantee.
    const delivered = await page.evaluate(async () => {
      const { default: init, Fleet } = await import('./pkg/reconcile_swarm_web.js');
      await init();
      const fleet = new Fleet(0, 3, 1200, 10);
      const timer = setInterval(() => fleet.step(), 50);
      try {
        fleet.command('pause');
        fleet.command('heal');
        fleet.command('peer/0/offline');
        const offline = JSON.parse(fleet.command('order/0/hold'));
        if (offline.nodes[0].some(([, value]) => value.Order)) throw Error('Offline recipient received an order');
        fleet.command('peer/0/online');
        const deadline = Date.now() + 90000;
        while (Date.now() < deadline) {
          const state = JSON.parse(fleet.state());
          const order = state.nodes[0].some(([, value]) => value.Order?.recipient === 0);
          const ack = state.nodes[state.center].some(([, value]) => value.Acknowledgement?.recipient === 0 && value.Acknowledgement.applied);
          if (order && ack) return true;
          await new Promise(resolve => setTimeout(resolve, 250));
        }
        return false;
      } finally {
        clearInterval(timer);
        fleet.free();
      }
    });
    assert.ok(delivered, 'capped three-drone fleet must execute its order and return an acknowledgement');
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
