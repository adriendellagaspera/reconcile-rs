import test from 'node:test';
import assert from 'node:assert/strict';
import { endpoint } from './api.js';

test('local API stays relative to the serving directory', () => {
  assert.equal(endpoint('state', '', 'http://127.0.0.1:8088/'), 'http://127.0.0.1:8088/state');
  assert.equal(endpoint('state', '', 'https://example.org/demo/index.html'), 'https://example.org/demo/state');
});
test('Pages actions and polling use the configured API prefix', () => {
  for (const path of ['state', 'order/0/hold', 'peer/1/offline']) {
    assert.equal(endpoint(path, 'https://fleet.example.org/api/', 'https://user.github.io/reconcile-rs/'), 'https://fleet.example.org/api/' + path);
  }
});
test('unconfigured Pages, mixed content and credential URLs fail explicitly', () => {
  assert.throws(() => endpoint('state', '', 'https://user.github.io/reconcile-rs/'), /not configured/);
  for (const base of ['http://fleet.example.org', 'https://user:pass@fleet.example.org', 'javascript:alert(1)', 'https://fleet.example.org/?secret=1']) {
    assert.throws(() => endpoint('state', base, 'https://user.github.io/reconcile-rs/'));
  }
});
