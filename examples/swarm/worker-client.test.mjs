import test from 'node:test';
import assert from 'node:assert/strict';
import { WorkerClient } from './worker-client.js';

function setup() {
  const worker = { sent: [], postMessage(message) { this.sent.push(message); }, terminate() { this.stopped = true; } };
  return { worker, client: new WorkerClient(worker, 1000) };
}
test('snapshots and commands resolve by request id even if responses arrive out of order', async () => {
  const { worker, client } = setup();
  const snapshot = client.request('state');
  const order = client.request('order/0/hold', 'POST');
  worker.onmessage({ data: { id: worker.sent[1].id, state: { ordered: true } } });
  worker.onmessage({ data: { id: worker.sent[0].id, state: { tick: 1 } } });
  assert.deepEqual(await snapshot, { tick: 1 });
  assert.deepEqual(await order, { ordered: true });
  assert.equal(client.pending.size, 0);
});
test('a fatal worker error rejects in-flight and subsequent requests', async () => {
  const { worker, client } = setup();
  const first = client.request('state');
  worker.onerror({ message: 'WASM initialization failed' });
  await assert.rejects(first, /initialization failed/);
  await assert.rejects(client.request('state'), /initialization failed/);
  assert.equal(client.pending.size, 0);
  assert.equal(worker.stopped, true);
});
test('an invalid command does not kill a healthy worker', async () => {
  const { worker, client } = setup();
  const first = client.request('order/0/invalid', 'POST');
  worker.onmessage({ data: { id: worker.sent[0].id, error: 'unknown order' } });
  await assert.rejects(first, /unknown order/);
  assert.equal(client.failure, null);
});
