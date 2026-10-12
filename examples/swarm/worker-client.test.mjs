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
  const order = client.request('next', 'POST');
  worker.onmessage({ data: { id: worker.sent[1].id, state: { stage: 1 } } });
  worker.onmessage({ data: { id: worker.sent[0].id, state: { stage: 0 } } });
  assert.deepEqual(await snapshot, { stage: 0 });
  assert.deepEqual(await order, { stage: 1 });
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
  const first = client.request('invalid', 'POST');
  worker.onmessage({ data: { id: worker.sent[0].id, error: 'invalid control' } });
  await assert.rejects(first, /invalid control/);
  assert.equal(client.failure, null);
});
